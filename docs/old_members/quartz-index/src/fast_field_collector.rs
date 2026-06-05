use std::marker::PhantomData;

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{Column, DynamicColumn, HasAssociatedColumnType};
use tantivy::{DocId, Score};

use crate::filters::{create_timestamp_filter_builder, TimestampFilter, TimestampFilterBuilder};



#[derive(Clone)]
pub(crate) struct FastFieldSegmentCollector<Item: HasAssociatedColumnType> {
    values: Vec<Item>,
    column_opt: Option<Column<Item>>,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl <Item: HasAssociatedColumnType> FastFieldSegmentCollector<Item> {
    pub fn new(
        column_opt: Option<Column<Item>>,
        timestamp_filter_opt: Option<TimestampFilter>,
    ) -> Self {
        Self {
            values: Vec::new(),
            column_opt,
            timestamp_filter_opt,
        }
    }

    pub fn accept_document(&self, doc_id: DocId) -> bool {
        match self.timestamp_filter_opt {
            Some(ref timestamp_filter) => timestamp_filter.contains_doc_timestamp(doc_id),
            None => true,
        }
    }
    
}

impl<Item: HasAssociatedColumnType> SegmentCollector for FastFieldSegmentCollector<Item> {
    type Fruit = Vec<Item>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        let Some(column) = self.column_opt.as_ref() else {
            return;
        };
        if !self.accept_document(doc_id) {
            return;
        }
        self.values.extend(column.values_for_doc(doc_id));
    }

    fn harvest(self) -> Self::Fruit {
        self.values
    }
}


pub(crate) struct FastFieldCollector<Item: HasAssociatedColumnType> {
    pub fast_field: String,
    pub timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub _marker: PhantomData<Item>,
}

impl<Item: HasAssociatedColumnType> FastFieldCollector<Item> {
    pub fn new(
        fast_field: &str,
        timestamp_field_opt: Option<&str>,
        start_timestamp_secs: Option<i64>,
        end_timestamp_secs: Option<i64>,
    ) -> Self {
        let timestamp_filter_builder_opt = create_timestamp_filter_builder(
            timestamp_field_opt,
            start_timestamp_secs,
            end_timestamp_secs,
        );
        Self { 
            fast_field: fast_field.to_string(),
            timestamp_filter_builder_opt,
            _marker: PhantomData 
        }
    }

}

impl<Item: HasAssociatedColumnType>  Collector for FastFieldCollector<Item> 
where DynamicColumn: Into<Option<Column<Item>>> {
    type Fruit = Vec<Item>;

    type Child = FastFieldSegmentCollector<Item>;

    fn for_segment(
        &self,
        _segment_ord: tantivy::SegmentOrdinal,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let timestamp_filter_opt = match &self.timestamp_filter_builder_opt {
            Some(timestamp_filter_builder) => {
                timestamp_filter_builder.build(segment_reader)?     
            }
            None => None,
        };
            
        let column_opt: Option<Column<Item>> = segment_reader
            .fast_fields()
            .column_opt::<Item>(&self.fast_field)?;

        Ok(FastFieldSegmentCollector::new(
            column_opt,
            timestamp_filter_opt,
        ))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<Item>>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits.into_iter().flatten().collect::<Vec<_>>())
    }
}


