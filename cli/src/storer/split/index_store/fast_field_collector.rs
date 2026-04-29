use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::Column;
use tantivy::{DocId, Result, SegmentReader};

pub struct U64FastFieldCollector {
    field_name: String,
}

impl U64FastFieldCollector {
    pub fn new(field_name: impl ToString) -> Self {
        Self {
            field_name: field_name.to_string(),
        }
    }
}

impl Collector for U64FastFieldCollector {
    type Fruit = Vec<u64>;
    type Child = U64FastFieldSegmentCollector;

    fn for_segment(
        &self,
        _segment_id: u32,
        segment: &SegmentReader,
    ) -> Result<U64FastFieldSegmentCollector> {
        let column = segment.fast_fields().u64(&self.field_name)?;
        Ok(U64FastFieldSegmentCollector {
            column,
            values: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<u64>>) -> Result<Vec<u64>> {
        Ok(segment_fruits.into_iter().flatten().collect())
    }
}

pub struct U64FastFieldSegmentCollector {
    column: Column<u64>,
    values: Vec<u64>,
}

impl SegmentCollector for U64FastFieldSegmentCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _score: f32) {
        if let Some(v) = self.column.first(doc) {
            self.values.push(v)
        }
    }

    fn harvest(self) -> Vec<u64> {
        self.values
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        for doc in docs {
            self.collect(*doc, 0.0);
        }
    }
}
