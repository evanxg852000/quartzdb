use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, error::Result, execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter};

use crate::search::context::TableSearchContext;

#[derive(Debug)]
pub struct SplitSearcher {
    context: Arc<TableSearchContext>,
}

impl SplitSearcher {
    pub async fn search(
        context: Arc<TableSearchContext>,
        schema: SchemaRef, 
        split_id: String,
        projection: Vec<u64>,
        fts_expr: Option<String>,
    ) -> Result<SendableRecordBatchStream> {
        println!("table_name: {}", context.get_table_meta().name);
        println!("split_id: {}", split_id);
        println!("projection: {:?}", projection);
        println!("fts_expr: {:?}", fts_expr);
        // download split
        // open (& cache it) index of fts_expr is not null 
        // open parquet 
        // let split_reader = SplitReader::new(split_id);
        // split_reader.open()?;
        // //cache
        // split_tag_filter = split_reader.get_tag_filter();
        // context.get_tags_filter_cache().put(split_id, split_tag_filter.clone());


        //TODO: !!! the last dance !!!

        let batch = fixture::get_sample_batch();
        // Wrap the single batch into an async stream DataFusion can pull from
        let stream = futures::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

mod fixture {
    use std::sync::Arc;
    use datafusion::arrow::{array::{BooleanArray, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
    
    pub fn get_sample_batch() -> RecordBatch {
        // Define the arrays (columns)
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
        let active_array = BooleanArray::from(vec![true, false, true]);

        // Define the schema matching the columns
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);

        // Create the RecordBatch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        ).unwrap()
    }

}


