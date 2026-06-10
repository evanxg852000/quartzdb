use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, error::Result, execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter};
use storage::Storage;

#[derive(Debug)]
pub struct SplitSearcher {}

impl SplitSearcher {
    pub async fn search(
        storage: Arc<dyn Storage>, 
        table_name: String,
        schema: SchemaRef, 
        split_id: String,
        projection: Vec<u64>,
        fts_expr: Option<String>,
    ) -> Result<SendableRecordBatchStream> {
        println!("split_id: {}", split_id);
        println!("projection: {:?}", projection);
        println!("fts_expr: {:?}", fts_expr);
        // download split
        // open index of fts_expr is not null
        // open parquet 

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


