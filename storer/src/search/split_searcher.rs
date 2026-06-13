use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, error::{DataFusionError, Result}, execution::SendableRecordBatchStream};

use crate::{search::context::TableSearchContext, split::reader::SplitReader};

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
        // filters: &[Expr],
        fts_expr: Option<String>,
        limit: Option<u64>,
    ) -> Result<SendableRecordBatchStream> {
        println!("table_name: {}", context.get_table_meta().name);
        println!("split_id: {}", split_id);
        println!("projection: {:?}", projection);
        println!("fts_expr: {:?}", fts_expr);
        println!("limit: {:?}", limit);
        
        // open reader & cache it 
        let split_reader = Arc::new(SplitReader::try_new(context, &split_id).await
            .map_err(|err| DataFusionError::Execution(format!("Failed to open split: {}", err)))?);
        split_reader.search(schema, projection, fts_expr, limit).await
    }
}
