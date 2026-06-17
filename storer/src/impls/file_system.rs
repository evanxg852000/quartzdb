use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;

use crate::{
    service::StorerService,
    table_processor_registry::TableProcessorRegistry,
};

pub struct FileSystemStorerServiceImpl {
    table_processor_registry: Arc<TableProcessorRegistry>,
}

impl FileSystemStorerServiceImpl {
    pub fn new(table_processor_registry: Arc<TableProcessorRegistry>) -> Self {
        Self {
            table_processor_registry,
        }
    }
}

#[tonic::async_trait]
impl StorerService for FileSystemStorerServiceImpl {
    async fn put(&self, table_name: &str, record_batch: RecordBatch) -> Result<()> {
        let processor = self
            .table_processor_registry
            .get_processor(&table_name)
            .await?;
        processor.put(record_batch).await?;
        Ok(())
    }

    async fn search(&self, table_name: &str, query: &str) -> Result<RecordBatch> {
        let processor = self
            .table_processor_registry
            .get_processor(&table_name)
            .await?;
        let record_batch = processor.search(&query).await?;
        Ok(record_batch)
    }
}
