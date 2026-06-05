use std::sync::Arc;

use storage::Storage;

use crate::{service::{StorerPutRequest, StorerPutRequestInfo, StorerQueryRequest, StorerService}, table_processor_registry::TableProcessorRegistry};


pub struct FileSystemStorerServiceImpl {
    // storage: Arc<dyn Storage>,
    table_processor_registry: Arc<TableProcessorRegistry>
}

impl FileSystemStorerServiceImpl {
    pub fn new(table_processor_registry: Arc<TableProcessorRegistry>) -> Self {
        Self { table_processor_registry }
    }
}

#[tonic::async_trait]
impl StorerService for FileSystemStorerServiceImpl {
    async fn put(&self, request: StorerPutRequest) -> anyhow::Result<()> {
        let StorerPutRequest{info: StorerPutRequestInfo{ table_name}, data} = request;
        let processor = self.table_processor_registry.get_processor(&table_name).await?;
        processor.process_batch(data).await?;
        Ok(())
    }

    async fn query(&self, query: StorerQueryRequest) -> anyhow::Result<()> {
        println!("Received Query request: {:?}", query);
        Ok(())
    }
}
