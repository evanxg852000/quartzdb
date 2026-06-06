use std::sync::Arc;

use crate::{
    service::{
        StorerPutRequest, StorerPutRequestInfo, StorerQueryRequest, StorerQueryResponse,
        StorerService,
    },
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
    async fn put(&self, request: StorerPutRequest) -> anyhow::Result<()> {
        let StorerPutRequest {
            info: StorerPutRequestInfo { table_name },
            data,
        } = request;
        let processor = self
            .table_processor_registry
            .get_processor(&table_name)
            .await?;
        processor.process_batch(data).await?;
        Ok(())
    }

    async fn query(&self, query: StorerQueryRequest) -> anyhow::Result<StorerQueryResponse> {
        println!("Received Query request: {:?}", query);
        Ok(StorerQueryResponse::default())
    }
}
