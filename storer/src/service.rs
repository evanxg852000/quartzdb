
use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;


#[derive(Debug)]
pub struct StorerPutRequestInfo {
    pub table_name: String,
}

#[derive(Debug)]
pub struct StorerPutRequest {
    pub info: StorerPutRequestInfo,
    pub data: RecordBatch,
}

#[derive(Debug)]
pub struct StorerQueryRequest {
    pub query: String,
}

#[async_trait]
pub trait StorerService: Send + Sync + 'static {
    async fn put(&self, request: StorerPutRequest) -> Result<()>;
    async fn query(&self, query: StorerQueryRequest) -> Result<()>;
}
