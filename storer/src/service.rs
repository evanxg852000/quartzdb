use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;

// #[derive(Debug)]
// pub struct StorerPutRequestInfo {
//     pub table_name: String,
// }

// #[derive(Debug)]
// pub struct StorerPutRequest {
//     pub info: StorerPutRequestInfo,
//     pub data: RecordBatch,
// }

// #[derive(Debug)]
// pub struct StorerSearchRequest {
//     pub table_name: String,
//     pub query: String,
// }

// #[derive(Debug, Default)]
// pub struct StorerSearchResponse {
//     pub batches: Vec<RecordBatch>,
// }

#[async_trait]
pub trait StorerService: Send + Sync + 'static {
    async fn put(&self, table_name: &str, record_batch: RecordBatch) -> Result<()>;
    async fn search(&self, table_name: &str, query: &str) -> Result<RecordBatch>;
}
