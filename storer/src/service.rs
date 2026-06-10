use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;

#[async_trait]
pub trait StorerService: Send + Sync + 'static {
    async fn put(&self, table_name: &str, record_batch: RecordBatch) -> Result<()>;
    async fn search(&self, table_name: &str, query: &str) -> Result<RecordBatch>;
}
