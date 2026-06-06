use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;

use crate::service::StorerService;

#[derive(Clone)]
pub struct StorerClient {
    inner: Arc<dyn StorerService>,
}

impl Debug for StorerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorerClient").finish_non_exhaustive()
    }
}

impl StorerClient {
    pub fn new(inner: Arc<dyn StorerService>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl StorerService for StorerClient {
    async fn put(&self, table_name: &str, record_batch: RecordBatch) -> Result<()> {
        self.inner.put(table_name, record_batch).await
    }

    async fn search(&self, table_name: &str, query: &str) -> Result<RecordBatch> {
        self.inner.search(table_name, query).await
    }
}
