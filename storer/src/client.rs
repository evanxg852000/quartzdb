use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use crate::service::{StorerPutRequest, StorerQueryRequest, StorerQueryResponse, StorerService};

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
    async fn put(&self, request: StorerPutRequest) -> Result<()> {
        self.inner.put(request).await
    }

    async fn query(&self, query: StorerQueryRequest) -> Result<StorerQueryResponse> {
        self.inner.query(query).await
    }
}
