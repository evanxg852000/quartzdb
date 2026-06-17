use std::{fmt::Debug, sync::Arc};

use crate::{events::MetastoreEvent, service::MetastoreService};
use anyhow::Result;
use common::catalog::SplitMeta;

#[derive(Clone)]
pub struct MetastoreClient {
    inner: Arc<dyn MetastoreService>,
}

impl MetastoreClient {
    pub fn new(inner: Arc<dyn MetastoreService>) -> Self {
        Self { inner }
    }
}

impl Debug for MetastoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreClient").finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl MetastoreService for MetastoreClient {
    async fn fetch_events(&self, last_checkin: Option<u64>) -> Result<Vec<MetastoreEvent>> {
        self.inner.fetch_events(last_checkin).await
    }

    async fn list_tables(&self) -> Result<Vec<common::catalog::TableMeta>> {
        self.inner.list_tables().await
    }

    async fn put_table(&self, table_meta: common::catalog::TableMeta) -> Result<()> {
        self.inner.put_table(table_meta).await
    }

    async fn get_table(&self, table_name: &str) -> Result<common::catalog::TableMeta> {
        self.inner.get_table(table_name).await
    }

    async fn delete_table(&self, table_name: &str) -> Result<()> {
        self.inner.delete_table(table_name).await
    }

    async fn put_split(&self, split_meta: SplitMeta) -> Result<()> {
        self.inner.put_split(split_meta).await
    }
}
