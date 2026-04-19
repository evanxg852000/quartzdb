use  std::sync::Arc;
use crate::metastore::local::LocalMetastore;
use crate::common::index::IndexMeta;

#[derive(Debug, Clone)]
pub struct MetastoreClient {
    inner_impl: Arc<LocalMetastore>,
} 

impl MetastoreClient {
    pub fn new(inner_impl: Arc<LocalMetastore>) -> Self {
        MetastoreClient { inner_impl }
    }

    pub async fn create_index(&self, index_meta: IndexMeta) -> anyhow::Result<()> {
        self.inner_impl.create_index(index_meta).await
    }

    pub async fn delete_index(&self, index_name: &str) -> anyhow::Result<()> {
        self.inner_impl.delete_index(index_name).await
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMeta>> {
        self.inner_impl.list_indexes().await
    }

}
