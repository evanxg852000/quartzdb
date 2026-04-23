use tokio::sync::broadcast::{self, Sender};

use crate::common::index::IndexMeta;
use crate::metastore::events::{MetastoreEvent, MetastoreEventStream};
use crate::metastore::local::LocalMetastore;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MetastoreClient {
    inner_impl: Arc<LocalMetastore>,
    mailbox: Sender<MetastoreEvent>,
}

impl MetastoreClient {
    pub fn new(inner_impl: Arc<LocalMetastore>) -> Self {
        let (mailbox, _) = broadcast::channel(100);
        MetastoreClient {
            inner_impl,
            mailbox,
        }
    }

    pub async fn put_index(&self, index_meta: IndexMeta) -> anyhow::Result<()> {
        let name = index_meta.name.clone();
        let config = index_meta.config.clone();
        self.inner_impl.put_index(index_meta).await?;
        self.mailbox
            .send(MetastoreEvent::IndexPut { name, config })?;
        Ok(())
    }

    pub async fn delete_index(&self, index_name: &str) -> anyhow::Result<()> {
        self.inner_impl.delete_index(index_name).await?;
        self.mailbox.send(MetastoreEvent::IndexDeleted {
            name: index_name.to_string(),
        })?;
        Ok(())
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMeta>> {
        self.inner_impl.list_indexes().await
    }

    pub fn subscribe_to_events(&self) -> MetastoreEventStream {
        self.mailbox.subscribe()
    }
}
