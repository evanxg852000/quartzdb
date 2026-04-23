use std::sync::Arc;

use anyhow::{Result, anyhow};
use proto::quartzdb::ProtoDocumentBatch;
use tokio::sync::oneshot;

use crate::{
    common::{index::IndexConfig, processors::Processor},
    storage::storage_impl::StorageImpl,
};

#[derive(Debug, Clone)]
pub struct BatchProcessor {
    storage: Arc<StorageImpl>,
    index_name: String,
    index_config: Arc<IndexConfig>,
}

impl Processor for BatchProcessor {}

impl BatchProcessor {
    pub fn new(
        storage: Arc<StorageImpl>,
        index_name: String,
        index_config: Arc<IndexConfig>,
    ) -> Self {
        Self {
            storage,
            index_name,
            index_config,
        }
    }

    pub async fn put_batch(
        &self,
        batch: ProtoDocumentBatch,
        reply_sender: oneshot::Sender<()>,
    ) -> Result<()> {
        put_batch(
            self.storage.clone(),
            self.index_name.clone(),
            &self.index_config,
            batch,
        )
        .await?;
        reply_sender
            .send(())
            .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
        Ok(())
    }
}

async fn put_batch(
    _storage: Arc<StorageImpl>,
    index_name: String,
    _index_config: &IndexConfig,
    _batch: ProtoDocumentBatch,
) -> Result<()> {
    //TODO: perform the parquet & tantivy magic
    println!("Storing split for {}", index_name);

    // build the split in temporary scratch folder

    // upload it or move it to storage folder

    // publish it

    Ok(())
}
