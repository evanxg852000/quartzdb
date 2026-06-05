use std::sync::Arc;

use anyhow::Result;

use common::catalog::TableMeta;
use datafusion::arrow::array::RecordBatch;
use metastore::{service::MetastoreService, client::MetastoreClient};
use storage::Storage;

use crate::{document::StorerBatch, split::writter::SplitWriter};

#[derive(Debug, Clone)]
pub struct StorerContext {
    table_meta: Arc<TableMeta>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
}

impl StorerContext {
    pub async fn try_new(
        table_meta: Arc<TableMeta>,
        storage: Arc<dyn Storage>,
        metastore_client: MetastoreClient,
    ) -> Result<Self> {
        let mut table_storage = storage;
        if let Some(index_storage_settings) = &table_meta.settings.storage {
            table_storage = table_storage
                .derive_remote(&index_storage_settings.url)
                .await?;
        }
        Ok(Self {
            table_meta,
            storage: table_storage,
            metastore_client,
        })
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_meta.name
    }
}

#[derive(Debug)]
pub struct TableProcessor {
    context: Arc<StorerContext>,
}

impl TableProcessor {
    pub fn new(context: Arc<StorerContext>) -> Self {
        Self { context }
    }

    pub fn get_context(&self) -> &StorerContext {
        &self.context
    }

    pub async fn process_batch(
        &self,
        batch: RecordBatch, 
    ) -> Result<()> {
        let context = self.context.clone();

        let storage = context.storage.clone();
        let table_name = context.table_meta.name.clone();
        let table_config = &context.table_meta.config;
        
        // build sorted batch
        let mut storer_batch = StorerBatch::try_from_record_batch(table_config, batch)?;
        storer_batch.sort();

        // build split & upload it
        let mut split_writer = SplitWriter::try_new(table_name, storage.clone()).await?;
        split_writer.write(table_config, storer_batch).await?;
        let split_meta = split_writer.finalize().await?;

        // publish it
        context.metastore_client.put_split(split_meta).await?;
        Ok(())
    }
}
