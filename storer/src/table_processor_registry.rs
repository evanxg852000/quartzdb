use std::sync::Arc;

use anyhow::Result;
use hashbrown::HashMap;
use futures::stream::{self, StreamExt, TryStreamExt};

use common::catalog::TableMeta;
use metastore::client::MetastoreClient;
use metastore::service::MetastoreService;
use storage::Storage;
use tokio::sync::Mutex;

use crate::table_processor::{StorerContext, TableProcessor};

pub struct TableProcessorRegistry {
    capacity: usize,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    entries: Mutex<HashMap<String, Arc<TableProcessor>>>,
}

impl TableProcessorRegistry {
    pub async fn try_new(capacity: usize, storage: Arc<dyn storage::Storage>, metastore_client: MetastoreClient) -> Result<Self> {
        let tables = metastore_client.list_tables().await?;
        let processors = stream::iter(tables)
            .map(|table_meta| async {
                let processor = Self::create_processor(table_meta, storage.clone(), metastore_client.clone()).await?;
                anyhow::Result::<_>::Ok(processor)
            })
            .buffer_unordered(20)
            .try_collect::<Vec<_>>()
            .await?;
        let mut entries = HashMap::new();
        for processor in processors {
            let table_name = processor.get_context().get_table_name();
            entries.insert(table_name.to_string(), processor);
        }
        Ok(Self {
            capacity,
            storage,
            metastore_client,
            entries: Mutex::new(entries),
        })
    }

    pub async fn get_processor(&self, table_name: &str) -> Result<Arc<TableProcessor>> {
        let mut entries = self.entries.lock().await;
        let processor = match entries.get(table_name) {
            Some(processor) => processor.clone(),
            None => {
                let table_meta = self.metastore_client.get_table(table_name).await?;
                let processor = Self::create_processor(table_meta, self.storage.clone(), self.metastore_client.clone()).await?;
                //TODO: check capacity and evict if needed!
                entries.insert(table_name.to_string(), processor.clone());
                processor
            }
        };
        Ok(processor)
    }

    pub async fn remove_processor(&self, table_name: &str) -> Result<()> {
        let mut entries = self.entries.lock().await;
        entries.remove(table_name);
        Ok(())
    }

    pub async fn refresh_processor(&self, table_name: &str, table_meta: TableMeta) -> Result<Arc<TableProcessor>> {
        let mut entries = self.entries.lock().await;
        let processor = Self::create_processor(table_meta, self.storage.clone(), self.metastore_client.clone()).await?;
        entries.insert(table_name.to_string(), processor.clone());
        Ok(processor)
    }

    async fn create_processor(table_meta: TableMeta, storage: Arc<dyn Storage>, metastore_client: MetastoreClient) -> Result<Arc<TableProcessor> >{
        let context = Arc::new(StorerContext::try_new(Arc::new(table_meta), storage, metastore_client).await?);
        Ok(Arc::new(TableProcessor::new(context)))
    }
}


