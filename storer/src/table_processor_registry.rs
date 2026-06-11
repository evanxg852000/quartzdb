use std::sync::Arc;

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};
use hashbrown::HashMap;

use common::catalog::TableMeta;
use metastore::client::MetastoreClient;
use metastore::service::MetastoreService;
use storage::Storage;
use tokio::sync::Mutex;

use crate::{search::{tags_filter::SearchTagsFilterCache, worker_manager::SearchWorkerManager}, table_processor::{StorerContext, TableProcessor}};

pub struct TableProcessorRegistry {
    capacity: usize, // max item for eviction support
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    entries: Mutex<HashMap<String, Arc<TableProcessor>>>,
    worker_manager: Arc<SearchWorkerManager>,
    tags_filter_cache: Arc<SearchTagsFilterCache>,
}

impl TableProcessorRegistry {
    pub async fn try_new(
        capacity: usize,
        storage: Arc<dyn storage::Storage>,
        metastore_client: MetastoreClient,
        tags_filter_cache: Arc<SearchTagsFilterCache>,
    ) -> Result<Self> {
        let worker_manager = Arc::new(SearchWorkerManager::try_new(metastore_client.clone())?);
        let tables = metastore_client.list_tables().await?;
        let processors = stream::iter(tables)
            .map(|table_meta| async {
                let processor =
                    Self::create_processor(
                        table_meta, 
                        storage.clone(), 
                        metastore_client.clone(),
                        worker_manager.clone(),
                        tags_filter_cache.clone(),
                    )
                        .await?;
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
            worker_manager,
            tags_filter_cache,
        })
    }

    pub async fn get_processor(&self, table_name: &str) -> Result<Arc<TableProcessor>> {
        let mut entries = self.entries.lock().await;
        let processor = match entries.get(table_name) {
            Some(processor) => processor.clone(),
            None => {
                let table_meta = self.metastore_client.get_table(table_name).await?;
                let worker_manager = self.worker_manager.clone();
                let processor = Self::create_processor(
                    table_meta,
                    self.storage.clone(),
                    self.metastore_client.clone(),
                    worker_manager,
                    self.tags_filter_cache.clone(),
                )
                .await?;
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

    pub async fn refresh_processor(
        &self,
        table_name: &str,
        table_meta: TableMeta,
    ) -> Result<Arc<TableProcessor>> {
        let mut entries = self.entries.lock().await;
        let processor = Self::create_processor(
            table_meta,
            self.storage.clone(),
            self.metastore_client.clone(),
            self.worker_manager.clone(),
            self.tags_filter_cache.clone(),
        )
        .await?;
        entries.insert(table_name.to_string(), processor.clone());
        Ok(processor)
    }

    async fn create_processor(
        table_meta: TableMeta,
        storage: Arc<dyn Storage>,
        metastore_client: MetastoreClient,
        worker_manager: Arc<SearchWorkerManager>,
        tags_filter_cache: Arc<SearchTagsFilterCache>,
    ) -> Result<Arc<TableProcessor>> {
        // // table specific storage
        // let storage = match &table_meta.settings.storage {
        //     Some(settings) => {
        //         storage.derive_remote(&settings.url).await?
        //     },
        //     None => storage,
        // };
        // let tags_filter_cache = Arc::new(SplitTagsFilterCache::new(100));
        let context = Arc::new(
            StorerContext::try_new(
                Arc::new(table_meta), 
                storage,
                metastore_client,
                worker_manager,
                tags_filter_cache,
            ).await?,
        );
        Ok(Arc::new(TableProcessor::new(context)))
    }
}
