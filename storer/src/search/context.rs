use std::sync::Arc;

use anyhow::Result;
use common::{catalog::TableMeta, schema::Schema};
use datafusion::arrow::datatypes::SchemaRef;
use storage::Storage;

use crate::search::tags_filter::SearchTagsFilterCache;


#[derive(Debug, Clone)]
pub struct SearchContext {
    storage: Arc<dyn Storage>,
    tags_filter_cache: Arc<SearchTagsFilterCache>,
}

impl SearchContext {
    pub fn new(
        storage: Arc<dyn Storage>, 
        tags_filter_cache: Arc<SearchTagsFilterCache>,
    ) -> Self {
        Self {storage, tags_filter_cache }
    }

    pub fn get_storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub fn get_tags_filter_cache(&self) -> &Arc<SearchTagsFilterCache> {
        &self.tags_filter_cache
    }
}


#[derive(Debug,Clone)]
pub struct TableSearchContext {
    table_meta: Arc<TableMeta>,
    storage: Arc<dyn Storage>, // table specific storage
    tags_filter_cache: Arc<SearchTagsFilterCache>,
}

impl TableSearchContext {

    pub fn try_new(
        table_meta: Arc<TableMeta>,
        context: Arc<SearchContext>,
    ) -> Result<Self> {
        // build table specific storage
        let moved_context = context.clone();
        let storage = match &table_meta.settings.storage {
            Some(settings) => tokio::task::block_in_place(move || {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(async { 
                    moved_context.storage.clone().derive_remote(&settings.url).await
                })
            })?,
            None => context.storage.clone(),
        };

        Ok(Self { table_meta, storage, tags_filter_cache: context.tags_filter_cache.clone() })
    }

    pub fn get_table_meta(&self) -> &Arc<TableMeta> {
        &self.table_meta
    }

    pub fn get_primary_schema(&self) -> SchemaRef {
        Schema::get_primary_schema(&self.table_meta.config)
    }

    pub fn get_storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub fn get_tags_filter_cache(&self) -> &Arc<SearchTagsFilterCache> {
        &self.tags_filter_cache
    }

}
