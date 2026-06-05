use async_trait::async_trait;

use anyhow::Result;

use common::catalog::{SplitMeta, TableMeta};

use crate::events::MetastoreEvent;


#[async_trait]
pub trait MetastoreService: Send + Sync + 'static {
    // pool metastore events & also serves a client heartbeat
    async fn fetch_events(&self, last_checkin: Option<u64>) -> Result<Vec<MetastoreEvent>>;
    
    async fn list_tables(&self) -> Result<Vec<TableMeta>>;
    async fn put_table(&self, table_meta: TableMeta) -> Result<()>;
    async fn get_table(&self, table_name: &str) -> Result<TableMeta>;
    async fn delete_table(&self, table_name: &str) -> Result<()>;

    async fn put_split(&self, split_meta: SplitMeta) -> Result<()>;


}





