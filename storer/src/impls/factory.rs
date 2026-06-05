use std::sync::Arc;

use anyhow::Result;
use storage::Storage;

use crate::{configs::StorerConfig, impls::file_system::FileSystemStorerServiceImpl, service::StorerService, table_processor_registry::{self, TableProcessorRegistry}};


pub struct StorerFactory{}

impl StorerFactory {
    pub async fn build(_config: &StorerConfig, table_processor_registry: Arc<TableProcessorRegistry>) -> Result<Arc<dyn StorerService>> {
        let storer: Arc<dyn StorerService> = Arc::new(FileSystemStorerServiceImpl::new(table_processor_registry));
        Ok(storer)
    }
}
