use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use storage::StorageConfig;

use crate::indexer::config::IndexerConfig;
use crate::metastore::config::MetastoreConfig;
use crate::storer::config::StorerConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuartzConfig {
    pub address: SocketAddr,
    pub endpoint: String,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub indexer: IndexerConfig,
    pub storer: StorerConfig,
}

impl Default for QuartzConfig {
    fn default() -> Self {
        let socket_addr = SocketAddr::from(([127, 0, 0, 1], 7280));
        QuartzConfig {
            address: socket_addr,
            endpoint: format!("http://{}", socket_addr),
            storage: StorageConfig::default(),
            metastore: MetastoreConfig::default(),
            indexer: IndexerConfig::default(),
            storer: StorerConfig::default(),
        }
    }
}
