use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use storage::StorageConfig;

use crate::indexer::config::IndexerConfig;
use crate::metastore::config::MetastoreConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuartzConfig {
    pub address: SocketAddr,
    pub endpoint: String,
    pub data_dir: PathBuf,
    pub metastore: MetastoreConfig,
    pub indexer: IndexerConfig,
    pub storage: StorageConfig,
}

impl Default for QuartzConfig {
    fn default() -> Self {
        let socket_addr = SocketAddr::from(([127, 0, 0, 1], 7280));
        QuartzConfig {
            address: socket_addr,
            endpoint: format!("http://{}", socket_addr),
            data_dir: PathBuf::from("./quartzdb_data"),
            metastore: MetastoreConfig::default(),
            indexer: IndexerConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}
