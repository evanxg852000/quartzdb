use std::net::SocketAddr;

use metastore::config::MetastoreConfig;
use storer::configs::StorerConfig;
use serde::{Deserialize, Serialize};
use storage::configs::StorageConfig;

use crate::indexer::config::IndexerConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuartzConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub node_address: SocketAddr,
    pub http_endpoint: String, // http endpoint when running cli commands, e.g. `quartzdb index list --endpoint http://
    pub metastore_uri: String,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub ingester: Option<IndexerConfig>,
    pub storer: Option<StorerConfig>,
}

impl Default for QuartzConfig {
    fn default() -> Self {
        QuartzConfig {
            cluster_id: "quartzdb-cluster".into(),
            node_id: "node1".into(),
            node_address: SocketAddr::from(([127, 0, 0, 1], 7280)),
            http_endpoint: "http://127.0.0.1:7280".into(),
            metastore_uri: "grpc://127.0.0.1:8081".into(),
            storage: StorageConfig::default(),
            metastore: MetastoreConfig::default(),
            ingester: None,
            storer: None,
        }
    }
}

impl QuartzConfig {
    pub fn http_address(&self) -> SocketAddr {
        self.node_address
    }

    pub fn grpc_address(&self) -> SocketAddr {
        let mut grpc_address = self.node_address.clone();
        grpc_address.set_port(self.node_address.port() + 1);
        grpc_address
    }
}
