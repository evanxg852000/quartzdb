use std::net::SocketAddr;

use ingester::configs::IngesterConfig;
use serde::{Deserialize, Serialize};
use storage::configs::StorageConfig;
use metastore::config::MetastoreConfig;
use storer::configs::StorerConfig;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SearcherConfig {
    pub enable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuartzConfig {
    pub id: String,
    pub address: SocketAddr,
    pub target: Option<String>,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub storer: StorerConfig,
    pub ingester: IngesterConfig,
    pub searcher: SearcherConfig,
}


impl Default for QuartzConfig {
    fn default() -> Self {
        QuartzConfig {
            id: "quartzdb-node".into(),
            address: SocketAddr::from(([127, 0, 0, 1], 7280)),
            target: None,
            storage: StorageConfig::default(),
            metastore: MetastoreConfig::default(),
            storer: StorerConfig::default(),
            ingester: IngesterConfig::default(),
            searcher: SearcherConfig::default(),
        }
    }
}


impl QuartzConfig {
    pub fn http_address(&self) -> SocketAddr {
        self.address
    }

    pub fn grpc_address(&self) -> SocketAddr {
        let mut grpc_address = self.address.clone();
        grpc_address.set_port(self.address.port() + 1);
        grpc_address
    }

    pub fn endpoint(&self) -> String {
        match &self.target {
            Some(url) => url.clone(),
            None => format!("http://{}", self.address.to_string()),
        }
    }
}
