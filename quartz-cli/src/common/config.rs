use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::metastore::config::MetastoreConfig;
use crate::ingest::config::IngestConfig;



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuartzConfig {
    pub address: SocketAddr,
    pub endpoint: String,
    pub data_dir: PathBuf,
    pub metastore: MetastoreConfig,
    pub ingest: IngestConfig,
}

impl Default for QuartzConfig {
    fn default() -> Self {
        let socket_addr = SocketAddr::from(([127, 0, 0, 1], 7280));
        QuartzConfig {
            address: socket_addr,
            endpoint: format!("http://{}", socket_addr),
            data_dir: PathBuf::from("./quartzdb_data"),
            metastore: MetastoreConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}
