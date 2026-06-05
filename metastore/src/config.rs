use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetastoreType {
    Memory,
    #[serde(rename = "fs")]
    FileSystem,
    Sqlite,
    Postgres { uri: Url },
    // Acts as a client & doesn't spawn a grpc server
    Remote{ uri: Url } 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    #[serde(rename = "type")]
    pub metastore_type: MetastoreType,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        MetastoreConfig {
            metastore_type: MetastoreType::Memory,
        }
    }
}
