use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetastoreType {
    Local,
    Postgres{
        connection_string: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    pub address: String,
    pub metastore_type: MetastoreType,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        MetastoreConfig {
            address: "localhost:8080".to_string(),
            metastore_type: MetastoreType::Local,
        }
    }
}
