use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    pub address: String,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        IndexerConfig {
            address: "localhost:8081".to_string(),
        }
    }
}
