use serde::{Deserialize, Serialize};
use storage::cachable_storage::CacheConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    pub address: String,
    pub cache: Option<CacheConfig>,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        IndexerConfig {
            address: "localhost:8081".to_string(),
            cache: None,
        }
    }
}
