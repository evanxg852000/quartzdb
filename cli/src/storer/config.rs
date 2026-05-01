use serde::{Deserialize, Serialize};
use storage::{StorageConfig, cachable_storage::CacheConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorerConfig {
    pub address: String,
    pub cache: Option<CacheConfig>,
}

impl Default for StorerConfig {
    fn default() -> Self {
        StorerConfig {
            address: "localhost:8081".to_string(),
            cache: None,
        }
    }
}
