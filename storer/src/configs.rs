use serde::{Deserialize, Serialize};
use storage::cachable_storage::CacheConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorerConfig {
    pub enable: bool,
    pub cache: Option<CacheConfig>,
}

impl Default for StorerConfig {
    fn default() -> Self {
        StorerConfig {
            enable: false,
            cache: None,
        }
    }
}
