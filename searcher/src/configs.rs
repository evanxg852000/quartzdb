use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearcherConfig {
    pub enable: bool,
}

impl Default for SearcherConfig {
    fn default() -> Self {
        SearcherConfig { enable: false }
    }
}
