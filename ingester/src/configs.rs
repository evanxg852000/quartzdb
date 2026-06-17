use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngesterConfig {
    pub enable: bool,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        IngesterConfig { enable: false }
    }
}
