use serde::{Deserialize, Serialize};



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    pub address: String,
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            address: "localhost:8081".to_string(),
        }
    }
}
