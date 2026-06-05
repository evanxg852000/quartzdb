use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Storage, cachable_storage::{CachableStorage, CacheConfig}, local_storage::LocalStorage};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StorageConfig {
    pub directory: PathBuf,
    pub cache: Option<CacheConfig>,
    pub uri: Option<Url>, // object storage url
}

impl StorageConfig {
    pub fn new(directory: &str) -> Self {
        Self {
            directory: PathBuf::from(directory),
            cache: None,
            uri: None,
        }
    }

    pub fn derive(&self, directory: &str, cache: Option<CacheConfig>) -> Self {
        Self {
            directory: self.directory.clone().join(directory),
            cache,
            uri: self.uri.clone(),
        }
    }

    pub async fn build(&self) -> Result<Arc<dyn Storage>> {
        tokio::fs::create_dir_all(&self.directory).await?;
        let mut storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&self.directory).await?);
        if let Some(cache_config) = &self.cache {
            storage = Arc::new(CachableStorage::new(storage, cache_config.clone())?);
        }
        if let Some(remote_uri) = &self.uri {
            storage = storage.derive_remote(remote_uri).await?;
        }
        Ok(storage)
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./quartzdb_data"),
            cache: None,
            uri: None,
        }
    }
}
