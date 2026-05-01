use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use url::Url;

use hashbrown::HashMap;

use crate::{BoxedBytesStream, Storage, remote_storage::RemoteStorage};
use anyhow::Result;

#[derive(Debug, Default, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    // Specify a period after which an entry can be considered for eviction
    Maturation(Duration),
    #[default]
    Lru,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    /// Available disk space in bytes
    pub capacity: usize,
    pub policy: CachePolicy,
}

#[derive(Debug, Default)]
pub struct CachItemMeta {
    created_at: i64,
    accessed_at: i64,
}

#[derive(Debug)]
pub struct CachableStorage {
    inner: Arc<dyn Storage>,
    items: HashMap<PathBuf, CachItemMeta>,
    config: CacheConfig,
    managed_prefixes: Vec<String>, // any item with this prefix will be considered in this cache
    used_disk_capacity: usize,     // consumed disk size in bytes
}

impl CachableStorage {
    pub fn new(storage: Arc<dyn Storage>, config: CacheConfig) -> Result<Self> {
        Ok(Self {
            inner: storage,
            items: HashMap::new(),
            config,
            managed_prefixes: vec![],
            used_disk_capacity: 0,
        })
    }
}

#[async_trait]
impl Storage for CachableStorage {
    fn root(&self) -> &Path {
        self.inner.root()
    }

    async fn exists(&self, location: &str) -> io::Result<bool> {
        self.inner.exists(location).await
    }

    async fn derive_remote(self: Arc<Self>, url: &Url) -> io::Result<Arc<dyn Storage>> {
        let storage = RemoteStorage::new(self.clone(), url).await?;
        Ok(Arc::new(storage))
    }

    // async fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
    //     self.storage.remove_dir_all(path).await
    // }

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()> {
        self.inner.put(to, data).await
    }

    async fn put_large(&self, from: &PathBuf, to: &PathBuf) -> io::Result<()> {
        self.inner.put_large(from, to).await
    }

    async fn put_stream(&self, stream: BoxedBytesStream, to: &str) -> io::Result<()> {
        self.inner.put_stream(stream, to).await
    }

    async fn get(&self, location: &str) -> io::Result<Bytes> {
        self.inner.get(location).await
    }

    async fn get_as_stream(&self, location: &str) -> io::Result<BoxedBytesStream> {
        self.inner.get_as_stream(location).await
    }

    async fn delete(&self, location: &str) -> io::Result<()> {
        self.inner.delete(location).await
    }
}
