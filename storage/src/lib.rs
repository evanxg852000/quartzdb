pub mod cachable_storage;
pub mod error;
pub mod local_storage;
pub mod object_storage;
pub mod remote_storage;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::{io, path::Path};
use tempfile::TempDir;
use url::Url;

use crate::cachable_storage::{CachableStorage, CacheConfig};
use crate::local_storage::LocalStorage;
use crate::remote_storage::RemoteStorage;

pub struct StorgeConfig {}

// typical config
// local_cached_store -> Cache(Local)
// global_remote -> Remote(local_cached_store, global_uri)
// custom_remote -> Remote(local_cached_store, index_uri)

const CHUNK_SIZE_BYTES: usize = 10 * 1024 * 1024; // 10MB

type BoxedBytesStream = Pin<Box<dyn Stream<Item = object_store::Result<Bytes>> + Send>>;

#[async_trait]
pub trait Storage: Send + Sync + Debug {
    fn tempdir(&self) -> io::Result<TempDir> {
        let root = self.root();
        TempDir::with_prefix_in("_qtz_temp_", root)
    }

    fn root(&self) -> &Path;

    async fn exists(&self, location: &str) -> io::Result<bool>;

    async fn swap_remote(&self, url: &Url) -> io::Result<Arc<dyn Storage>>;

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()>;

    async fn put_large(&self, from: &str, to: &str) -> io::Result<()>;

    async fn put_stream(&self, mut stream: BoxedBytesStream, to: &str) -> io::Result<()>;

    async fn get(&self, location: &str) -> io::Result<Bytes>;

    async fn get_as_stream(&self, location: &str) -> io::Result<BoxedBytesStream>;

    async fn delete(&self, location: &str) -> io::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StorageConfig {
    pub directory: PathBuf,
    pub cache: Option<CacheConfig>,
    pub uri: Option<Url>, // s3 url
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("."),
            cache: None,
            uri: None,
        }
    }
}

impl StorageConfig {

    pub async fn build(&self) -> Result<Arc<dyn Storage>> {
        let mut storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(&self.directory).await?);
        if let Some(cache_config) = &self.cache {
            storage = Arc::new(CachableStorage::new(storage, cache_config.clone())?);
        }
        if let Some(remote_uri) = &self.uri {
            storage = Arc::new(RemoteStorage::new(storage, remote_uri).await?);
        }
        Ok(storage)
    }

}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        cachable_storage::{CachableStorage, CacheConfig},
        local_storage::LocalStorage,
        remote_storage::RemoteStorage,
    };

    use super::*;

    #[tokio::test]
    async fn test_storage_layering() -> anyhow::Result<()> {
        let tempdir = TempDir::new_in("base")?;
        let dir = tempdir.path().join("./data-dir");
        tokio::fs::create_dir_all(dir.clone()).await?;

        let local = Arc::new(LocalStorage::new(dir).await?);
        let cached = Arc::new(CachableStorage::new(local, CacheConfig::default())?);
        let golbal_storage = RemoteStorage::new(cached.clone(), &Url::parse("s3://foo/bar").unwrap()).await?;
        let my_index_storage = RemoteStorage::new(cached.clone(), &Url::parse("s3://my/index/bucket").unwrap()).await?;

        // s.exists("location".into()).await?;

        Ok(())
    }
}

// c.put("foo/bar/baz.bin", Bytes::from_static(b"evance")).await?;

// let store = storage::object_storage::ObjectStorageWrapper::new_local_fs("./data-dir".into()).await?;
// store.create_dir_all(&PathBuf::from("foo/bar")).await?;
// store.remove_dir_all(&PathBuf::from("foo/bar")).await?;

// use tokio::sync::mpsc;
// use tokio_stream::wrappers::ReceiverStream;
// use futures::StreamExt;

// async fn tee(stream: BoxedBytesStream) -> (BoxedBytesStream, BoxedBytesStream) {
//     let (tx, rx) = tokio::sync::broadcast::channel(10);

//     tokio_stream::wrappers::BroadcastStream::new(rx.)

//     tokio::spawn(async move {
//         let chunk = Bytes::from("Hello, object store!");
//         let _ = tx.send(chunk).await;
//         // Channel closes when tx is dropped
//     });

//     tokio::spawn(async move {
//         let chunk = Bytes::from("Hello, object store!");
//         let _ = tx.send(chunk).await;
//         // Channel closes when tx is dropped
//     });
// }
