pub mod object_storage;
pub mod local_storage;
pub mod cachable_storage;
pub mod remote_storage;
pub mod error;

use std::path::PathBuf;
use std::pin::Pin;
use std::{io, path::Path};
use std::fmt::Debug;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tempfile::TempDir;

pub struct StorgeConfig {

}


// typical config
// local_cached_store -> Cache(Local)
// global_remote -> Remote(local_cached_store, global_uri)
// custom_remote -> Remote(local_cached_store, index_uri)

const CHUNK_SIZE_BYTES: usize = 10 * 1024 * 1024;  // 10MB

type BoxedBytesStream = Pin<Box<dyn Stream<Item = object_store::Result<Bytes>> + Send>>;


#[async_trait]
pub trait Storage:  Send + Sync + Debug {

    fn tempdir(&self) -> io::Result<TempDir> {
        let root = self.root();
        TempDir::new_in(root)
    }

    fn root(&self) -> &Path;

    async fn exists(&self, location: &str) -> io::Result<bool>;

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()>;

    async fn put_large(&self, from: &str, to: &str) -> io::Result<()>;

    async fn put_stream(&self, mut stream: BoxedBytesStream, to: &str) -> io::Result<()>;

    async fn get(&self, location: &str) -> io::Result<Bytes>;

    async  fn get_as_stream(&self, location: &str) -> io::Result<BoxedBytesStream>;

    async fn delete(&self, location: &str) -> io::Result<()>;
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{cachable_storage::{CachableStorage, CacheConfig}, local_storage::LocalStorage, remote_storage::RemoteStorage};

    use super::*;

    #[tokio::test]
    async fn test_storage_layering() -> anyhow::Result<()> {
        let tempdir = TempDir::new_in("base")?;
        let dir = tempdir.path().join("./data-dir");
        tokio::fs::create_dir_all(dir.clone()).await?;

        let local = Arc::new(LocalStorage::new(dir).await?);
        let cached = Arc::new(CachableStorage::new(local, CacheConfig::default())?);
        let golbal_storage = RemoteStorage::new(cached.clone(), "s3://foo/bar").await?;
        let my_index_storage = RemoteStorage::new(cached.clone(), "s3://my/index/bucket").await?;
        
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
