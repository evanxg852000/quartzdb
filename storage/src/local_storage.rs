use async_trait::async_trait;
use bytes::Bytes;
use std::{io, path::{Path, PathBuf}, sync::Arc};

use anyhow::Result;

use crate::{BoxedBytesStream, Storage, object_storage::ObjectStorageWrapper};

#[derive(Debug)]
pub struct LocalStorage {
    inner: Arc<dyn Storage>,
}

impl LocalStorage {
    pub async fn new(directory: impl Into<PathBuf>) -> Result<Self> {
        let local_fs_storage = ObjectStorageWrapper::new_local_fs(directory).await?;
        Ok(Self {
            inner: Arc::new(local_fs_storage),
        })
    }

}

#[async_trait]
impl Storage for LocalStorage {

    fn root(&self) -> &Path {
        self.inner.root()
    }

    async fn exists(&self, location: &str) -> io::Result<bool> {
        self.inner.exists(location).await
    }

    // async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
    //     fs::create_dir_all(self.directory.join(path)).await
    // }

    // async fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
    //     fs::remove_dir_all(self.directory.join(path)).await?;
    //     Ok(())
    // }

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()> {
        self.inner.put(to, data).await
    }

    async fn put_large(&self, from: &str, to: &str) -> io::Result<()> {
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
