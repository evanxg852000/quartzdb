use async_trait::async_trait;

use std::{
    io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, WriteMultipart,
    parse_url, path::Path as StorePath,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};
use url::Url;

use crate::{BoxedBytesStream, CHUNK_SIZE_BYTES, Storage, remote_storage::RemoteStorage};

#[derive(Debug, Clone)]
pub struct ObjectStorageWrapper {
    root: PathBuf,
    innger_storage: Arc<dyn ObjectStore>,
}

impl ObjectStorageWrapper {
    pub async fn new(url: &Url) -> io::Result<Self> {
        let (store, path) = parse_url(&url)?;
        Ok(Self {
            root: PathBuf::from("/").join(path.as_ref()),
            innger_storage: Arc::new(store),
        })
    }

    pub async fn new_local_fs(directory: impl Into<PathBuf>) -> io::Result<Self> {
        let directory_absolute_path = tokio::fs::canonicalize(directory.into()).await?;
        let url = Url::from_str(&format!("file://{}", directory_absolute_path.display()))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        Self::new(&url).await
    }
}

#[async_trait]
impl Storage for ObjectStorageWrapper {
    fn root(&self) -> &Path {
        &self.root
    }

    async fn exists(&self, location: &str) -> io::Result<bool> {
        let location = to_store_path(self.root().join(location))?;
        let exist = match self.innger_storage.head(&location).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(err) => return Err(err.into()),
        };
        Ok(exist)
    }

    async fn derive_remote(self: Arc<Self>, url: &Url) -> io::Result<Arc<dyn Storage>> {
        let derived_storage = RemoteStorage::new(self.clone(), url).await?;
        Ok(Arc::new(derived_storage))
    }

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()> {
        let to = to_store_path(self.root().join(to))?;
        self.innger_storage
            .put_opts(&to, PutPayload::from_bytes(data), PutOptions::default())
            .await?;
        Ok(())
    }

    async fn put_large(&self, from: &PathBuf, to: &PathBuf) -> io::Result<()> {
        let to = to_store_path(self.root().join(to))?;
        let source_file = File::open(from).await?;
        let mut reader = BufReader::new(source_file);
        let uploader = self
            .innger_storage
            .put_multipart_opts(&to, PutMultipartOptions::default())
            .await?;
        let mut upload_writer = WriteMultipart::new(uploader);

        let mut buffer = vec![0u8; CHUNK_SIZE_BYTES]; // 10MB chunks
        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 {
                break; // EOF
            }
            upload_writer.write(&buffer[..n]);
        }
        upload_writer.finish().await?;
        Ok(())
    }

    async fn put_stream(&self, mut stream: BoxedBytesStream, to: &str) -> io::Result<()> {
        let to = to_store_path(self.root().join(to))?;
        let uploader = self
            .innger_storage
            .put_multipart_opts(&to, PutMultipartOptions::default())
            .await?;
        let mut upload_writer = WriteMultipart::new(uploader);
        while let Some(chunk) = stream.next().await {
            let chunk_bytes = chunk?;
            upload_writer.write(&chunk_bytes);
        }
        upload_writer.finish().await?;
        Ok(())
    }

    async fn get(&self, location: &str) -> io::Result<Bytes> {
        let location = to_store_path(self.root.join(location))?;
        let result = self.innger_storage.get(&location).await?;
        let data = result.bytes().await?;
        Ok(data)
    }

    async fn get_as_stream(&self, location: &str) -> io::Result<BoxedBytesStream> {
        let location = to_store_path(self.root.join(location))?;
        let stream = self.innger_storage.get(&location).await?.into_stream();
        Ok(stream)
    }

    async fn delete(&self, location: &str) -> io::Result<()> {
        let location = to_store_path(self.root().join(location))?;
        self.innger_storage.delete(&location).await?;
        Ok(())
    }
}

fn to_store_path(path: PathBuf) -> io::Result<StorePath> {
    path.to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Path is invalid"))
        .map(StorePath::from)
}
