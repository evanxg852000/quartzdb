use async_trait::async_trait;
use bytes::Bytes;
use url::Url;
use std::{io, path::Path, sync::Arc};

use crate::{BoxedBytesStream, Storage, object_storage::ObjectStorageWrapper};
use anyhow::Result;
use object_store::{ObjectStore, aws::AmazonS3Builder};

#[derive(Debug)]
pub struct RemoteStorage {
    local: Arc<dyn Storage>,
    remote: Arc<dyn Storage>,
}

impl RemoteStorage {
    pub async fn new(storage: Arc<dyn Storage>, url: &Url) -> Result<Self> {
        let remote = ObjectStorageWrapper::new(url).await?;
        Ok(Self {
            local: storage,
            remote: Arc::new(remote),
        })
    }



    pub(crate) fn create_remote_store(uri: impl Into<String>) -> Result<Arc<dyn ObjectStore>> {
        // use object_store::parse_url_opts;
        // use url::Url;
        // let url = Url::parse("s3://my-bucket/data")?;
        // let options = vec![
        //     ("aws_access_key_id", "my_key"),
        //     ("aws_secret_access_key", "my_secret"),
        //     ("endpoint", "http://localhost:9000"), // For MinIO
        //     ("allow_http", "true"),
        // ];

        // let (store, path) = parse_url_opts(&url, options)?;

        let aws_store = AmazonS3Builder::new()
            .with_endpoint(uri)
            .with_bucket_name("my-bucket")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;
        Ok(Arc::new(aws_store))
    }
}

#[async_trait]
impl Storage for RemoteStorage {
    fn root(&self) -> &Path {
        self.local.root()
    }

    async fn exists(&self, location: &str) -> io::Result<bool> {
        let local_exists = self.local.exists(location).await?;
        let remote_exists = self.remote.exists(location).await?;
        Ok(local_exists && remote_exists)
    }

    async fn swap_remote(&self, url: &Url) -> io::Result<Arc<dyn Storage>> {
        let remote = ObjectStorageWrapper::new(url).await?;
        Ok(Arc::new(Self {
            local: self.local.clone(),
            remote: Arc::new(remote),
        }))
    }
    // async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
    //     self.storage.create_dir_all(path).await
    // }

    // async fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
    //     self.storage.remove_dir_all(path).await
    // }

    async fn put(&self, to: &str, data: Bytes) -> io::Result<()> {
        tokio::try_join!(self.local.put(to, data.clone()), self.remote.put(to, data),)?;
        Ok(())
    }

    async fn put_large(&self, from: &str, to: &str) -> io::Result<()> {
        tokio::try_join!(
            self.local.put_large(from, to),
            self.remote.put_large(to, to),
        )?;
        Ok(())
    }

    async fn put_stream(&self, stream: BoxedBytesStream, to: &str) -> io::Result<()> {
        //TODO: tee the stream
        self.local.put_stream(stream, to).await?;
        let local_stream = self.local.get_as_stream(to).await?;
        self.remote.put_stream(local_stream, to).await
    }

    async fn get(&self, location: &str) -> io::Result<Bytes> {
        let local_exists = self.local.exists(location).await?;
        if !local_exists {
            // download to local
            let remote_data = self.remote.get(location).await?;
            self.local.put(location, remote_data.clone()).await?;
            return Ok(remote_data);
        }
        self.local.get(location).await
    }

    async fn get_as_stream(&self, location: &str) -> io::Result<BoxedBytesStream> {
        let local_exists = self.local.exists(location).await?;
        if !local_exists {
            //download
            let remote_stream = self.remote.get_as_stream(location).await?;
            self.local.put_stream(remote_stream, location).await?;
        }
        self.local.get_as_stream(location).await
    }

    async fn delete(&self, location: &str) -> io::Result<()> {
        let local_exist = self.local.exists(location).await?;
        let remote_exist = self.remote.exists(location).await?;
        if local_exist {
            self.local.delete(location).await?;
        }
        if remote_exist {
            self.remote.delete(location).await?;
        }
        Ok(())
    }
}
