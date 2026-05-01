use std::sync::Arc;

use anyhow::Result;

use crate::common::config::QuartzConfig;
use crate::metastore::client::MetastoreClient;
use crate::metastore::local::LocalMetastore;

pub struct MetastoreService {
    metastore: Arc<LocalMetastore>,
}

impl MetastoreService {
    pub async fn try_new(config: &QuartzConfig) -> Result<Self> {
        //TO FIX: create metastore dir since we are using local
        let local_metastore = LocalMetastore::try_new(&config.storage.directory).await?;
        Ok(MetastoreService {
            metastore: Arc::new(local_metastore),
        })
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        self.metastore.load_indexes().await
    }

    pub fn new_client(&self) -> MetastoreClient {
        MetastoreClient::new(self.metastore.clone())
    }
}
