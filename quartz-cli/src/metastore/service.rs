use std::path::PathBuf;
use std::sync::Arc;

use crate::metastore::client::MetastoreClient;
use crate::metastore::local::LocalMetastore;

pub struct MetastoreService {
    metastore: Arc<LocalMetastore>,
}

impl MetastoreService {
    pub fn new(data_dir: PathBuf) -> Self {
        MetastoreService {
            metastore: Arc::new(LocalMetastore::new(data_dir)),
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        self.metastore.load_indexes().await
    }

    pub fn new_client(&self) -> MetastoreClient {
        MetastoreClient::new(self.metastore.clone())
    }
}
