use std::sync::Arc;
use std::path::PathBuf;

use crate::metastore::local::LocalMetastore;
use crate::metastore::client::MetastoreClient;

pub struct MetastoreService{
    metastore: Arc<LocalMetastore>,
}

impl MetastoreService {
    pub fn new(data_dir: PathBuf) -> Self {
        MetastoreService { metastore: Arc::new(LocalMetastore::new(data_dir)) }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        self.metastore.load_indexes().await
    }

    pub fn new_client(&self) -> MetastoreClient {
        MetastoreClient::new(self.metastore.clone())
    }
}

