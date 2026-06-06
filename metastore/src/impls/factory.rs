use std::sync::Arc;

use anyhow::Result;
use storage::Storage;

use crate::{
    config::{MetastoreConfig, MetastoreType},
    impls::{
        file_system::FileSystemMetastoreServiceImpl, grpc_client::GrpcClientMetastoreServiceImpl,
    },
    service::MetastoreService,
};

pub struct MetastoreFactory;

impl MetastoreFactory {
    pub async fn build(
        storage: Arc<dyn Storage>,
        config: &MetastoreConfig,
    ) -> Result<Arc<dyn MetastoreService>> {
        let data_dir = storage.root().to_path_buf();
        let metastore: Arc<dyn MetastoreService> = match &config.metastore_type {
            MetastoreType::FileSystem => {
                Arc::new(FileSystemMetastoreServiceImpl::try_new(&data_dir).await?)
            }
            MetastoreType::Remote { uri } => {
                Arc::new(GrpcClientMetastoreServiceImpl::try_new(uri.to_string()).await?)
            }
            // TODO: implement other types
            _ => unimplemented!("Metastore type not implemented yet"),
        };
        Ok(metastore)
    }
}
