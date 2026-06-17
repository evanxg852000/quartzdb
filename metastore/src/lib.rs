pub mod client;
pub mod config;
pub mod events;
mod impls;
pub mod service;
mod web;
use storage::configs::StorageConfig;

use axum::Router;

use common::proto::grpc_metastore_service_server::GrpcMetastoreServiceServer;

const METASTORE_DIR: &str = "metastore";

use crate::{
    client::MetastoreClient,
    config::{MetastoreConfig, MetastoreType},
    impls::{factory::MetastoreFactory, grpc_server::GrpcServerMetastoreServiceImpl},
    web::setup_http_routes,
};

pub struct MetastoreServiceStartResult {
    pub metastore_client: MetastoreClient,
    pub metastore_http_router: Router,
    pub metastore_grpc_service_opt:
        Option<GrpcMetastoreServiceServer<GrpcServerMetastoreServiceImpl>>,
}

pub async fn start_metastore_service(
    metastore_config: &MetastoreConfig,
    storage_config: &StorageConfig,
    // storage: Arc<dyn Storage>,
) -> anyhow::Result<MetastoreServiceStartResult> {
    let storage = storage_config.derive(METASTORE_DIR, None).build().await?;
    let base_metastore = MetastoreFactory::build(storage, &metastore_config).await?;
    let metastore_client = MetastoreClient::new(base_metastore.clone());
    let metastore_http_router = setup_http_routes(metastore_client.clone());
    let metastore_grpc_service_opt = match &metastore_config.metastore_type {
        MetastoreType::Remote { .. } => None,
        _ => {
            let grpc_server_impl = GrpcServerMetastoreServiceImpl::new(base_metastore.clone());
            Some(GrpcMetastoreServiceServer::new(grpc_server_impl))
        }
    };
    Ok(MetastoreServiceStartResult {
        metastore_client,
        metastore_http_router,
        metastore_grpc_service_opt,
    })
}
