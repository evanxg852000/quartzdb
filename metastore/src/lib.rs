pub mod client;
pub mod config;
pub mod events;
mod impls;
mod web;
pub mod service;
use storage::Storage;

use std::sync::Arc;

use axum::Router;

use common::proto::grpc_metastore_service_server::GrpcMetastoreServiceServer;

use crate::{
    client::MetastoreClient,
    config::{MetastoreConfig, MetastoreType},
    impls::{factory::MetastoreFactory, grpc_server::GrpcServerMetastoreServiceImpl}, web::setup_http_routes,
};

pub struct MetastoreServiceStartResult {
    pub metastore_client: MetastoreClient,
    pub metastore_http_router: Router,
    pub metastore_grpc_service_opt: Option<GrpcMetastoreServiceServer<GrpcServerMetastoreServiceImpl>>,
}


// pub async fn setup_grpc_service(
//     storage: Arc<dyn Storage>,
//     config: MetastoreConfig,
// ) -> anyhow::Result<GrpcMetastoreServiceServer<GrpcServerMetastoreServiceImpl>> {
//     let fs_metastore = MetastoreFactory::build(storage, &config).await?;
//     let grpc_server_impl = GrpcServerMetastoreServiceImpl::new(fs_metastore);
//     Ok(GrpcMetastoreServiceServer::new(grpc_server_impl))
// }

pub async fn start_metastore_service(
    config: &MetastoreConfig,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<MetastoreServiceStartResult> {
    let base_metastore = MetastoreFactory::build(storage, &config).await?;
    let metastore_client = MetastoreClient::new(base_metastore.clone());
    let metastore_http_router = setup_http_routes(metastore_client.clone());
    let metastore_grpc_service_opt = match &config.metastore_type {
        MetastoreType::Remote { .. } => None,
        _ =>  {
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
