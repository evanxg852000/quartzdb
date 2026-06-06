pub mod client;
pub mod configs;
mod document;
pub mod impls;
pub mod service;
pub mod split;
pub mod search;
mod table_processor;
mod table_processor_registry;

use std::sync::Arc;

use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion_distributed::{Worker, WorkerServiceServer};
use metastore::client::MetastoreClient;
use storage::configs::StorageConfig;

use crate::{
    client::StorerClient, configs::StorerConfig, impls::{factory::StorerFactory, grpc_server::GrpcServerStorerServiceImpl}, search::worker::SearchWorkerBuilder, table_processor_registry::TableProcessorRegistry
};

const STORER_DIR: &str = "storer";

pub struct StorerServiceStartResult {
    pub storer_client: StorerClient,
    pub storer_store_grpc_service: FlightServiceServer<GrpcServerStorerServiceImpl>,
    pub storer_search_grpc_service: WorkerServiceServer<Worker>,
}

pub async fn start_storer_service(
    storer_config: &StorerConfig,
    storage_config: &StorageConfig,
    metastore_client: MetastoreClient,
) -> Result<StorerServiceStartResult> {
    let storage = storage_config
        .derive(STORER_DIR, storer_config.cache.clone())
        .build()
        .await?;
    let table_processor_registry =
        Arc::new(TableProcessorRegistry::try_new(500, storage, metastore_client).await?);
    let base_storer = StorerFactory::build(&storer_config, table_processor_registry).await?;
    let grpc_server_impl = GrpcServerStorerServiceImpl::new(base_storer.clone());
    let storer_store_grpc_service = FlightServiceServer::new(grpc_server_impl);
    let storer_client = StorerClient::new(base_storer.clone());

    let search_worker = SearchWorkerBuilder::build();
    let storer_search_grpc_service = search_worker.into_worker_server();

    Ok(StorerServiceStartResult {
        storer_client,
        storer_store_grpc_service,
        storer_search_grpc_service,
    })
}
