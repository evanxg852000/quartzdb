pub mod client;
pub mod configs;
pub mod service;
pub mod impls;
pub mod split;
mod table_processor_registry;
mod table_processor;

use std::sync::Arc;

use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use metastore::client::MetastoreClient;
use storage::Storage;

use crate::{client::StorerClient, configs::StorerConfig, impls::{factory::StorerFactory, grpc_server::GrpcServerStorerServiceImpl}, table_processor_registry::TableProcessorRegistry};


pub struct StorerServiceStartResult {
    pub storer_client: StorerClient,
    pub storer_store_grpc_service: FlightServiceServer<GrpcServerStorerServiceImpl>,
    // use datafusion-distribute
    // pub storer_query_grpc_service: Option<FlightServiceServer<QueryService>>,
    // let storer_query_grpc_service = Some(FlightServiceServer::new(/* QueryService implementation */));
}

pub async fn start_storer_service(config: &StorerConfig, storage: Arc<dyn Storage>, metastore_client: MetastoreClient) -> Result<StorerServiceStartResult> {
    let table_processor_registry = Arc::new(TableProcessorRegistry::try_new(500, storage, metastore_client).await?);
    let base_storer = StorerFactory::build(&config, table_processor_registry).await?;
    let grpc_server_impl = GrpcServerStorerServiceImpl::new(base_storer.clone());
    let storer_store_grpc_service = FlightServiceServer::new(grpc_server_impl);
    let storer_client = StorerClient::new(base_storer.clone());
    
    Ok(StorerServiceStartResult {
        storer_client,
        storer_store_grpc_service,
    })
}


