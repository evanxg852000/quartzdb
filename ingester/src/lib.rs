mod web;
pub mod configs;
pub mod service;
pub mod client;
pub mod commands;
pub mod document;
mod table_processor_registry;
mod table_processor;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use metastore::client::MetastoreClient;
use storage::Storage;
use storer::client::StorerClient;

use crate::{configs::IngesterConfig, service::IngesterService, web::setup_http_routes};



pub struct IngesterServiceStartResult {
    pub ingester_http_router: Router,
}

pub async fn start_ingester_service(
    config: &IngesterConfig, 
    storage: Arc<dyn Storage>, 
    metastore_client: MetastoreClient,
    storer_client: Option<StorerClient>,
) -> Result<IngesterServiceStartResult> {
    // if storer_client is None, it means storer is disabled,
    // and we should try to discover storer clients from metastore
    let storer_client = storer_client
        .ok_or_else(|| anyhow::anyhow!("TODO:"))?;

    let mut ingester_service = IngesterService::try_new(
        config.clone(),
        storage,
        metastore_client.clone(),
        storer_client.clone()
    ).await?;
    ingester_service.start().await?;
    
    let ingester_client = ingester_service.new_client();
    let ingester_http_router = setup_http_routes(ingester_client);
    Ok(IngesterServiceStartResult {ingester_http_router})
}


