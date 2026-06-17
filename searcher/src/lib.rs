pub mod client;
pub mod commands;
pub mod configs;
pub mod search_processor;
pub mod service;
pub mod web;

use anyhow::Result;
use axum::Router;
use metastore::client::MetastoreClient;
use storage::configs::StorageConfig;
use storer::client::StorerClient;

use crate::{configs::SearcherConfig, service::SearcherService, web::setup_http_routes};

pub struct SearcherServiceStartResult {
    pub searcher_http_router: Router,
}

pub async fn start_searcher_service(
    searcher_config: &SearcherConfig,
    storage_config: &StorageConfig,
    metastore_client: MetastoreClient,
    storer_client: Option<StorerClient>,
) -> Result<SearcherServiceStartResult> {
    // if storer_client is None, it means storer is disabled,
    // and we should try to discover storer clients from metastore
    let storer_client = storer_client.ok_or_else(|| anyhow::anyhow!("TODO:"))?;

    let mut searcher_service = SearcherService::try_new(
        searcher_config,
        storage_config,
        metastore_client.clone(),
        storer_client.clone(),
    )
    .await?;
    searcher_service.start().await?;

    let searcher_client = searcher_service.new_client();
    let searcher_http_router = setup_http_routes(searcher_client);
    Ok(SearcherServiceStartResult {
        searcher_http_router,
    })
}
