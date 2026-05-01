use std::path::PathBuf;

use tokio::fs;
use tokio_util::io::ReaderStream;

use crate::cli::utils;
use crate::common::index::IndexMeta;
use crate::common::models::{ApiError, ApiOk};
use crate::common::{config::QuartzConfig, models::AppInfo};
use crate::indexer::service::IndexerService;
use crate::metastore::service::MetastoreService;
use crate::storer::service::StorerService;

pub async fn handle_run(config: QuartzConfig) -> anyhow::Result<()> {
    // initilize data directory
    let data_dir = config.storage.directory.clone();
    tokio::fs::create_dir_all(&data_dir).await?;

    let mut metastore_service = MetastoreService::try_new(&config).await?;
    metastore_service.start().await?;
    let metastore_client = metastore_service.new_client();

    let mut storer_service = StorerService::new(&config, metastore_client.clone()).await?;
    storer_service.start().await?;
    let storer_client = storer_service.new_client();

    let mut ingest_service = IndexerService::new(metastore_client.clone(), storer_client);
    ingest_service.start().await?;
    let ingest_client = ingest_service.new_client();

    let services_router = axum::Router::new()
        .merge(crate::metastore::web::setup_web_routes(metastore_client))
        .merge(crate::indexer::web::setup_web_routes(ingest_client));

    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::get(|| async { axum::Json(AppInfo::new()) }),
        )
        .nest("/api/v1", services_router);

    // run our app with hyper, listening globally on port 3000
    let server_address = &config.address;
    println!("QuartzDB listening @ http://{}", server_address);
    let listener = tokio::net::TcpListener::bind(server_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}

pub async fn handle_index_list(config: QuartzConfig) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/v1/metastore/indexes", config.endpoint))
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let api_ok = response.json::<ApiOk<Vec<IndexMeta>>>().await?;
            let indexes = api_ok.data.unwrap_or_else(|| vec![]);
            for index_meta in indexes {
                println!("{}", index_meta.name);
            }
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to list indexes: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_index_create(config: QuartzConfig, file: PathBuf) -> anyhow::Result<()> {
    let index_meta = utils::read_as_object::<IndexMeta>(file.as_path()).await?;

    let client = reqwest::Client::new();
    let response = client
        .put(format!("{}/api/v1/metastore/indexes", config.endpoint))
        .json(&index_meta)
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let _ = response.json::<ApiOk<IndexMeta>>().await?;
            println!("Index created successfuly")
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to create index: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_index_delete(config: QuartzConfig, index_name: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .delete(format!(
            "{}/api/v1/metastore/indexes/{}",
            config.endpoint, index_name
        ))
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let _ = response.json::<ApiOk<()>>().await?;
            println!("Index deleted successfuly")
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to delete index: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_ingest(
    config: QuartzConfig,
    index_name: &str,
    file_path: PathBuf,
) -> anyhow::Result<()> {
    let file = fs::File::open(file_path).await?;
    let stream = ReaderStream::new(file);
    let body = reqwest::Body::wrap_stream(stream);
    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/api/v1/ingest/ndjson/{}",
            config.endpoint, index_name
        ))
        .body(body)
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let _ = response.json::<ApiOk<()>>().await?;
            println!("Data successfuly ingested")
        }
        false => {
            //let api_error = response.json::<ApiError>().await?;
            println!("ERROR: {} ", response.status());
            // eprintln!("Failed to ingest data: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_query(
    _config: QuartzConfig,
    _index_name: &str,
    query: &str,
) -> anyhow::Result<()> {
    println!("Executing query: {}", query);
    Ok(())
}
