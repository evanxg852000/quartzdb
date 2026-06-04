use std::net::SocketAddr;
use std::path::PathBuf;

use ingester::IngesterServiceStartResult;
use metastore::MetastoreServiceStartResult;
use storer::StorerServiceStartResult;
use tokio::fs;
use tokio_util::io::ReaderStream;

use crate::cli::utils;
use common::{catalog::TableMeta, models::{ApiError, ApiOk, AppInfo}};
use configs::QuartzConfig;
use tonic::service::Routes;

async fn run_http_server(
    http_router: axum::Router,
    http_address: SocketAddr,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(http_address).await.unwrap();
    println!("QuartzDB listening on http://{}", http_address);
    axum::serve(listener, http_router).await.unwrap();
    Ok(())
}

async fn run_grpc_server(grpc_router: Routes, grpc_address: SocketAddr) -> anyhow::Result<()> {
    println!("QuartzDB internally listening on grpc://{}", grpc_address);
    tonic::transport::Server::builder()
        .add_routes(grpc_router)
        .serve(grpc_address.clone())
        .await
        .unwrap();
    Ok(())
}

pub async fn handle_run(config: QuartzConfig) -> anyhow::Result<()> {
    // initilize data directory
    let data_dir = config.storage.directory.clone();
    tokio::fs::create_dir_all(&data_dir).await?;

    let mut services_router = axum::Router::new();
    let mut grpc_router_builder = Routes::builder();

    // initilize storage
    let storage = config.storage.build().await?;

    // initilize metastore
    let MetastoreServiceStartResult {
        metastore_client,
        metastore_http_router,
        metastore_grpc_service_opt,
    } = metastore::start_metastore_service(&config.metastore, storage.clone()).await?;
    services_router = services_router.merge(metastore_http_router);
    if let Some(metastore_grpc_service) = metastore_grpc_service_opt {
        grpc_router_builder.add_service(metastore_grpc_service);
    }

    // initilize storer
    let storer_client = match config.storer.enable {
        true => {
            let StorerServiceStartResult {
                storer_client,
                storer_store_grpc_service,
                ..
            } = storer::start_storer_service(&config.storer, storage.clone(), metastore_client.clone()).await?;
            grpc_router_builder.add_service(storer_store_grpc_service);
            Some(storer_client)
        },
        false => None,
    };
    
    if config.ingester.enable {
        println!("Starting ingester service...");
        let IngesterServiceStartResult {
            ingester_http_router,
        } = ingester::start_ingester_service(&config.ingester, storage.clone(), metastore_client, storer_client.clone()).await?;
        services_router = services_router.merge(ingester_http_router);
    }



    
    // let mut storer_service = StorerService::new(&config.storer, metastore_client.clone()).await?;
    // storer_service.start().await?;
    // let storer_client = storer_service.new_client();

    // let mut metastore_service = MetastoreService::try_new(&config).await?;
    // metastore_service.start().await?;
    // let metastore_client = metastore_service.new_client();

    // let mut ingest_service = IndexerService::new(metastore_client.clone(), storer_client);
    // ingest_service.start().await?;
    // let ingest_client = ingest_service.new_client();

    let http_router = axum::Router::new()
        .route(
            "/",
            axum::routing::get(|| async { axum::Json(AppInfo::new()) }),
        )
        .nest("/api/v1", services_router);

    let grpc_router = grpc_router_builder.routes();
    tokio::try_join!(
        run_grpc_server(grpc_router, config.grpc_address()),
        run_http_server(http_router, config.http_address())
    )?;
    Ok(())
}

pub async fn handle_table_list(config: QuartzConfig) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/v1/metastore/tables", config.endpoint()))
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let api_ok = response.json::<ApiOk<Vec<TableMeta>>>().await?;
            let tables = api_ok.data.unwrap_or_else(|| vec![]);
            for table_meta in tables {
                println!("{}", table_meta.name);
            }
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to list tables: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_table_put(config: QuartzConfig, file: PathBuf) -> anyhow::Result<()> {
    let table_meta = utils::read_as_object::<TableMeta>(file.as_path()).await?;

    let client = reqwest::Client::new();
    let response = client
        .put(format!("{}/api/v1/metastore/tables", config.endpoint()))
        .json(&table_meta)
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let _ = response.json::<ApiOk<TableMeta>>().await?;
            println!("Table created successfuly")
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to create table: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_table_delete(config: QuartzConfig, table_name: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .delete(format!(
            "{}/api/v1/metastore/tables/{}",
            config.endpoint(), table_name
        ))
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let _ = response.json::<ApiOk<()>>().await?;
            println!("Table deleted successfuly")
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
            eprintln!("Failed to delete table: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_ingest(
    config: QuartzConfig,
    table_name: &str,
    file_path: PathBuf,
) -> anyhow::Result<()> {
    let file = fs::File::open(file_path).await?;
    let stream = ReaderStream::new(file);
    let body = reqwest::Body::wrap_stream(stream);
    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/api/v1/ingest/ndjson/{}",
            config.endpoint(), table_name
        ))
        .body(body)
        .send()
        .await?;
    match response.status().is_success() {
        true => {
            let api_response = response.json::<ApiOk<serde_json::Value>>().await?;
            println!("Data successfuly ingested");
            println!("{}", api_response.data.unwrap().to_string());
        }
        false => {
            let api_error = response.json::<ApiError>().await?;
             println!("ERROR: {:?} ", api_error);
            // println!("ERROR: {} ", response.status());
            // eprintln!("Failed to ingest data: {}", api_error.error)
        }
    }
    Ok(())
}

pub async fn handle_query(
    _config: QuartzConfig,
    _table_name: &str,
    query: &str,
) -> anyhow::Result<()> {
    println!("Executing query: {}", query);
    Ok(())
}
