use crate::{client::MetastoreClient, service::MetastoreService};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, put},
};
use common::{
    catalog::TableMeta,
    models::{ApiError, ApiOk, ApiResponse},
};

pub(crate) fn setup_http_routes(metastore_client: MetastoreClient) -> Router {
    Router::new()
        .route("/metastore/tables", get(handle_list_tables))
        .route("/metastore/tables", put(handle_put_table))
        .route("/metastore/tables/{table_name}", get(handle_get_table))
        .route(
            "/metastore/tables/{table_name}",
            delete(handle_delete_table),
        )
        .with_state(metastore_client)
}

async fn handle_list_tables(
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<Vec<TableMeta>>, ApiError> {
    let indexes = state
        .list_tables()
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", Some(indexes)))
}

async fn handle_put_table(
    State(state): State<MetastoreClient>,
    Json(table_meta): Json<TableMeta>,
) -> Result<ApiOk<()>, ApiError> {
    state
        .put_table(table_meta)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", None))
}

async fn handle_get_table(
    Path(table_name): Path<String>,
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<TableMeta>, ApiError> {
    let index = state
        .get_table(&table_name)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", Some(index)))
}

async fn handle_delete_table(
    Path(table_name): Path<String>,
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<()>, ApiError> {
    state
        .delete_table(&table_name)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", None))
}
