use crate::{
    common::{
        index::IndexMeta,
        models::{ApiError, ApiOk, ApiResponse},
    },
    metastore::client::MetastoreClient,
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, put},
};

pub fn setup_web_routes(metastore_client: MetastoreClient) -> Router {
    Router::new()
        .route("/metastore/indexes", put(handle_put_index))
        .route("/metastore/indexes/{index_name}", get(handle_get_index))
        .route("/metastore/indexes", get(handle_list_indexes))
        .route(
            "/metastore/indexes/{index_name}",
            delete(handle_delete_index),
        )
        .with_state(metastore_client)
}

async fn handle_put_index(
    State(state): State<MetastoreClient>,
    Json(index_meta): Json<IndexMeta>,
) -> Result<ApiOk<()>, ApiError> {
    state
        .put_index(index_meta)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", None))
}

async fn handle_get_index(
    Path(index_name): Path<String>,
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<IndexMeta>, ApiError> {
    let index =state
        .get_index(&index_name)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", Some(index)))
}

async fn handle_list_indexes(
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<Vec<IndexMeta>>, ApiError> {
    let indexes = state
        .list_indexes()
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", Some(indexes)))
}

async fn handle_delete_index(
    Path(index_name): Path<String>,
    State(state): State<MetastoreClient>,
) -> Result<ApiOk<()>, ApiError> {
    state
        .delete_index(&index_name)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", None))
}
