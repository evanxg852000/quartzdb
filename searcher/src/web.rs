use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use serde::{Deserialize, Serialize};

use common::models::{ApiError, ApiOk, ApiResponse};

use crate::{client::SearcherClient, search_processor::SearchResult};

#[derive(Deserialize, Serialize)]
pub struct SearchRequest {
    pub query: String,
}

pub(crate) fn setup_http_routes(service_client: SearcherClient) -> Router {
    axum::Router::new()
        .route("/search/{table_name}", post(handle_search))
        .with_state(service_client)
}

async fn handle_search(
    Path(table_name): Path<String>,
    State(state): State<SearcherClient>,
    Json(search_request): Json<SearchRequest>,
) -> Result<ApiOk<SearchResult>, ApiError> {
    let result = state
        .search(table_name, search_request.query)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(ApiResponse::ok("OK", Some(result)))
}
