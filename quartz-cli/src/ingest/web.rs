use crate::{common::models::{ApiOk, ApiError, ApiResponse}, ingest::client::InsertServiceClient};
use axum::{
    extract::{State, Path},
    Router,
    routing::post,
};

pub fn setup_web_routes(service_client: InsertServiceClient) -> Router {
    // POST: /api/v1/ingest/{protocol}
    // - protocol: "ndjson", "influxline", "prometheus", "opentelemetry
    axum::Router::new()
        .route("/ingest/ndjson/{index_name}", post(handle_ndjson_ingest))
        // .route("ingest/influxline", axum::routing::post(handle_influxline_ingest))
        // .route("ingest/prometheus", axum::routing::post(handle_prometheus_ingest))
        // .route("ingest/opentelemetry", axum::routing::post(handle_opentelemetry_ingest))
        .with_state(service_client)
        
}

async fn handle_ndjson_ingest(
    Path(index_name): Path<String>,
    State(state): State<InsertServiceClient>,
) -> Result<ApiOk<()>, ApiError> {
    println!("Received NDJSON ingest request for index: {}", index_name);

    //TODO: parse & perform validation based on schema & protolcol

    //TODO: convert to BSON (internal format)
    
    // Send doc to insert service
    let doc = r#"{"timestamp": 1627847284, "value": 42}"#.as_bytes().to_vec();
    state.send_message(doc).await.unwrap();
    Ok(ApiResponse::ok("OK", None))
}

// - implement reducing TTL
// - think of future other ways to do this

