use crate::{common::{models::{ApiError, ApiOk, ApiResponse}, document::{Document, DocumentBatch}}, ingest::{client::InsertServiceClient, doc_processor::{DocProcessor, DocProcessorPolicy, ProcessingReport}}};
use axum::{
    extract::{State, Path},
    Router,
    routing::post,
};
use reqwest::StatusCode;

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
) -> Result<ApiOk<ProcessingReport>, ApiError> {
    println!("Received NDJSON ingest request for index: {}", index_name);
    let mut doc_bacth = DocumentBatch::with_capacity(100);
    //TODO: parse & convert to json based on protolcol

    // add doc & send batch for processing
    let doc = Document{
        line_number: 1,
        json_value: serde_json::json!({"timestamp": 1627847284, "value": 42}),
        raw_size: 24,
    };
    doc_bacth.add_document(doc);

    let report = state.process_batch(doc_bacth, DocProcessorPolicy::Lenient)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    
    if !report.accepted {
        return Err(ApiResponse::error(StatusCode::BAD_REQUEST, "Some(report)".to_string()));
    }

    Ok(ApiResponse::ok("OK", Some(report)))
}

// - implement reducing TTL
// - think of future other ways to do this

