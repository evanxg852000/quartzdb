use axum::{
    Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::StatusCode,
    routing::post,
};
use futures_util::TryStreamExt;
use serde_json::Value as JsonValue;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use crate::{
    common::{
        document::DocumentBatch,
        models::{ApiError, ApiOk, ApiResponse},
    },
    ingest::{
        client::IngestServiceClient,
        doc_processor::{DocProcessorPolicy, ProcessingReport},
    },
};

pub fn setup_web_routes(service_client: IngestServiceClient) -> Router {
    // PUT: /api/v1/ingest/{protocol}
    // - protocol: "ndjson", "influxline", "prometheus", "opentelemetry
    axum::Router::new()
        .route(
            "/ingest/ndjson/{index_name}",
            post(handle_ndjson_ingest).layer(DefaultBodyLimit::max(10 * 1024 * 1024)),
        )
        // .route("ingest/influxline", axum::routing::post(handle_influxline_ingest))
        // .route("ingest/prometheus", axum::routing::post(handle_prometheus_ingest))
        // .route("ingest/opentelemetry", axum::routing::post(handle_opentelemetry_ingest))
        .with_state(service_client)
}

async fn handle_ndjson_ingest(
    Path(index_name): Path<String>,
    State(state): State<IngestServiceClient>,
    req: Request,
) -> Result<ApiOk<ProcessingReport>, ApiError> {
    let body_stream = req
        .into_body()
        .into_data_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(body_stream);
    let mut lines = stream_reader.lines();

    let mut doc_batch = DocumentBatch::with_capacity(100);
    while let Ok(Some(json_line)) = lines.next_line().await {
        let value = serde_json::from_str::<JsonValue>(&json_line)
            .map_err(|err| ApiResponse::error(StatusCode::BAD_REQUEST, err.to_string()))?;
        doc_batch.add_document(value, json_line.len());
    }

    let report = state
        .process_batch(index_name, doc_batch, DocProcessorPolicy::Lenient)
        .await
        .map_err(|err| ApiResponse::error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    if !report.accepted {
        return Err(ApiResponse::error(
            StatusCode::BAD_REQUEST,
            "Some(report)".to_string(),
        ));
    }

    Ok(ApiResponse::ok("OK", Some(report)))
}
