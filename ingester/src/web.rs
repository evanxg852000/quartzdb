use axum::{
    Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::StatusCode,
    routing::post,
};
use futures_util::TryStreamExt;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use common::models::{ApiError, ApiOk, ApiResponse};

use crate::{client::IngesterClient, table_processor::{BatchProcessorPolicy, ProcessingReport}, document::IngestBatch};

pub(crate) fn setup_http_routes(service_client: IngesterClient) -> Router {
    // PUT: /api/v1/ingest/{protocol}
    // - protocol: "ndjson", "influxline", "prometheus", "opentelemetry
    axum::Router::new()
        .route(
            "/ingest/ndjson/{table_name}",
            post(handle_ndjson_ingest).layer(DefaultBodyLimit::max(10 * 1024 * 1024)),
        )
        // .route("ingest/influxline", axum::routing::post(handle_influxline_ingest))
        // .route("ingest/prometheus", axum::routing::post(handle_prometheus_ingest))
        // .route("ingest/opentelemetry", axum::routing::post(handle_opentelemetry_ingest))
        .with_state(service_client)
}

async fn handle_ndjson_ingest(
    Path(table_name): Path<String>,
    State(state): State<IngesterClient>,
    req: Request,
) -> Result<ApiOk<ProcessingReport>, ApiError> {
    let body_stream = req
        .into_body()
        .into_data_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(body_stream);
    let mut lines = stream_reader.lines();

    let mut ingest_batch = IngestBatch::with_capacity(1024);
    while let Ok(Some(json_line)) = lines.next_line().await {
        ingest_batch.add_document(json_line);
    }
    
    let report = state
        .process_batch(table_name, ingest_batch, BatchProcessorPolicy::Lenient)
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
