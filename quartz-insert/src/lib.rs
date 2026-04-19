mod protocols;
mod buffers;

use anyhow::Ok;
use axum::Router;


pub struct InsertService{
    router: axum::Router,
}

impl InsertService {
    pub fn start() -> anyhow::Result<Self> {
        let router = setup_router();
        Ok(InsertService { router })
    }

    pub fn router(&self) -> axum::Router {
        self.router.clone()
    }
}


//indexId ->(streamID or seriesID)


fn setup_router() -> Router {
    // POST: /api/v1/ingest/{protocol}
    // - protocol: "ndjson", "influxline", "prometheus", "opentelemetry
    axum::Router::new()
        .route("/ingest/ndjson/{index-id}", axum::routing::post(handle_ndjson_ingest))
        // .route("ingest/influxline", axum::routing::post(handle_influxline_ingest))
        // .route("ingest/prometheus", axum::routing::post(handle_prometheus_ingest))
        // .route("ingest/opentelemetry", axum::routing::post(handle_opentelemetry_ingest));
}

async fn handle_ndjson_ingest() -> &'static str {
    "NDJSON ingest endpoint"
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
