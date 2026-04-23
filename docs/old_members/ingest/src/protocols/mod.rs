mod ndjson;


pub enum IngestProtocol {
    NDJSON,
    InfluxLine,
    Prometheus,
    OpenTelemetry,
    OpenTSDB,
    GraphitePlaintext,
    StatsD,
    
}
