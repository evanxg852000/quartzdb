use crate::TimeSeriesDataType;

/// Ingestion parameters:
///
/// _time_field: timestamp field for both logs and metrics
/// _message_field: only for logs
/// _tags_fields: both logs and metrics
/// _metric_fields: only for metrics [list of measurement fields and optionally their data types]
///
/// for a log, all remaining fields that are not special will be stored along with
/// __message
/// 
#[derive(Debug)]
pub struct IngestionParams {
    pub time_field: String,
    pub message_field: String,
    pub tags_fields: Vec<String>,
    pub metric_fields: Vec<MetricField>,
}



#[derive(Debug)]
pub struct MetricField {
    pub name: String,
    pub data_type: Option<TimeSeriesDataType>,
}
