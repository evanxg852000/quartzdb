
mod log;
mod time_series;
mod utils;
mod ingestion;
mod data_block;
mod serialization;

use std::collections::BTreeSet;

pub use log::*;
pub use time_series::*;
pub use utils::*;
pub use ingestion::*;
pub use data_block::*;

// An identifier referencing a time_series_id or a log_stream_id.
pub type ObjectId = u64;

// The time_series_id is a unique identifier for a time series.
pub type TimeSeriesId = u64;

// The log_stream_id is a unique identifier for a log stream.
pub type LogStreamId = u64;

// The log_id is the log (row) index within a stream record 
// batch in a segment.
pub type LogRowIndex = u64;


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tag {
    pub key: String,
    pub value: String,
}


#[derive(Debug)]
pub struct TagSet {
    tags: BTreeSet<Tag>,
}

impl TagSet {
    pub fn new() -> Self {
        TagSet { tags: BTreeSet::new() }
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.tags.insert(Tag { key, value });
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tag> {
        self.tags.iter()
    }

}

impl From<Vec<Tag>> for TagSet {
    fn from(tags: Vec<Tag>) -> Self {
        TagSet { tags: tags.into_iter().collect() }
    }
}


#[derive(Debug)]
pub enum Object{
    TimeSeries(TimeSeries),
    LogStream(LogStream),
}

impl Object {
    pub fn get_id(&self) -> ObjectId {
        match self {
            Object::TimeSeries(ts) => ts.time_series_id,
            Object::LogStream(ls) => ls.log_stream_id,
        }
    }
}

// #[derive(Debug)]
// enum RecordBatch {
//     Metric(MetricRecordBatch),
//     Log(LogRecordBatch),
// }

// Parquet Schema for logs and metrics
// Logs: [timestamp, msgpack(trimmed_log_line)]
// Metrics:  [timestamp, value]


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
