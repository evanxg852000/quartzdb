use crate::{LogStreamId, TagSet};

#[derive(Debug)]
pub struct LogStream {
    pub log_stream_id: LogStreamId,         
    pub stream_name: String,   // __name__:{stream_name}
    pub tags: TagSet,         // add message_field as tags
}


#[derive(Debug)]
pub struct LogRecordBatch {
    stream: LogStream,
    min_timestamp: u64,
    max_timestamp: u64,
    rows: Vec<(u64, String)>, // (timestamp, value(json/msg-pack))  sorted by timestamp ASC
}

impl LogRecordBatch {
    pub fn new(stream: LogStream) -> Self {
        LogRecordBatch { 
            stream: stream, 
            min_timestamp: 0, 
            max_timestamp: 0, 
            rows: Vec::new()
        }
    }

    pub fn get_log_stream(&self) -> &LogStream {
        &self.stream
    }

    pub fn get_min_timestamp(&self) -> u64 {
        self.min_timestamp
    }

    pub fn get_max_timestamp(&self) -> u64 {
        self.max_timestamp
    }

    pub fn get_rows(&self) -> &Vec<(u64, String)> {
        &self.rows
    }

    pub fn insert(&mut self, timestamp: u64, value: String) {
        if self.rows.is_empty() {
            self.min_timestamp = timestamp;
            self.max_timestamp = timestamp;
        } 

        self.rows.push((timestamp, value));
        self.min_timestamp = std::cmp::min(self.min_timestamp, timestamp);
        self.max_timestamp = std::cmp::max(self.max_timestamp, timestamp);
    }

    pub fn insert_rows(&mut self, rows: Vec<(u64, String)>) {
        self.rows.extend(rows);
        self.sort();
    }

    pub fn extend(&mut self, other: LogRecordBatch) {
        self.rows.extend(other.rows.into_iter());
        self.sort();
    }

    pub fn sort(&mut self) {
        self.rows.sort_by(|a, b| a.0.cmp(&b.0));
        self.min_timestamp = self.rows[0].0;
        self.max_timestamp = self.rows[self.rows.len() - 1].0;
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn estimate_memory_size(&self) -> u64 {
        self.rows.iter()
            .fold(0usize, |acc, (timestamp, value)| {
                acc + value.len() + 8 // timestamp size
            }) as u64
    }
}

