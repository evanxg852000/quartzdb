use std::path::{Path, PathBuf};

use hashbrown::HashMap;
use quartz_common::{LogRecordBatch, LogStreamId, TimeSeriesId, TimeSeriesRecordBatch};

use super::{Segment, SegmentMetadata};


#[derive(Debug)]
struct SegmentBuilder {
    id: String,
    directory: PathBuf,
    min_timestamp: u64,
    max_timestamp: u64,
    memory_size: u64,
    time_series_record_count: u64,
    log_stream_record_count: u64,
    series_batches: HashMap<TimeSeriesId, TimeSeriesRecordBatch>,
    log_batches: HashMap<LogStreamId, LogRecordBatch>,
}

impl SegmentBuilder {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        let id = ulid::Ulid::new().to_string();
        let segment_directory = directory.as_ref().join(&id);
        Self {
            id,
            directory: segment_directory,
            min_timestamp: 0,
            max_timestamp: 0,
            memory_size: 0,
            time_series_record_count: 0,
            log_stream_record_count: 0,
            series_batches: HashMap::new(),
            log_batches: HashMap::new(),
        }
    }

    pub fn ingest_time_series_bacth(&mut self, batch: TimeSeriesRecordBatch) {
        self.time_series_record_count += batch.len() as u64;
        self.min_timestamp = self.min_timestamp.min(batch.get_min_timestamp());
        self.max_timestamp = self.max_timestamp.max(batch.get_max_timestamp());
        self.memory_size = batch.estimate_memory_size();

        let time_series_id = batch.get_time_series().time_series_id;
        if !self.series_batches.contains_key(&time_series_id) {
            self.series_batches.insert(time_series_id, batch);
            return;
        }
        self.series_batches.get_mut(&time_series_id).unwrap().extend(batch);
    }

    pub fn ingest_log_stream_bacth(&mut self, batch: LogRecordBatch) {
        self.log_stream_record_count += batch.len() as u64;
        self.min_timestamp = self.min_timestamp.min(batch.get_min_timestamp());
        self.max_timestamp = self.max_timestamp.max(batch.get_max_timestamp());
        self.memory_size = batch.estimate_memory_size();

        let log_stream_id = batch.get_log_stream().log_stream_id;
        if !self.log_batches.contains_key(&log_stream_id) {
            self.log_batches.insert(log_stream_id, batch);
            return;
        }
        self.log_batches.get_mut(&log_stream_id).unwrap().extend(batch);
    }

    pub fn build(self) -> Segment {
        // extract metadata 
        let metadata = SegmentMetadata {
            segment_id: self.id,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            memory_size: self.memory_size,
            time_series_count: self.series_batches.len() as u64,
            log_stream_count: self.log_batches.len() as u64,
            time_series_record_count: self.time_series_record_count,
            log_stream_record_count: self.log_stream_record_count,
        };

        // extract document list & term dictionary
        let mut documents = Vec::new();
        let mut term_dict = HashMap::new();
        for (stream_id, log_batch) in self.log_batches.iter(){
            for tag in log_batch.get_log_stream().tags.iter() {
                let term = format!("{}:{}", tag.key, tag.value);
                term_dict.entry(term)
                    .or_insert_with(Vec::new)
                    .push(*stream_id);
            }

            let mut log_row_index = 0;
            for (_, log_content) in log_batch.get_rows().iter() {
                documents.push((log_content.as_str(), *stream_id, log_row_index));
                log_row_index += 1;
            }
        }

        for (series_id, series_batch) in self.series_batches.iter() {
            for tag in series_batch.get_time_series().tags.iter() {
                let term = format!("{}:{}", tag.key, tag.value);
                term_dict.entry(term)
                    .or_insert_with(Vec::new)
                    .push(*series_id);
            }
        }

        // 14" -. 23M ->  $2,653
        
        
        
        // extract data from batches
        let time_series_data = self.series_batches.values().collect();
        let log_data = self.log_batches.values().collect();
        
        // create segment
        Segment::create(self.directory, metadata, term_dict, documents, time_series_data, log_data)
    }

}
