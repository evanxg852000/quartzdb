use std::path::Path;

use hashbrown::HashMap;
use quartz_common::{DataBlock, LogId, LogRecordBatch, LogStream, LogStreamId, ObjectId, TimeSeries, TimeSeriesId, TimeSeriesRecordBatch};
use quartz_index::{Index, Query};
use data::SegmentData;
use serde::{Deserialize, Serialize};

mod data;
mod builder;


const METADATA_FILE: &str = "meta.json";
const INDEX_DIRECTORY: &str = "index";
const DATA_FILE: &str = "data.bin";

///
/// A segment is a immutable part of a collection of metric and log data 
/// that is stored on disk and can be queried.
/// it is a directory that contains the following files:
/// split_ulid/meta.json
/// split_ulid/index/
/// split_ulid/data.bin
/// 



#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub segment_id: String,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    memory_size: u64,
    time_series_count: u64,
    log_stream_count: u64,
    time_series_record_count: u64,
    log_stream_record_count: u64,
}

impl SegmentMetadata {
    pub fn new(id: String) -> Self {
        Self {
            segment_id: id,
            ..Default::default()
        }
    }

}


#[derive(Debug)]
pub struct Segment {
    //TODO: add bloom filter based on tenant/label 
    metadata: SegmentMetadata,
    index: Index,
    data: SegmentData,
}

impl Segment {
    pub fn create(
        directory: impl AsRef<Path>,
        metadata: SegmentMetadata,
        term_dict: HashMap<String, Vec<ObjectId>>,
        documents: Vec<(&str, ObjectId, ObjectId)>,
        time_series_data: Vec<&TimeSeriesRecordBatch>,
        log_data: Vec<&LogRecordBatch>,
    ) -> Self {
        std::fs::create_dir_all(&directory).unwrap();
        //TODO: save meta.json
        let meta_file_path = directory.as_ref().join(METADATA_FILE);
        let serialized_metadata = serde_json::to_string(&metadata).unwrap();
        std::fs::write(meta_file_path, serialized_metadata).unwrap();

        let index_directory = directory.as_ref().join(INDEX_DIRECTORY);
        let index= Index::create(index_directory, term_dict, documents).unwrap();
        let data = SegmentData::create(&directory, time_series_data, log_data);
        
        Self {metadata, index, data}
    }

    pub fn open(directory: impl AsRef<Path>) -> Self {
        let meta_file_path = directory.as_ref().join(METADATA_FILE);
        let serialized_metadata = std::fs::read(meta_file_path).unwrap();
        let metadata = serde_json::from_slice::<SegmentMetadata>(&serialized_metadata).unwrap();
        
        let index_directory = directory.as_ref().join(INDEX_DIRECTORY);
        let index = Index::open(index_directory).unwrap();

        let data = SegmentData::open(&directory);

        Self {metadata, index, data}
    }

    pub fn get_metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub fn search_inverted_index(&self, query: Query) -> Vec<ObjectId> {
       self.index.search_inverted_index(query).unwrap()
    }

    pub fn search_fts(&self, query: Query) -> Vec<(LogStreamId, Vec<LogId>)> {
        self.index.search_fts(query).unwrap()
    }
    
    pub fn fetch_block( &self, object_id: ObjectId) -> Result<DataBlock, String> {
        self.data.fetch_block(object_id)
    }

}


