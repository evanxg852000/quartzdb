/// A simple log store that writes and reads log entries to and from a file.
///
/// The log store is append only and does not support deletion of log entries.
/// The log store is optimized for sequential writes and random reads.
/// The log store is not thread safe.
/// 
/// A log entry is a tuple of a log id and log data.
/// The log id is a unique identifier for the log entry. it's important to  
/// note that the log id should be globally  unique and monotonically increasing.
/// The log data is the actual data of the log entry.
///
/// The log store is divided into blocks of fixed size.
/// Each block contains multiple log entries.
/// Each block is compressed before writing to the file.
/// 

//TODO: custom error, add tests

mod store;
mod segment;
mod block;
mod writer;
mod reader;

use std::{
    path::{Path, PathBuf}, sync::{Arc, RwLock}
};

use block::BlockSize;
use reader::LogStoreReader;
use segment::LogSegment;
use writer::LogStoreWriter;


#[derive(Debug)]
pub struct LogStore {
    log_store_inner: Arc<RwLock<LogStoreInner>>,
}

impl LogStore {
    pub fn open(directory: impl AsRef<Path>, block_size: BlockSize) -> Self {
        let log_store_inner = Arc::new(RwLock::new(LogStoreInner::open(directory, block_size)));
        LogStore { log_store_inner }
    }

    pub fn reader(&self) -> LogStoreReader {
        LogStoreReader::new(self.log_store_inner.clone())
    }

    pub fn writer(&self) -> LogStoreWriter {
        LogStoreWriter::new(self.log_store_inner.clone())
    }
}

#[derive(Debug)]
pub(crate) struct LogStoreInner {
    directory: PathBuf,
    log_id_counter: u64,
    block_size: usize,
    segments: Vec<LogSegment>, // sorted by max_log_id
}

impl LogStoreInner {

    pub fn open(directory: impl AsRef<Path>, block_size: BlockSize) -> Self {
        std::fs::create_dir_all(&directory).unwrap();
        // fetch all the segment files in the directory
        let mut segment_files = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| {
                entry.map(|entry| entry.path())
            })
            .collect::<Result<Vec<_>, _>>().unwrap();
        let block_size = block_size.count() as usize;
        segment_files.sort(); // segment files are name using ULID so they can be sorted by creation order
        let segments= segment_files.into_iter().map(|path| {
                let segment_id = path.file_stem().unwrap().to_str().unwrap().to_string();
                LogSegment::open(directory.as_ref(), segment_id, block_size)
            })
            .collect::<Vec<_>>();

        let log_id_counter = segments.last().map_or(1, |segment| segment.get_max_log_id() + 1);
        LogStoreInner { 
            directory: directory.as_ref().to_path_buf(),
            log_id_counter,
            block_size, 
            segments,
        }
    }
    
    pub fn append(&mut self, log_data: &[u8]) -> Result<u64, String> {
        let mut last_segment = self.segments.last_mut().unwrap();
        if last_segment.will_overflow(log_data.len() as u64) {
            let segment_id = ulid::Ulid::new().to_string();
            let new_segment = LogSegment::open(&self.directory, segment_id, self.block_size);
            self.segments.push(new_segment);
            last_segment = self.segments.last_mut().unwrap();
        }

        let log_id = self.log_id_counter;
        last_segment.append(log_id, log_data);
        self.log_id_counter += 1;
        Ok(log_id)
    }

    pub fn get_segment_responsible_for_log_id(&self, log_id: u64) -> Option<LogSegment> {
        //TODO: lock
        // find the segment that contains the log_id
        let segment_index = self.segments.binary_search_by(|segment| {
            log_id.cmp(&segment.get_max_log_id()) 
        }).ok().unwrap();
        self.segments.get(segment_index).cloned()
    }

}


