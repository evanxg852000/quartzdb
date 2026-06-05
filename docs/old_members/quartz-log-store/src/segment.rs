use std::{path::Path, sync::{Arc, RwLock}};

use crate::store::LogSegmentInner;

const MAX_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4GB

#[derive(Clone, Debug)]
pub struct LogSegment {
    id: String,
    inner: Arc<RwLock<LogSegmentInner>>,
}

impl PartialEq for LogSegment {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for LogSegment {}

impl PartialOrd for LogSegment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for LogSegment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    } 
}


impl LogSegment {

    pub fn open(directory: impl AsRef<Path>, segment_id: String, block_size: usize) -> Self {
        let inner = Arc::new(RwLock::new(LogSegmentInner::open(directory, &segment_id, block_size)));
        LogSegment { id: segment_id.to_string(), inner }
    }

    pub fn get_min_log_id(&self) -> u64 {
        self.inner.read().unwrap().get_min_log_id()
    }

    pub fn get_max_log_id(&self) -> u64 {
        self.inner.read().unwrap().get_max_log_id()
    }
    
    pub fn will_overflow(&self, log_entry_len: u64) -> bool {
        self.inner.read().unwrap().get_log_file_size() + log_entry_len >= MAX_SEGMENT_SIZE
    }

    pub fn append(&mut self, log_id: u64, log_data: &[u8]) {
        self.inner.write().unwrap().append(log_id, log_data).unwrap();
    }

    pub fn get(&self, log_id: u64) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(log_id)
    }
    
}
