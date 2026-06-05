use std::sync::{Arc, RwLock};

use crate::LogStoreInner;


pub struct LogStoreReader {
    log_store_inner: Arc<RwLock<LogStoreInner>>,
}

impl LogStoreReader {
    pub(crate) fn new(log_store_inner: Arc<RwLock<LogStoreInner>>) -> Self {
        LogStoreReader { log_store_inner }
    }

    pub fn get(&self, log_id: u64) -> Option<Vec<u8>> {
        let segment = {
            let log_store_guard = self.log_store_inner.read().unwrap();
            log_store_guard.get_segment_responsible_for_log_id(log_id).unwrap()
        };
        segment.get(log_id)
    }
    
    //TODO: add read range & read all
}



// pub struct LogSegmentReader {
//     segment_inner: Arc<RwLock<LogSegmentInner>>,
// } 

// impl LogSegmentReader {
//     pub fn new(segment_inner: Arc<RwLock<LogSegmentInner>>) -> Self {
//         LogSegmentReader { segment_inner }
//     }

//     pub fn get(&self, log_id: u64) -> Option<Vec<u8>> {
//         let segment_guard = self.segment_inner.read().unwrap();
//         segment_guard.get(log_id)
//     }
// }
