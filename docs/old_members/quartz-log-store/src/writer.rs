use std::sync::{Arc, RwLock};

use crate::LogStoreInner;


pub struct LogStoreWriter {
    log_store_inner: Arc<RwLock<LogStoreInner>>,
}

impl LogStoreWriter {
    pub(crate) fn new(log_store_inner: Arc<RwLock<LogStoreInner>>) -> Self {
        LogStoreWriter { log_store_inner }
    }

    pub fn append(&mut self, log_data: &[u8]) -> Result<u64, String> {
        let mut log_store_guard = self.log_store_inner.write().unwrap();
        log_store_guard.append(log_data)
    }
}

// pub struct LogSegmentWriter {
//     store: Arc<RwLock<LogSegmentInner>>,
// }

// impl LogSegmentWriter {

//     pub fn new(store: Arc<RwLock<LogSegmentInner>>) -> Self {
//         let block_size = store.read().unwrap().get_block_size_count();
//         LogSegmentWriter {
//             store,
//             block_builder: BlockBuilder::new(block_size),
//         }
//     }

//     // Append a log entry to the log store
//     // log_id is the id of the log entry.
//     // It should be globally unique and monotonically increasing
//     pub fn append(&mut self, log_id: u64, log_data: &[u8]) -> Result<(), String>  {
//         self.store
//     }

// }
