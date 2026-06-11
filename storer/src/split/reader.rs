use std::sync::Arc;

use anyhow::Result;
use common::catalog::SplitMeta;
use storage::Storage;


pub struct SplitReader {
    // tag_filter, // decoded bloom filter
    // index_store, //opened ind
    // columns_store: 
}

impl SplitReader {

    pub fn open() -> Result<Self> {
        Ok(Self{})
    }


}

// pub struct SplitFilter {
//     split_meta: SplitMeta,
//     bloom_filter: BloomFilter,
// }

// impl SplitFilter {
//     pub fn new(storage: Arc<dyn Storage>, split_meta: SplitMeta) -> Result<Self> {
        
//     }

//     pub fn is_in_time_range(start_timestamp: Option<i64>, end_timestamp: Option<i64>) -> bool {
//         //TODO:
//         false
//     }

//     pub fn has_tag() -> bool {
//         //TODO:
//         true
//     }
// }
