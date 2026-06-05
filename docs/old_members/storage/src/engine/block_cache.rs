use std::sync::Arc;

use lru::LruCache;
use parking_lot::RwLock;
use quartz_common::{DataBlock, ObjectId};

use crate::engine::segment::Segment;


#[derive(Debug)]
pub struct BlockCache {
    data: RwLock<LruCache<String, DataBlock>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: RwLock::new(LruCache::new(capacity)),
        }
    }

    pub fn fetch_block(&self, segment: Arc<Segment>, object_id: ObjectId) -> Option<DataBlock> {
        let item_key = format!("{}-{}", segment.get_metadata().segment_id, object_id);
        let mut cache = self.data.write();
        if let Some(block) = cache.get(&item_key) {
            return Some(block.clone());
        }

        // If the block is not in the cache, fetch it from the segment
        // and store it in the cache
        let block = segment.fetch_block(object_id).unwrap();
        cache.put(item_key.clone(), block.clone());
        Some(block)
    }
    
}
