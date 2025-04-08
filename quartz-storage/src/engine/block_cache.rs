use lru::LruCache;
use parking_lot::RwLock;
use quartz_common::{DataBlock, ObjectId};

use crate::segment::Segment;



#[derive(Debug, Clone)]
pub struct BlockCache {
    data: Arc<RwLock<LruCache<String, DataBlock>>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(LruCache::new(capacity))),
        }
    }

    pub fn fetch_block(&self, segment: Segment, object_id: ObjectId) -> Option<DataBlock> {
        let item_key = format!("{}-{}", segment.get_metadata().segment_id, object_id);
        let mut cache = self.data.write();
        if let Some(block) = cache.get(&item_key) {
            return Some(block.clone());
        }

        let block = segment.fetch_block(object_id).unwrap();
        cache.put(item_key.clone(), block.clone());
        Some(block)
    }
    
}
