use std::{collections::BTreeMap, hash::Hash, sync::Arc};

use hashbrown::HashMap;
use parking_lot::RwLock;
use quartz_index::Query;

use super::segment::Segment;



#[derive(Debug)]
pub struct SegmentList {
    segments: RwLock<BTreeMap<String, Arc<Segment>>>,
}
impl SegmentList {
    pub fn new() -> Self {
        Self {
            segments: RwLock::new(Vec::new()),
        }
    }

    pub fn add_segment(&self, segment: Segment) {
        let segment_id = segment.get_metadata().segment_id.clone();
        let mut segments_guard = self.segments.write();
        segments_guard.insert(segment_id, Arc::new(segment));
    }

    pub fn get_segment(&self, segment_id: &str) -> Option<Arc<Segment>> {
        let segments_guard = self.segments.read();
        segments_guard.get(segment_id).cloned()
    }

    pub fn remove_segment(&mut self, segment_id: &str) {
        let mut segments_guard = self.segments.write();
        segments_guard.remove(segment_id);
    }

    // segment pruning (timestamp, bloom-filter)
    // TODO: pass in a query instead of min/max timestamp
    pub fn get_relevant_segments_for_query(&self, min_timestamp: u64, max_timestamp: u64) -> Vec<Arc<Segment>> {
        self.segments
                .read()
                .iter()
                .filter(|segment| segment.get_metadata().min_timestamp <= max_timestamp && segment.get_metadata().max_timestamp >= min_timestamp)
                .cloned()
                .collect()
    }

}
