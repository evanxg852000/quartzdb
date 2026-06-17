use fastbloom::BloomFilter;
use quick_cache::sync::Cache;

#[derive(Debug)]
pub struct SearchTagsFilterCache {
    cache: Cache<String, BloomFilter>,

}

impl SearchTagsFilterCache {

    pub fn new(capacity: usize) -> Self {
        Self { cache: Cache::new(capacity) }
    }

    pub fn put(&self, split_id: String, filter: BloomFilter) {
        self.cache.insert(split_id.to_string(), filter);
    }

    pub fn contain_tags(&self, split_id: &str, tags: Vec<&str>) -> bool {
        self.cache.get(split_id)
            .map_or(false, |filter| {
                tags.into_iter().any(|tag| filter.contains(tag))
            })
    }
}
