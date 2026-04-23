use std::sync::Arc;

use anyhow::Result;
use hashbrown::{HashMap, hash_map::Entry};
use tokio::sync::Mutex;

use crate::common::index::IndexConfig;

pub trait Processor {}

pub struct ProcessorRegistry<T> {
    indexes: Mutex<HashMap<String, (Arc<IndexConfig>, Arc<T>)>>,
}

impl<T: Processor> ProcessorRegistry<T> {
    pub fn new() -> Self {
        Self {
            indexes: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_initial_processors(&self, processors: Vec<(String, (Arc<IndexConfig>, Arc<T>))>) {
        let mut indexes = self.indexes.lock().await;
        for (key, processor) in processors {
            indexes.insert(key, processor);
        }
    }

    pub async fn put_index<F>(&self, index_name: String, initialize: F)
    where
        F: FnOnce() -> (Arc<IndexConfig>, Arc<T>),
    {
        let mut indexes = self.indexes.lock().await;
        let (index_config, processor) = initialize();
        match indexes.entry(index_name) {
            Entry::Occupied(mut entry) => {
                entry.insert((index_config, processor));
            }
            Entry::Vacant(entry) => {
                entry.insert((index_config, processor));
            }
        }
    }

    pub async fn delete_index(&self, index_name: &str) {
        let mut indexes = self.indexes.lock().await;
        indexes.remove(index_name);
    }

    pub async fn get_processor(&self, index_name: &str) -> Result<Arc<T>> {
        let mut indexes = self.indexes.lock().await;
        let (_, processor) = indexes.get_mut(index_name).ok_or_else(|| {
            anyhow::anyhow!("Index `{}` not found in the processor registry", index_name)
        })?;
        Ok(processor.clone())
    }
}
