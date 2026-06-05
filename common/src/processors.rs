use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use hashbrown::HashMap;
use tokio::sync::Mutex;

use crate::catalog::TableMeta;


pub trait Processor : Debug {}

#[derive(Debug)]
pub struct ProcessorWrapper<T: Processor> {
    table: Arc<TableMeta>, //TODO: may not be needed since we can get the table meta from the processor context.
    pub processor: Arc<T>,
}

impl<T> ProcessorWrapper<T>
where
    T: Processor,
{
    pub fn new(table: Arc<TableMeta>, processor: Arc<T>) -> Self {
        Self { table, processor }
    }
}

pub struct ProcessorRegistry<T: Processor> {
    entries: Mutex<HashMap<String, ProcessorWrapper<T>>>,
}

impl<T: Processor> ProcessorRegistry<T> {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn set_initial_processors(
        &self,
        processors: Vec<ProcessorWrapper<T>>,
    ) {
        let mut entries = self.entries.lock().await;
        for processor in processors {
            entries.insert(processor.table.name.clone(), processor);
        }
    }

    pub async fn add_processor(&self, processor: ProcessorWrapper<T>) -> Result<()> {
        let mut entries = self.entries.lock().await;
        entries.insert(processor.table.name.clone(), processor);
        Ok(())
    }

    pub async fn delete_processor(&self, name: &str) {
        let mut entries = self.entries.lock().await;
        entries.remove(name);
    }

    pub async fn get_processor(&self, name: &str) -> Option<Arc<T>> {
        let mut entries = self.entries.lock().await;
        entries.get_mut(name).map(|wrapper| wrapper.processor.clone())
    }
}
