use std::sync::Arc;

use anyhow::{Result, anyhow};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::oneshot;

use crate::common::{
    document::{Document, DocumentBatch},
    index::IndexConfig,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationError {
    doc: Document,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessingReport {
    pub num_docs: usize,
    pub accepted: bool,
    pub errors: Vec<ValidationError>,
}

impl ProcessingReport {
    pub fn new(num_docs: usize) -> Self {
        Self {
            num_docs: num_docs,
            accepted: false,
            errors: vec![],
        }
    }

    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    pub fn has_error(&self) -> bool {
        self.errors.len() > 0
    }

    pub fn errors_iter(&self) -> impl Iterator<Item = &ValidationError> {
        self.errors.iter()
    }
}

#[derive(Debug)]
pub enum DocProcessorPolicy {
    Strict,
    Lenient,
}

#[derive(Debug, Clone)]
pub struct DocProcessor {
    index_config: Arc<IndexConfig>,
}

impl DocProcessor {
    pub fn new(index_config: Arc<IndexConfig>) -> Self {
        Self { index_config }
    }

    pub async fn process_batch(
        &self,
        mut batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    ) -> Result<()> {
        let mut report = validate_batch(&mut batch);
        if matches!(policy, DocProcessorPolicy::Strict) && report.has_error() {
            reply_sender
                .send(report)
                .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
            return Ok(());
        }

        // we accepted the batch
        report.accepted = true;

        // TODO: create the intermediate split with the filtered batch
        // send it to storage
        // & send the report

        reply_sender
            .send(report)
            .map_err(|_| anyhow!("Failed to send on reply channel"))?;
        Ok(())
    }
}

fn validate_batch(batch: &mut DocumentBatch) -> ProcessingReport {
    let report = ProcessingReport::new(batch.len());
    //TODO: filter the doc batch a see what docs are wrong

    //TODO: check if json value is objcet
    // if !matches!(value, JsonValue::Object(_)) {
    //     return Err(anyhow::anyhow!("Expected a json object"));
    // }
    report
}

pub struct ProcessorRegistry {
    indexes: Mutex<HashMap<String, (Arc<IndexConfig>, Arc<DocProcessor>)>>,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            indexes: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_index(&self, index_name: String, index_config: IndexConfig) {
        let mut indexes = self.indexes.lock().await;
        if indexes.contains_key(&index_name) {
            indexes.remove(&index_name);
        }

        let index_config = Arc::new(index_config);
        let processor = Arc::new(DocProcessor::new(index_config.clone()));
        indexes.insert(index_name, (index_config, processor));
    }

    pub async fn delete_index(&self, index_name: &str) {
        let mut indexes = self.indexes.lock().await;
        indexes.remove(index_name);
    }

    pub async fn get_processor(&self, index_name: &str) -> Result<Arc<DocProcessor>> {
        let mut indexes = self.indexes.lock().await;
        let (_, processor) = indexes.get_mut(index_name).ok_or_else(|| {
            anyhow::anyhow!("Index `{}` not found in the processor registry", index_name)
        })?;
        Ok(processor.clone())
    }
}
