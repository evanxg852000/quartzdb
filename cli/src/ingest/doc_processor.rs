use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::common::processors::Processor;
use crate::common::schema::Schema;
use crate::common::{
    document::{Document, DocumentBatch},
    index::IndexConfig,
};
use crate::storage::client::StorageServiceClient;

use proto::quartzdb::{ProtoDocument, ProtoDocumentBatch};

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationError {
    pub source: String,
    pub error: String,
}

impl ValidationError {
    pub fn new(document: &Document, error: String) -> Self {
        Self {
            source: document.json_object.to_string(),
            error,
        }
    }
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
            accepted: true,
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
    index_name: String,
    index_config: Arc<IndexConfig>,
    storage_client: StorageServiceClient,
}

impl Processor for DocProcessor {}

impl DocProcessor {
    pub fn new(
        index_name: String,
        index_config: Arc<IndexConfig>,
        storage_client: StorageServiceClient,
    ) -> Self {
        Self {
            index_name,
            index_config,
            storage_client,
        }
    }

    pub async fn process_batch(
        &self,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    ) -> Result<()> {
        let (proto_batch, report) = process_batch(&self.index_config, batch, policy)?;
        if !report.accepted {
            reply_sender
                .send(report)
                .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
            return Ok(());
        }

        self.storage_client
            .put_batch(&self.index_name, proto_batch)
            .await?;

        reply_sender
            .send(report)
            .map_err(|_| anyhow!("Failed to send on reply channel"))?;
        Ok(())
    }
}

fn process_batch(
    index_config: &IndexConfig,
    batch: DocumentBatch,
    policy: DocProcessorPolicy,
) -> Result<(ProtoDocumentBatch, ProcessingReport)> {
    let batch_id = uuid::Uuid::now_v7().to_string();
    let mut proto_batch = ProtoDocumentBatch::with_capacity(batch_id, batch.len());
    let mut report = ProcessingReport::new(batch.len());
    for document in batch.0 {
        match process_document(index_config, document) {
            Ok(proto_document) => proto_batch.add_document(proto_document),
            Err(err) => report.add_error(err),
        }
    }

    if matches!(policy, DocProcessorPolicy::Strict) && report.has_error() {
        report.accepted = false
    }

    Ok((proto_batch, report))
}

fn process_document(
    index_config: &IndexConfig,
    document: Document,
) -> Result<ProtoDocument, ValidationError> {
    let id = document.id;
    let timestamp = Schema::extract_timestamp(index_config, &document)
        .map_err(|err| ValidationError::new(&document, err.to_string()))?;
    let source = document.json_object.to_string();
    let values = Schema::extract_field_values(index_config, &document)
        .map_err(|err| ValidationError::new(&document, err.to_string()))?;
    let labels = Schema::extract_label_values_as_object(index_config, &document)
        .map_err(|err| ValidationError::new(&document, err.to_string()))?
        .to_string();
    let tags = Schema::extract_tag_values(index_config, &document)
        .map_err(|err| ValidationError::new(&document, err.to_string()))?;
    Ok(ProtoDocument {
        id,
        timestamp,
        source,
        values,
        labels,
        tags,
    })
}

// pub struct ProcessorRegistry {
//     indexes: Mutex<HashMap<String, (Arc<IndexConfig>, Arc<DocProcessor>)>>,
// }

// impl ProcessorRegistry {
//     pub fn new() -> Self {
//         Self {
//             indexes: Mutex::new(HashMap::new()),
//         }
//     }

//     pub async fn put_index(&self, index_name: String, index_config: IndexConfig) {
//         let mut indexes = self.indexes.lock().await;
//         let index_config = Arc::new(index_config);
//         let processor = Arc::new(DocProcessor::new(index_config.clone()));
//         match indexes.entry(index_name) {
//             Entry::Occupied(mut entry) => {
//                 entry.insert((index_config, processor));
//             }
//             Entry::Vacant(entry) => {
//                 entry.insert((index_config, processor));
//             }
//         }
//     }

//     pub async fn delete_index(&self, index_name: &str) {
//         let mut indexes = self.indexes.lock().await;
//         indexes.remove(index_name);
//     }

//     pub async fn get_processor(&self, index_name: &str) -> Result<Arc<DocProcessor>> {
//         let mut indexes = self.indexes.lock().await;
//         let (_, processor) = indexes.get_mut(index_name).ok_or_else(|| {
//             anyhow::anyhow!("Index `{}` not found in the processor registry", index_name)
//         })?;
//         Ok(processor.clone())
//     }
// }
