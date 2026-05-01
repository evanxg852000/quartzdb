use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::common::index::IndexMeta;
use crate::common::processors::Processor;
use crate::common::schema::Schema;
use crate::common::{
    document::{Document, DocumentBatch},
    index::IndexConfig,
};
use crate::storer::client::StorerServiceClient;

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

#[derive(Debug, Clone)]
pub struct IndexerContext {
    storer_client: StorerServiceClient,
    index_meta: Arc<IndexMeta>,
}

impl IndexerContext {
    pub fn new(storer_client: StorerServiceClient, index_meta: Arc<IndexMeta>) -> Self {
        Self {
            storer_client,
            index_meta,
        }
    }
}

#[derive(Debug)]
pub enum DocProcessorPolicy {
    Strict,
    Lenient,
}

#[derive(Debug)]
pub struct DocProcessor {
    context: Arc<IndexerContext>,
}

impl Processor for DocProcessor {}

impl DocProcessor {
    pub fn new(context: Arc<IndexerContext>) -> Self {
        Self { context }
    }

    pub async fn process_batch(
        &self,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    ) -> Result<()> {
        let (proto_batch, report) = process_batch(&self.context.index_meta.config, batch, policy)?;
        if !report.accepted {
            reply_sender
                .send(report)
                .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
            return Ok(());
        }

        self.context
            .storer_client
            .put_batch(&self.context.index_meta.name, proto_batch)
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
    proto_batch.sort(); // VERY IMPORTANT

    if matches!(policy, DocProcessorPolicy::Strict) && report.has_error() {
        report.accepted = false
    }

    Ok((proto_batch, report))
}

fn process_document(
    index_config: &IndexConfig,
    document: Document,
) -> Result<ProtoDocument, ValidationError> {
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
        timestamp,
        source,
        values,
        labels,
        tags,
    })
}
