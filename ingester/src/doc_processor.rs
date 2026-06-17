use std::sync::Arc;

use anyhow::{Result, anyhow};
use common::{catalog::{TableConfig, TableMeta}, processors::Processor, schema::Schema};
use datafusion::{arrow::{array::{RecordBatch, StringBuilder}, datatypes::{DataType, Field, Schema as DataFusionSchema}}};
use serde::{Deserialize, Serialize};
use storer::{client::StorerClient, service::{StorerPutRequest, StorerPutRequestInfo, StorerService}};
use tokio::sync::oneshot;
use serde_json::Value as JsonValue;

use crate::document::{IngestBatch, IngestDocument};

const SOURCE_COLUMN_NAME: &'static str = "_source";

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationError {
    pub source: String,
    pub line: u64,
    pub error: String,
}

impl ValidationError {
    pub fn new(source: &str, line: u64, error: String) -> Self {
        Self {
            source: source.to_string(),
            line,
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
pub struct IngesterContext {
    storer_client: StorerClient,
    table_meta: Arc<TableMeta>,
}

impl IngesterContext {
    pub fn new(storer_client: StorerClient, table_meta: Arc<TableMeta>) -> Self {
        Self {
            storer_client,
            table_meta,
        }
    }
}

#[derive(Debug)]
pub enum BatchProcessorPolicy {
    Strict,
    Lenient,
}

#[derive(Debug, Clone)]
pub struct DocProcessor {
    context: Arc<IngesterContext>,
}

impl Processor for DocProcessor {}

impl DocProcessor {
    pub fn new(context: Arc<IngesterContext>) -> Self {
        Self { context }
    }

    pub fn get_table_meta(&self) -> Arc<TableMeta> {
        self.context.table_meta.clone()
    }

    pub async fn process_batch(
        &self,
        batch: IngestBatch,
        policy: BatchProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    ) -> Result<()> {
        let (storer_put_request, report) = process_batch(&self.context, batch, policy)?;
        if !report.accepted {
            reply_sender
                .send(report)
                .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
            return Ok(());
        }

        //TODO: store batch in WAL & reply to client before 
        // putting to storer. spawn task to put to storer with support for retrying on failure.
        // once storer put succeeds, truncate the wall
        self.context
            .storer_client
            .put(storer_put_request)
            .await?;
        reply_sender
            .send(report)
            .map_err(|_| anyhow!("Failed to send on reply channel"))?;
        Ok(())
    }
}

fn process_batch(
    context: &IngesterContext,
    batch: IngestBatch,
    policy: BatchProcessorPolicy,
) -> Result<(StorerPutRequest, ProcessingReport)> {
    let mut report = ProcessingReport::new(batch.len());
    let mut document_source_array_builder = StringBuilder::new();
    for document in batch.0 {
        match validate_document(&context.table_meta.config, &document) {
            Ok(_) => document_source_array_builder.append_value(document.source),
            Err(err) => report.add_error(err),
        }
    }

    if matches!(policy, BatchProcessorPolicy::Strict) && report.has_error() {
        report.accepted = false
    }

    let record_batch = RecordBatch::try_new(
        Arc::new(DataFusionSchema::new(vec![
            Field::new(SOURCE_COLUMN_NAME, DataType::Utf8View, false),
        ])),
        vec![Arc::new(document_source_array_builder.finish())],
    )?;

     let storer_put_request = StorerPutRequest {
        info: StorerPutRequestInfo {
            table_name: context.table_meta.name.clone(),
        },
        data: record_batch,
    };

    Ok((storer_put_request, report))
}

fn validate_document(
    table_config: &TableConfig,
    document: &IngestDocument,
) -> Result<(), ValidationError> {
    let parsed_document = serde_json::from_str::<JsonValue>(&document.source)
        .map_err(|err| ValidationError::new(&document.source, document.line, err.to_string()))?;
    let _ = Schema::extract_timestamp(table_config, &parsed_document)
        .map_err(|err| ValidationError::new(&document.source, document.line, err.to_string()))?;
    let _ = Schema::extract_field_values(table_config, &parsed_document)
        .map_err(|err| ValidationError::new(&document.source, document.line, err.to_string()))?;
    let _ = Schema::extract_label_values_as_object(table_config, &parsed_document)
        .map_err(|err| ValidationError::new(&document.source, document.line, err.to_string()))?;
    let _ = Schema::extract_tag_values(table_config, &parsed_document)
        .map_err(|err| ValidationError::new(&document.source, document.line, err.to_string()))?;
    Ok(())
}
