use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::common::{index::IndexConfig, document::{Document, DocumentBatch}};

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationError {
    doc: Document,
    message: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessingReport {
    pub num_docs: usize, 
    pub accepted: bool,
    pub errors: Vec<ValidationError>
}

impl ProcessingReport {
    
    pub fn new(num_docs: usize) -> Self {
        Self {
            num_docs: num_docs,
            accepted: false,
            errors: vec![] 
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

pub enum DocProcessorPolicy {
    Strict,
    Lenient,
}

pub struct BatchRequest {
    pub batch: DocumentBatch,
    pub policy: DocProcessorPolicy,
    pub reply_sender: oneshot::Sender<ProcessingReport>,
}

pub struct DocProcessor {
    index_config: IndexConfig, 

}

impl DocProcessor {

    pub fn new(index_config: IndexConfig) -> Self {
        Self { index_config }
    }

    pub async fn process_batch(&self, request: BatchRequest) -> Result<()> {
        let BatchRequest{mut batch, policy, reply_sender} = request;
        let mut report = validate_batch(&mut batch);
        if  matches!(policy, DocProcessorPolicy::Strict) && report.has_error() {
            reply_sender.send(report)
                .map_err(|_| anyhow!("Failed to send on reply channel"))?;
            return Ok(());
        }

        // we accepted the batch
        report.accepted = true;
        
        //TODO: create the split with the filtered batch
        // & send the report

        reply_sender.send(report)
            .map_err(|_| anyhow!("Failed to send on reply channel"))?;
        Ok(())
    }

}


fn validate_batch(batch: &mut DocumentBatch) -> ProcessingReport {
    let report = ProcessingReport::new(batch.len());
    //TODO: filter the doc back a see what docs are wrong
    report
}
