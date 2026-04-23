use tokio::sync::{mpsc, oneshot};

use crate::{
    common::document::DocumentBatch,
    ingest::doc_processor::{DocProcessorPolicy, ProcessingReport},
};

pub type IngestServiceMailbox = mpsc::Sender<IngestServiceCommand>;

#[derive(Debug)]
pub enum IngestServiceCommand {
    Stop,
    IngestBatch {
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    },
}
