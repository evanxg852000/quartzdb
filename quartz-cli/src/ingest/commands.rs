use tokio::sync::{mpsc, oneshot};

use crate::{
    common::{document::DocumentBatch, index::IndexConfig},
    ingest::doc_processor::{DocProcessorPolicy, ProcessingReport},
};

pub type InsertServiceMailbox = mpsc::Sender<InsertServiceCommand>;

#[derive(Debug)]
pub enum InsertServiceCommand {
    Stop,
    InsertBatch {
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    },
}
