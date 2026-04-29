use tokio::sync::{mpsc, oneshot};

use crate::{
    common::document::DocumentBatch,
    indexer::doc_processor::{DocProcessorPolicy, ProcessingReport},
};

pub type IndexerServiceMailbox = mpsc::Sender<IndexerServiceCommand>;

#[derive(Debug)]
pub enum IndexerServiceCommand {
    Stop,
    IngestBatch {
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    },
}
