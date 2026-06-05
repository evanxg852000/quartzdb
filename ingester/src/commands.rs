use tokio::sync::{mpsc, oneshot};

use crate::{table_processor::{BatchProcessorPolicy, ProcessingReport}, document::IngestBatch};


pub type IngesterMailbox = mpsc::Sender<IngesterCommand>;

#[derive(Debug)]
pub enum IngesterCommand {
    Stop,
    IngestBatch {
        table_name: String,
        batch: IngestBatch,
        policy: BatchProcessorPolicy,
        reply_sender: oneshot::Sender<ProcessingReport>,
    },
}
