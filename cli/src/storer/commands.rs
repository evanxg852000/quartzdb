use tokio::sync::{mpsc, oneshot};

use proto::quartzdb::ProtoDocumentBatch;

pub type StorageServiceMailbox = mpsc::Sender<StorageServiceCommand>;

#[derive(Debug)]
pub enum StorageServiceCommand {
    Stop,
    PutBatch {
        index_name: String,
        batch: ProtoDocumentBatch,
        reply_sender: oneshot::Sender<()>,
    },
}
