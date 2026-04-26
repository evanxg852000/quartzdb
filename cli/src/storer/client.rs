use anyhow::Result;
use proto::quartzdb::ProtoDocumentBatch;
use tokio::sync::oneshot;

use crate::storer::commands::{StorageServiceCommand, StorageServiceMailbox};

#[derive(Debug, Clone)]
pub struct StorerServiceClient {
    mailbox: StorageServiceMailbox,
}

impl StorerServiceClient {
    pub fn new(mailbox: StorageServiceMailbox) -> Self {
        StorerServiceClient { mailbox }
    }

    pub async fn put_batch(&self, index_name: &str, batch: ProtoDocumentBatch) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let request = StorageServiceCommand::PutBatch {
            index_name: index_name.into(),
            batch,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
