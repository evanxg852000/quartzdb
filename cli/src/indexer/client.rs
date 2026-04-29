use tokio::sync::oneshot;

use crate::{
    common::document::DocumentBatch,
    indexer::{
        commands::{IndexerServiceCommand, IndexerServiceMailbox},
        doc_processor::{DocProcessorPolicy, ProcessingReport},
    },
};

#[derive(Debug, Clone)]
pub struct IndexerServiceClient {
    mailbox: IndexerServiceMailbox,
}

impl IndexerServiceClient {
    pub fn new(mailbox: IndexerServiceMailbox) -> Self {
        IndexerServiceClient { mailbox }
    }

    pub async fn process_batch(
        &self,
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
    ) -> anyhow::Result<ProcessingReport> {
        let (tx, rx) = oneshot::channel();
        let request = IndexerServiceCommand::IngestBatch {
            index_name,
            batch,
            policy,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
