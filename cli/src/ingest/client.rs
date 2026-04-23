use tokio::sync::oneshot;

use crate::{
    common::document::DocumentBatch,
    ingest::{
        commands::{IngestServiceCommand, IngestServiceMailbox},
        doc_processor::{DocProcessorPolicy, ProcessingReport},
    },
};

#[derive(Debug, Clone)]
pub struct IngestServiceClient {
    mailbox: IngestServiceMailbox,
}

impl IngestServiceClient {
    pub fn new(mailbox: IngestServiceMailbox) -> Self {
        IngestServiceClient { mailbox }
    }

    pub async fn process_batch(
        &self,
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
    ) -> anyhow::Result<ProcessingReport> {
        let (tx, rx) = oneshot::channel();
        let request = IngestServiceCommand::IngestBatch {
            index_name,
            batch,
            policy,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
