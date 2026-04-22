use anyhow::{Ok, Result};
use tokio::sync::oneshot;

use crate::{
    common::{document::DocumentBatch, index::IndexConfig},
    ingest::{
        commands::{InsertServiceCommand, InsertServiceMailbox},
        doc_processor::{DocProcessorPolicy, ProcessingReport},
    },
};

#[derive(Debug, Clone)]
pub struct InsertServiceClient {
    mailbox: InsertServiceMailbox,
}

impl InsertServiceClient {
    pub fn new(mailbox: InsertServiceMailbox) -> Self {
        InsertServiceClient { mailbox }
    }

    pub async fn process_batch(
        &self,
        index_name: String,
        batch: DocumentBatch,
        policy: DocProcessorPolicy,
    ) -> anyhow::Result<ProcessingReport> {
        let (tx, rx) = oneshot::channel();
        let request = InsertServiceCommand::InsertBatch {
            index_name,
            batch,
            policy,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
