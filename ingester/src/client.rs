use anyhow::Result;
use tokio::sync::oneshot;

use crate::{commands::{IngesterCommand, IngesterMailbox}, table_processor::{BatchProcessorPolicy, ProcessingReport}, document::IngestBatch};


#[derive(Debug, Clone)]
pub struct IngesterClient {
    mailbox: IngesterMailbox,
}

impl IngesterClient {
    pub fn new(mailbox: IngesterMailbox) -> Self {
        IngesterClient { mailbox }
    }

    pub async fn process_batch(
        &self,
        table_name: String,
        batch: IngestBatch,
        policy: BatchProcessorPolicy,
    ) -> Result<ProcessingReport> {
        let (tx, rx) = oneshot::channel();
        let request = IngesterCommand::IngestBatch {
            table_name,
            batch,
            policy,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
