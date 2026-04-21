use tokio::sync::oneshot;

use crate::{
    common::document::DocumentBatch, 
    ingest::{
        doc_processor::{
            BatchRequest, 
            DocProcessorPolicy, 
            ProcessingReport,
        }, 
        service::BatchRequestSender
    }
};



#[derive(Debug, Clone)]
pub struct InsertServiceClient {
    sender: BatchRequestSender,
}

impl InsertServiceClient {
    pub fn new(sender: BatchRequestSender) -> Self {
        InsertServiceClient { sender }
    }

    pub async fn process_batch(&self, batch: DocumentBatch, policy: DocProcessorPolicy) -> anyhow::Result<ProcessingReport> {
        let(tx, rx) = oneshot::channel();
        let request = BatchRequest{
            batch: batch,
            policy: policy,
            reply_sender: tx,
        };
        self.sender.send(request).await?;
        let report  = rx.await?;
        Ok(report)
    }
}
