

use anyhow::{Result};
use hashbrown::HashMap;
use tokio::task::JoinHandle;
use tokio::sync::{oneshot::{self}, mpsc::{self}};

use crate::ingest::doc_processor::BatchRequest;
use crate::ingest::{client::InsertServiceClient, doc_processor::DocProcessor};


pub type BatchRequestSender = mpsc::Sender<BatchRequest>;

pub struct InsertService{
    sender: Option<BatchRequestSender>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: HashMap<String, DocProcessor>
}

impl InsertService {
    pub fn new() -> Self {
        InsertService { 
            sender: None,
            join_handle: None,
            processors: HashMap::new(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(500);
        self.sender = Some(tx.clone());

        let handle = tokio::spawn(async move {
            loop {
                //TODO: use tokio::select! for graceful shutdown
                let batch_request_opt = rx.recv().await;
                match batch_request_opt {
                    Some(batch_request) => {
                        let processor = DocProcessor::new();
                        processor.process_batch(batch_request).await?;
                    },
                    None => break,
                }
            }
            Ok(())
        });
        self.join_handle = Some(handle);
        Ok(())
    }

    pub fn new_client(&self) -> InsertServiceClient {
        let sender = self.sender.as_ref().expect("start the service before creating a client");
        InsertServiceClient::new(sender.clone())
    }
}
