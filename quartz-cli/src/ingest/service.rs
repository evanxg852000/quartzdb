use std::sync::Arc;

use anyhow::Result;
// use tokio::sync::mpsc::{self};
use tokio::task::JoinHandle;

use crate::ingest::client::InsertServiceClient;
use crate::ingest::commands::{InsertServiceCommand, InsertServiceMailbox};
use crate::ingest::doc_processor::ProcessorRegistry;
use crate::metastore::events::{MetastoreEvent, MetastoreEventStream};

pub struct InsertService {
    mailbox: Option<InsertServiceMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: Arc<ProcessorRegistry>,
}

impl InsertService {
    pub fn new() -> Self {
        InsertService {
            mailbox: None,
            join_handle: None,
            processors: Arc::new(ProcessorRegistry::new()),
        }
    }

    pub async fn start(&mut self, mut metastore_events_stream: MetastoreEventStream) -> Result<()> {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());

        let processors_registry = self.processors.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            InsertServiceCommand::Stop => break,
                            other_command => handle_other_commands(processors_registry.clone(), other_command).await?,
                        }
                    }
                    Ok(event) = metastore_events_stream.recv() => {
                        handle_event(processors_registry.clone(), event).await?;
                    }
                    else => { // The else block or matching None handles channel closure
                        break;
                    }
                }
            }
            Ok(())
        });
        self.join_handle = Some(handle);
        Ok(())
    }

    pub fn new_client(&self) -> InsertServiceClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        InsertServiceClient::new(mailbox.clone())
    }
}

async fn handle_other_commands(
    processors_registry: Arc<ProcessorRegistry>,
    command: InsertServiceCommand,
) -> Result<()> {
    match command {
        InsertServiceCommand::InsertBatch {
            index_name,
            batch,
            policy,
            reply_sender,
        } => {
            let processor = processors_registry.get_processor(&index_name).await?;
            processor.process_batch(batch, policy, reply_sender).await
        }
        _ => {
            // already handled
            Ok(())
        }
    }
}

async fn handle_event(
    processors_registry: Arc<ProcessorRegistry>,
    event: MetastoreEvent,
) -> Result<()> {
    match event {
        MetastoreEvent::IndexPut { name, config } => {
            processors_registry.put_index(name, config).await;
        }
        MetastoreEvent::IndexDeleted { name } => {
            processors_registry.delete_index(&name).await;
        }
    }
    Ok(())
}
