use std::sync::Arc;

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::common::index::IndexConfig;
use crate::common::processors::ProcessorRegistry;
use crate::ingest::client::IngestServiceClient;
use crate::ingest::commands::{IngestServiceCommand, IngestServiceMailbox};
use crate::ingest::doc_processor::DocProcessor;
use crate::metastore::client::MetastoreClient;
use crate::metastore::events::MetastoreEvent;
use crate::storage::client::StorageServiceClient;

type DocProcessorRegistry = ProcessorRegistry<DocProcessor>;

pub struct IngestService {
    mailbox: Option<IngestServiceMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: Arc<DocProcessorRegistry>,
    metastore_client: MetastoreClient,
    storage_client: StorageServiceClient,
}

impl IngestService {
    pub fn new(metastore_client: MetastoreClient, storage_client: StorageServiceClient) -> Self {
        IngestService {
            mailbox: None,
            join_handle: None,
            processors: Arc::new(DocProcessorRegistry::new()),
            metastore_client,
            storage_client,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());
        let mut metastore_events_stream = self.metastore_client.subscribe_to_events();
        
        // create processors for existing indexes
        let indexes = self.metastore_client.list_indexes().await?;
        let index_processors = indexes.into_iter().map(|index_meta|{
            let index_name = index_meta.name.clone();
            let processor = initialize_processor(self.storage_client.clone(), index_meta.name, index_meta.config);
            (index_name, processor)
        }).collect::<Vec<_>>();
        self.processors.add_initial_processors(index_processors).await;

        let processors_registry = self.processors.clone();
        let moved_storage_client = self.storage_client.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            IngestServiceCommand::Stop => break,
                            other_command => handle_other_commands(processors_registry.clone(), other_command).await?,
                        }
                    }
                    Ok(event) = metastore_events_stream.recv() => {
                        handle_event(processors_registry.clone(), moved_storage_client.clone(), event).await?;
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

    pub fn new_client(&self) -> IngestServiceClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        IngestServiceClient::new(mailbox.clone())
    }
}

async fn handle_other_commands(
    processors_registry: Arc<DocProcessorRegistry>,
    command: IngestServiceCommand,
) -> Result<()> {
    match command {
        IngestServiceCommand::IngestBatch {
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
    processors_registry: Arc<DocProcessorRegistry>,
    storage_client: StorageServiceClient,
    event: MetastoreEvent,
) -> Result<()> {
    match event {
        MetastoreEvent::IndexPut { name, config } => {
            processors_registry
                .put_index(name.clone(), || initialize_processor(storage_client, name, config))
                .await;
        }
        MetastoreEvent::IndexDeleted { name } => {
            processors_registry.delete_index(&name).await;
        }
    }
    Ok(())
}

fn initialize_processor(storage_client: StorageServiceClient, index_name: String, config: IndexConfig) -> (Arc<IndexConfig>, Arc<DocProcessor>) {
    let index_config = Arc::new(config);
    let processor = Arc::new(DocProcessor::new(
        index_name,
        index_config.clone(),
        storage_client,
    ));
    (index_config, processor)
}
