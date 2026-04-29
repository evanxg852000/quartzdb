use std::sync::Arc;

use anyhow::Result;
use tokio::task::JoinHandle;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::common::index::IndexMeta;
use crate::common::processors::ProcessorRegistry;
use crate::indexer::client::IndexerServiceClient;
use crate::indexer::commands::{IndexerServiceCommand, IndexerServiceMailbox};
use crate::indexer::doc_processor::{DocProcessor, IndexerContext};
use crate::metastore::client::MetastoreClient;
use crate::metastore::events::MetastoreEvent;
use crate::storer::client::StorerServiceClient;

type DocProcessorRegistry = ProcessorRegistry<DocProcessor>;

pub struct IndexerService {
    mailbox: Option<IndexerServiceMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: Arc<DocProcessorRegistry>,
    metastore_client: MetastoreClient,
    storer_client: StorerServiceClient,
}

impl IndexerService {
    pub fn new(metastore_client: MetastoreClient, storer_client: StorerServiceClient) -> Self {
        IndexerService {
            mailbox: None,
            join_handle: None,
            processors: Arc::new(DocProcessorRegistry::new()),
            metastore_client,
            storer_client,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());
        let mut metastore_events_stream = self.metastore_client.subscribe_to_events();

        // create processors for existing indexes
        let indexes = self.metastore_client.list_indexes().await?;
        let index_processors = stream::iter(indexes)
            .map(|index_meta| async {
                let index_name = index_meta.name.clone();
                let processor = initialize_processor(self.storer_client.clone(), Arc::new(index_meta)).await?;
                anyhow::Result::<_>::Ok((index_name, processor))
            })
            .buffer_unordered(20)
            .try_collect()
            .await?;
        self.processors
            .add_initial_processors(index_processors)
            .await;

        let processors_registry = self.processors.clone();
        let moved_storage_client = self.storer_client.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            IndexerServiceCommand::Stop => break,
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

    pub fn new_client(&self) -> IndexerServiceClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        IndexerServiceClient::new(mailbox.clone())
    }
}

async fn handle_other_commands(
    processors_registry: Arc<DocProcessorRegistry>,
    command: IndexerServiceCommand,
) -> Result<()> {
    match command {
        IndexerServiceCommand::IngestBatch {
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
    storage_client: StorerServiceClient,
    event: MetastoreEvent,
) -> Result<()> {
    match event {
        MetastoreEvent::IndexPut { name, index_meta } => {
            processors_registry
                .put_index(name.clone(), || {
                    initialize_processor(storage_client, Arc::new(index_meta))
                })
                .await;
        }
        MetastoreEvent::IndexDeleted { name } => {
            processors_registry.delete_index(&name).await;
        }
    }
    Ok(())
}

async fn initialize_processor(
    storer_client: StorerServiceClient,
    index_meta: Arc<IndexMeta>,
) -> Result<(Arc<IndexMeta>, Arc<DocProcessor>)> {
    let context = Arc::new(IndexerContext::new(storer_client, index_meta.clone()));
    let processor = Arc::new(DocProcessor::new(context));
    Ok((index_meta, processor))
}
