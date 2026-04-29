use std::sync::Arc;

use anyhow::Result;
use futures_util::stream;
use storage::Storage;
use tokio::task::JoinHandle;
use futures::stream::{StreamExt, TryStreamExt};

use crate::{
    common::{index::IndexMeta, processors::ProcessorRegistry},
    metastore::{client::MetastoreClient, events::MetastoreEvent},
    storer::{
        batch_processor::{BatchProcessor, StorerContext},
        client::StorerServiceClient,
        commands::{StorageServiceCommand, StorageServiceMailbox},
    },
};

type BatchProcessorRegistry = ProcessorRegistry<BatchProcessor>;

pub struct StorerService {
    mailbox: Option<StorageServiceMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: Arc<BatchProcessorRegistry>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
}

impl StorerService {
    pub fn new(storage: Arc<dyn Storage>, metastore_client: MetastoreClient) -> Self {
        StorerService {
            mailbox: None,
            join_handle: None,
            processors: Arc::new(ProcessorRegistry::new()),
            storage,
            metastore_client,
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
                let processor = initialize_processor(self.storage.clone().clone(), Arc::new(index_meta)).await?;
                anyhow::Result::<_>::Ok((index_name, processor))
            })
            .buffer_unordered(20)
            .try_collect()
            .await?;
        self.processors
            .add_initial_processors(index_processors)
            .await;

        let processors_registry = self.processors.clone();
        let moved_storage = self.storage.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            StorageServiceCommand::Stop => break,
                            other_command => handle_other_commands(processors_registry.clone(), other_command).await?,
                        }
                    }
                    Ok(event) = metastore_events_stream.recv() => {
                        handle_event(processors_registry.clone(), moved_storage.clone(), event).await?;
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

    pub fn new_client(&self) -> StorerServiceClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        StorerServiceClient::new(mailbox.clone())
    }
}

async fn handle_other_commands(
    processors_registry: Arc<BatchProcessorRegistry>,
    command: StorageServiceCommand,
) -> Result<()> {
    match command {
        StorageServiceCommand::PutBatch {
            index_name,
            batch,
            reply_sender,
        } => {
            let processor = processors_registry.get_processor(&index_name).await?;
            processor.put_batch(batch, reply_sender).await
        }
        _ => {
            // already handled
            Ok(())
        }
    }
}

async fn handle_event(
    processors_registry: Arc<BatchProcessorRegistry>,
    storage: Arc<dyn Storage>,
    event: MetastoreEvent,
) -> Result<()> {
    match event {
        MetastoreEvent::IndexPut { name, index_meta } => {
            processors_registry
                .put_index(name.clone(), || async {
                    initialize_processor(storage, Arc::new(index_meta)).await
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
    storage: Arc<dyn Storage>,
    index_meta: Arc<IndexMeta>,
) -> Result<(Arc<IndexMeta>, Arc<BatchProcessor>)> {
    let context = Arc::new(StorerContext::new(storage, index_meta.clone()).await?);
    let processor = Arc::new(BatchProcessor::new(context));
    Ok((index_meta, processor))
}
