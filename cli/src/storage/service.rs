use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::{
    common::{index::IndexConfig, processors::ProcessorRegistry},
    metastore::{client::MetastoreClient, events::MetastoreEvent},
    storage::{
        batch_processor::BatchProcessor,
        client::StorageServiceClient,
        commands::{StorageServiceCommand, StorageServiceMailbox},
        storage_impl::StorageImpl,
    },
};

type BatchProcessorRegistry = ProcessorRegistry<BatchProcessor>;

pub struct StorageService {
    mailbox: Option<StorageServiceMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    processors: Arc<BatchProcessorRegistry>,
    storage: Arc<StorageImpl>,
    metastore_client: MetastoreClient,
}

impl StorageService {
    pub fn new(data_dir: PathBuf, metastore_client: MetastoreClient) -> Self {
        StorageService {
            mailbox: None,
            join_handle: None,
            processors: Arc::new(ProcessorRegistry::new()),
            storage: Arc::new(StorageImpl::new(data_dir)),
            metastore_client,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        self.storage.init().await?;
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());
        let mut metastore_events_stream = self.metastore_client.subscribe_to_events();


         // create processors for existing indexes
        let indexes = self.metastore_client.list_indexes().await?;
        let index_processors = indexes.into_iter().map(|index_meta|{
            let index_name = index_meta.name.clone();
            let processor = initialize_processor(self.storage.clone(), index_meta.name, index_meta.config);
            (index_name, processor)
        }).collect::<Vec<_>>();
        self.processors.add_initial_processors(index_processors).await;

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

    pub fn new_client(&self) -> StorageServiceClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        StorageServiceClient::new(mailbox.clone())
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
    storage: Arc<StorageImpl>,
    event: MetastoreEvent,
) -> Result<()> {
    match event {
        MetastoreEvent::IndexPut { name, config } => {
            processors_registry
                .put_index(name.clone(), || initialize_processor(storage, name, config))
                .await;
        }
        MetastoreEvent::IndexDeleted { name } => {
            processors_registry.delete_index(&name).await;
        }
    }
    Ok(())
}


fn initialize_processor(storage: Arc<StorageImpl>, index_name: String, config: IndexConfig) -> (Arc<IndexConfig>, Arc<BatchProcessor>) {
    let index_config = Arc::new(config);
    let processor = Arc::new(BatchProcessor::new(
        storage,
        index_name,
        index_config.clone(),
    ));
    (index_config, processor)
}
