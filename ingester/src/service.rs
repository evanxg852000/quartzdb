use std::sync::Arc;

use anyhow::Result;

use metastore::{client::MetastoreClient, events::{MetastoreEvent, MetastoreEventType, MetastoreEventsFetcher}};
use storage::{Storage, configs::StorageConfig};
use storer::{client::StorerClient};
use tokio::task::JoinHandle;

use crate::{client::IngesterClient, commands::{IngesterCommand, IngesterMailbox}, configs::IngesterConfig, table_processor::ProcessingReport, table_processor_registry::TableProcessorRegistry};

const INGESTER_DIR: &str = "ingester";

pub struct IngesterService {
    config: IngesterConfig,
    // Use for WAL
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    storer_client: StorerClient,
    mailbox: Option<IngesterMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    table_processor_registry: Arc<TableProcessorRegistry>,
    /// Serves for fetching metastore events & node heartbeat
    metastore_events_fetcher: MetastoreEventsFetcher,
}

impl IngesterService {
    pub async fn try_new(
        ingester_config: &IngesterConfig,
        storage_config: &StorageConfig,
        metastore_client: MetastoreClient,
        storer_client: StorerClient,
    ) -> Result<Self> {
        let storage = storage_config.derive(INGESTER_DIR, None).build().await?;
        let table_processor_registry = Arc::new(TableProcessorRegistry::try_new(500, storage.clone(), storer_client.clone(), metastore_client.clone()).await?);
        let metastore_events_fetcher = MetastoreEventsFetcher::new(metastore_client.clone());
        Ok(IngesterService {
            config: ingester_config.clone(),
            storage,
            metastore_client,
            storer_client,
            mailbox: None,
            join_handle: None,
            table_processor_registry,
            metastore_events_fetcher,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());

        self.metastore_events_fetcher.start(std::time::Duration::from_secs(5)).await?;
        let mut metastore_events_stream = self.metastore_events_fetcher.subscribe_to_events();

        let processors_registry = self.table_processor_registry.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            IngesterCommand::Stop => break,
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

    pub fn new_client(&self) -> IngesterClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        IngesterClient::new(mailbox.clone())
    }
}

async fn handle_other_commands(
    table_processor_registry: Arc<TableProcessorRegistry>,
    command: IngesterCommand,
) -> Result<()> {
    match command {
        IngesterCommand::IngestBatch {
            table_name,
            batch,
            policy,
            reply_sender,
        } => {
            let response: Result<ProcessingReport, anyhow::Error> = async {
                let processor = table_processor_registry.get_processor(&table_name).await?;
                let report = processor.process_batch(batch, policy).await?;
                Ok(report)
            }.await;
            let report = match response {
                Ok(report) => report,
                Err(err) => ProcessingReport::from_error(&err),
            };
            reply_sender
                .send(report)
                .map_err(|_| anyhow::anyhow!("Failed to send reply"))?;
            Ok(())
        }
        _ => {
            // already handled
            Ok(())
        }
    }
}

async fn handle_event(
    table_processor_registry: Arc<TableProcessorRegistry>,
    event: MetastoreEvent,
) -> Result<()> {
    match event.event_type {
        MetastoreEventType::TablePut { name, table_meta } => {
            table_processor_registry
                .refresh_processor(&name, table_meta)
                .await?;
        }
        MetastoreEventType::TableDeleted { name } => {
            table_processor_registry.remove_processor(&name).await?;
        }
        _ => {
            eprintln!("Unhandled event: {:?}", event);
        }
    }
    Ok(())
}

// async fn new_processor(
//     storer_client: StorerClient,
//     table_meta: TableMeta,
// ) -> Result<ProcessorWrapper<DocProcessor>> {
//     let context = Arc::new(IngesterContext::new(storer_client.clone(), Arc::new(table_meta)));
//     let processor = Arc::new(DocProcessor::new(context));
//     let processor_wrapper = ProcessorWrapper::new(processor.get_table_meta(), processor);
//     Ok(processor_wrapper)
// }
