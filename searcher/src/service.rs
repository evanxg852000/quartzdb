use std::sync::Arc;

use anyhow::Result;

use metastore::{
    client::MetastoreClient,
    events::{MetastoreEvent, MetastoreEventsFetcher},
};
use storage::{Storage, configs::StorageConfig};
use storer::client::StorerClient;
use tokio::task::JoinHandle;

use crate::{
    client::SearcherClient,
    commands::{SearcherCommand, SearcherMailbox},
    configs::SearcherConfig,
    search_processor::{SearchProcessor, SearchResult, SearcherContext},
};

const SEARCHER_DIR: &str = "searcher";

pub struct SearcherService {
    config: SearcherConfig,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    storer_client: StorerClient,
    mailbox: Option<SearcherMailbox>,
    join_handle: Option<JoinHandle<Result<()>>>,
    search_processor: Arc<SearchProcessor>,
    /// Serves for fetching metastore events & node heartbeat
    metastore_events_fetcher: MetastoreEventsFetcher,
}

impl SearcherService {
    pub async fn try_new(
        searcher_config: &SearcherConfig,
        storage_config: &StorageConfig,
        metastore_client: MetastoreClient,
        storer_client: StorerClient,
    ) -> Result<Self> {
        let storage = storage_config.derive(SEARCHER_DIR, None).build().await?;
        let search_processor = Arc::new(SearchProcessor::new(Arc::new(SearcherContext::new(
            storer_client.clone(),
        ))));
        let metastore_events_fetcher = MetastoreEventsFetcher::new(metastore_client.clone());
        Ok(SearcherService {
            config: searcher_config.clone(),
            storage,
            metastore_client,
            storer_client,
            mailbox: None,
            join_handle: None,
            search_processor,
            metastore_events_fetcher,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(500);
        self.mailbox = Some(command_tx.clone());

        self.metastore_events_fetcher
            .start(std::time::Duration::from_secs(5))
            .await?;
        let mut metastore_events_stream = self.metastore_events_fetcher.subscribe_to_events();

        let search_processor = self.search_processor.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_rx.recv() => {
                        match command {
                            SearcherCommand::Stop => break,
                            search_command => handle_search_command(search_processor.clone(), search_command).await?,
                        }
                    }
                    Ok(event) = metastore_events_stream.recv() => {
                        handle_event(search_processor.clone(), event).await?;
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

    pub fn new_client(&self) -> SearcherClient {
        let mailbox = self
            .mailbox
            .as_ref()
            .expect("start the service before creating a client");
        SearcherClient::new(mailbox.clone())
    }
}

async fn handle_search_command(
    search_processor: Arc<SearchProcessor>,
    command: SearcherCommand,
) -> Result<()> {
    match command {
        SearcherCommand::Search {
            table_name,
            query,
            reply_sender,
        } => {
            let response: Result<SearchResult, anyhow::Error> = async {
                let response = search_processor.query(table_name, query).await?;
                Ok(response)
            }
            .await;
            let search_result = match response {
                Ok(search_result) => search_result,
                Err(err) => SearchResult::from_error(&err),
            };
            reply_sender
                .send(search_result)
                .map_err(|_| anyhow::anyhow!("Failed to send reply"))?;
            Ok(())
        }
        _ => {
            // already handled
            Ok(())
        }
    }
}

async fn handle_event(search_processor: Arc<SearchProcessor>, event: MetastoreEvent) -> Result<()> {
    match event.event_type {
        _ => {
            eprintln!("Unhandled event: {:?}", event);
        }
    }
    Ok(())
}
