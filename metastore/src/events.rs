use std::{sync::atomic::AtomicU64, time::Duration};

use anyhow::Result;
use common::catalog::TableMeta;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::client::MetastoreClient;
use crate::service::MetastoreService;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreEvent {
    pub timestamp: u64,
    pub event_type: MetastoreEventType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetastoreEventType {
    TablePut { name: String, table_meta: TableMeta },
    TableDeleted { name: String },
    NodeJoin { id: String, services: Vec<String> },
    NodeLeft { id: String },
}

pub type MetastoreEventsStream = Receiver<MetastoreEvent>;

pub struct MetastoreEventsFetcher {
    client: MetastoreClient,
    mailbox: Sender<MetastoreEvent>,
    join_handle: Option<JoinHandle<Result<()>>>,
}

impl MetastoreEventsFetcher {
    pub fn new(client: MetastoreClient) -> Self {
        let (mailbox, _) = broadcast::channel(100);
        Self {
            client,
            mailbox,
            join_handle: None,
        }
    }

    pub async fn start(&mut self, fetch_interval: Duration) -> Result<()> {
        let moved_mailbox = self.mailbox.clone();
        let moved_client = self.client.clone();
        let handle = tokio::spawn(async move {
            let mut fetch_ticker = tokio::time::interval(fetch_interval);
            let mut last_checkin: Option<u64> = None;
            loop {
                fetch_ticker.tick().await;
                let events = moved_client.fetch_events(last_checkin).await?;
                if let Some(event) = events.iter().last() {
                    last_checkin = Some(event.timestamp);
                }
                for event in events {
                    moved_mailbox.send(event)?;
                }
            }
            Ok(())
        });

        self.join_handle = Some(handle);
        Ok(())
    }

    pub fn subscribe_to_events(&self) -> MetastoreEventsStream {
        self.mailbox.subscribe()
    }
}

pub struct MetastoreEventQueue {
    clock: AtomicU64,
    events: Vec<MetastoreEvent>,
}

impl MetastoreEventQueue {
    pub fn new() -> Self {
        Self {
            clock: AtomicU64::new(0),
            events: Vec::new(),
        }
    }

    pub fn publish(&mut self, event_type: MetastoreEventType) {
        let timestamp = self.clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let metastore_event = MetastoreEvent {
            timestamp,
            event_type,
        };
        self.events.push(metastore_event);
    }

    pub fn fetch(&self, last_checkin: Option<u64>) -> Vec<MetastoreEvent> {
        //TODO: clean up old events (older than 5min)
        if let Some(checkin) = last_checkin {
            return self
                .events
                .iter()
                .filter(|event| event.timestamp > checkin)
                .cloned()
                .take(50)
                .collect();
        }

        self.events.iter().cloned().take(50).collect()
    }
}
