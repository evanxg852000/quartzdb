use anyhow::Ok;

use crate::ingest::client::InsertServiceClient;

pub type MessageSender = tokio::sync::mpsc::Sender<Vec<u8>>;

pub struct InsertService{
    sender: Option<MessageSender>,
}

impl InsertService {
    pub fn new() -> Self {
        InsertService { sender: None }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let (tx, _) = tokio::sync::mpsc::channel(500);
        self.sender = Some(tx.clone());
        Ok(())
    }

    pub fn new_client(&self) -> InsertServiceClient {
        let sender = self.sender.as_ref().expect("start the service before creating a client");
        InsertServiceClient::new(sender.clone())
    }
}
