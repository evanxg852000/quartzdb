use crate::ingest::service::MessageSender;



#[derive(Debug, Clone)]
pub struct InsertServiceClient {
    sender: MessageSender,
}

impl InsertServiceClient {
    pub fn new(sender: MessageSender) -> Self {
        InsertServiceClient { sender }
    }

    pub async fn send_message(&self, message: Vec<u8>) -> anyhow::Result<()> {
        self.sender.send(message).await?;
        Ok(())
    }
}
