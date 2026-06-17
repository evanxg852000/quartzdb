use anyhow::Result;
use tokio::sync::oneshot;

use crate::{
    commands::{SearcherCommand, SearcherMailbox},
    search_processor::SearchResult,
};

#[derive(Debug, Clone)]
pub struct SearcherClient {
    mailbox: SearcherMailbox,
}

impl SearcherClient {
    pub fn new(mailbox: SearcherMailbox) -> Self {
        SearcherClient { mailbox }
    }

    pub async fn search(&self, table_name: String, query: String) -> Result<SearchResult> {
        let (tx, rx) = oneshot::channel();
        let request = SearcherCommand::Search {
            table_name,
            query,
            reply_sender: tx,
        };
        self.mailbox.send(request).await?;
        Ok(rx.await?)
    }
}
