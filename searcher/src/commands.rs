use tokio::sync::{mpsc, oneshot};

use crate::search_processor::SearchResult;

pub type SearcherMailbox = mpsc::Sender<SearcherCommand>;

#[derive(Debug)]
pub enum SearcherCommand {
    Stop,
    Search {
        table_name: String,
        query: String,
        reply_sender: oneshot::Sender<SearchResult>,
    },
}
