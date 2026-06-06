use std::{sync::Arc, vec};

use anyhow::Result;

use serde::{Deserialize, Serialize};
use storer::{
    client::StorerClient,
    service::{StorerQueryRequest, StorerQueryResponse, StorerService},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub count: u64,
    pub rows: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SearchResult {
    pub fn from_error<E: ToString>(error: &E) -> Self {
        SearchResult {
            count: 0,
            rows: vec![],
            error: Some(error.to_string()),
        }
    }
}

impl From<StorerQueryResponse> for SearchResult {
    fn from(value: StorerQueryResponse) -> Self {
        SearchResult {
            count: value.count,
            rows: value.rows,
            error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearcherContext {
    storer_client: StorerClient,
}

impl SearcherContext {
    pub fn new(storer_client: StorerClient) -> Self {
        Self { storer_client }
    }
}

#[derive(Debug, Clone)]
pub struct SearchProcessor {
    context: Arc<SearcherContext>,
}

impl SearchProcessor {
    pub fn new(context: Arc<SearcherContext>) -> Self {
        Self { context }
    }

    pub async fn query(&self, table_name: String, query: String) -> Result<SearchResult> {
        let storer_query_request = StorerQueryRequest { table_name, query };
        let response = self
            .context
            .storer_client
            .query(storer_query_request)
            .await?;
        Ok(SearchResult::from(response))
    }
}
