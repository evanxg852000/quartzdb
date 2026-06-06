use std::{sync::Arc, vec};

use anyhow::Result;

use common::{convert::{record_batch_from_bytes, record_batch_to_json}, proto::SearchResponse};
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use storer::{
    client::StorerClient,
    service::StorerService,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub data: Vec<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SearchResult {
    pub fn from_error<E: ToString>(error: &E) -> Self {
        SearchResult {
            data: vec![],
            error: Some(error.to_string()),
        }
    }
}

impl TryFrom<RecordBatch> for SearchResult {
    type Error = String;

    fn try_from(record_batch: RecordBatch) -> Result<Self, Self::Error> {
        let data = record_batch_to_json(record_batch)?;
        Ok(SearchResult {data, error: None})
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

    pub async fn search(&self, table_name: String, query: String) -> Result<SearchResult> {
        let record_batch = self
            .context
            .storer_client
            .search(&table_name, &query)
            .await?;
        Ok(SearchResult::try_from(record_batch)?)
    }
}
