use anyhow::Result;
use common::{
    catalog::{FieldValue, TableConfig},
    schema::Schema,
};
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use serde_json::Value as JsonValue;

#[derive(Debug)]
pub struct StorerDocument {
    /// timestamp in nanoseconds precision
    pub timestamp: i64,
    /// source document as json string
    pub source: String,
    /// extracted field values
    pub values: Vec<FieldValue>,
    /// extracted label values as JSON object (fts)
    pub labels: JsonValue,
    /// extracted  tag values
    pub tags: Vec<String>,
}

impl StorerDocument {
    pub fn new(
        timestamp: i64,
        source: String,
        values: Vec<FieldValue>,
        labels: JsonValue,
        tags: Vec<String>,
    ) -> Self {
        Self {
            timestamp,
            source,
            values,
            labels,
            tags,
        }
    }

    pub fn try_from_json_str(table_config: &TableConfig, source: &str) -> Result<Self> {
        let parsed_document = serde_json::from_str::<JsonValue>(source)?;
        let timestamp = Schema::extract_timestamp(table_config, &parsed_document)?;
        let values = Schema::extract_field_values(table_config, &parsed_document)?;
        let labels = Schema::extract_label_values_as_object(table_config, &parsed_document)?;
        let tags = Schema::extract_tag_values(table_config, &parsed_document)?;
        Ok(Self::new(
            timestamp,
            source.to_string(),
            values,
            labels,
            tags,
        ))
    }
}

#[derive(Debug)]
pub struct StorerBatch {
    pub documents: Vec<StorerDocument>,
}

impl StorerBatch {
    pub fn new() -> Self {
        Self { documents: vec![] }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            documents: Vec::with_capacity(capacity),
        }
    }

    pub fn try_from_record_batch(table_config: &TableConfig, batch: RecordBatch) -> Result<Self> {
        let source_doc_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("expected first column to be"))?;
        let mut batch = StorerBatch::with_capacity(source_doc_array.len());
        for source_doc_opt in source_doc_array {
            let source_doc =
                source_doc_opt.ok_or_else(|| anyhow::anyhow!("source cannot be null"))?;
            let document = StorerDocument::try_from_json_str(table_config, source_doc)?;
            batch.add_document(document);
        }
        Ok(batch)
    }

    pub fn add_document(&mut self, document: StorerDocument) {
        self.documents.push(document);
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn sort(&mut self) {
        self.documents.sort_by_key(|document| document.timestamp);
    }

    pub fn min_timestamp(&self) -> i64 {
        if self.documents.is_empty() {
            return 0;
        }
        self.documents[0].timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        let length = self.documents.len();
        if length == 0 {
            return 0;
        }
        self.documents[length - 1].timestamp
    }
}
