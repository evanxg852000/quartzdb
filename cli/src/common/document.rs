use std::vec;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Document {
    pub id: u64,
    pub line_number: u64,
    pub json_object: JsonValue,
    pub raw_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DocumentBatch(pub Vec<Document>);

impl DocumentBatch {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn add_document(&mut self, value: JsonValue, size: usize) {
        let next = self.0.len() as u64 + 1;
        self.0.push(Document {
            id: next,
            line_number: next,
            json_object: value,
            raw_size: size,
        });
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
