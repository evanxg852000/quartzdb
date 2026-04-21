use std::vec;

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Document {
    pub line_number: usize,
    pub json_value: serde_json::Value,
    pub raw_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DocumentBatch(Vec<Document>);

impl DocumentBatch {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn add_document(&mut self, document: Document) {
        self.0.push(document);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}




