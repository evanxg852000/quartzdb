use std::vec;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngestDocument {
    pub line: u64,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngestBatch(pub Vec<IngestDocument>);

impl IngestBatch {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn add_document(&mut self, source: String) {
        let next = self.0.len() as u64 + 1;
        self.0.push(IngestDocument { line: next, source });
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
