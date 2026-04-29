pub use crate::protobuf::quartzdb::*;
use crate::quartzdb::field_value::Kind;

pub const SERVICE_DESCRIPTOR: &[u8] = include_bytes!("protobuf/services_descriptor.bin");

impl FieldValue {
    pub fn null() -> Self {
        Self { kind: None }
    }

    pub fn uint(v: u64) -> Self {
        Self {
            kind: Some(Kind::UintVal(v)),
        }
    }

    pub fn int(v: i64) -> Self {
        Self {
            kind: Some(Kind::IntVal(v)),
        }
    }

    pub fn float(v: f64) -> Self {
        Self {
            kind: Some(Kind::FloatVal(v)),
        }
    }

    pub fn string(v: String) -> Self {
        Self {
            kind: Some(Kind::StringVal(v)),
        }
    }

    pub fn bool(v: bool) -> Self {
        Self {
            kind: Some(Kind::BoolVal(v)),
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self.kind {
            Some(Kind::UintVal(v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self.kind {
            Some(Kind::IntVal(v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self.kind {
            Some(Kind::FloatVal(v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match &self.kind {
            Some(Kind::StringVal(v)) => Some(v.clone()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self.kind {
            Some(Kind::BoolVal(v)) => Some(v),
            _ => None,
        }
    }
}

impl ProtoDocument {
    pub fn new(
        timestamp: i64,
        source: String,
        values: Vec<FieldValue>,
        labels: String,
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
}

impl ProtoDocumentBatch {
    pub fn new(batch_id: String) -> Self {
        Self {
            id: batch_id,
            documents: vec![],
        }
    }

    pub fn with_capacity(batch_id: String, capacity: usize) -> Self {
        Self {
            id: batch_id,
            documents: Vec::with_capacity(capacity),
        }
    }

    pub fn add_document(&mut self, proto_document: ProtoDocument) {
        self.documents.push(proto_document);
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn sort(&mut self) {
        self.documents.sort_by_key(|doc| doc.timestamp);
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
