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
}

impl ProtoDocument {
    pub fn new(
        id: u64,
        timestamp: i64,
        source: String,
        values: Vec<FieldValue>,
        labels: String,
        tags: Vec<String>,
    ) -> Self {
        Self {
            id,
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
            documents: vec![] 
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
}
