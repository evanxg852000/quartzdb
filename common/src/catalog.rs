use hrw_hash::HrwNode;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FieldName(String); //JSON Path that escape dot if needed

impl FieldName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn is_valid(&self) -> bool {
        false
    }

    pub fn segments(&self) -> Vec<&str> {
        let mut segments = Vec::new();
        let mut start = 0;
        let mut i = 0;
        let bytes = self.0.as_bytes();

        while i < bytes.len() {
            if bytes[i] == b'\\' && i + 1 < bytes.len() && bytes[i + 1] == b'.' {
                // Skip escaped dot
                i += 2;
            } else if bytes[i] == b'.' {
                // Found unescaped dot - create segment
                segments.push(&self.0[start..i]);
                start = i + 1;
                i += 1;
            } else {
                i += 1;
            }
        }

        // Add the last segment
        if start <= self.0.len() {
            segments.push(&self.0[start..]);
        }

        segments
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Uint,
    Int,
    Float,
    Bool,
    String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum FieldValue {
    Null,
    Uint(u64),
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl FieldValue {
    pub fn null() -> Self {
        Self::Null
    }

    pub fn uint(v: u64) -> Self {
        Self::Uint(v)
    }

    pub fn int(v: i64) -> Self {
        Self::Int(v)
    }

    pub fn float(v: f64) -> Self {
        Self::Float(v)
    }

    pub fn bool(v: bool) -> Self {
        Self::Bool(v)
    }

    pub fn string(v: String) -> Self {
        Self::String(v)
    }

    pub fn as_u64(&self) -> Option<u64> {
        match &self {
            FieldValue::Uint(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match &self {
            FieldValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match &self {
            FieldValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match &self {
            FieldValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match &self {
            FieldValue::String(v) => Some(v.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FieldConfig {
    pub name: FieldName,
    #[serde(rename = "type")]
    pub field_type: FieldType,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TableConfig {
    pub timestamp: FieldName,
    pub labels: Vec<FieldName>,
    pub tags: Vec<FieldName>,
    pub fields: Vec<FieldConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StorageSettings {
    pub url: Url,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngesterSettings {
    pub batch_size: u64,
    pub commit_timeout_secs: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SearcherSettings {
    pub max_results: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RetentionSettings {
    pub period: String,
    pub schedule: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TableSettings {
    pub storage: Option<StorageSettings>,
    pub ingester: IngesterSettings,
    pub searcher: SearcherSettings,
    pub retention: Option<RetentionSettings>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMeta {
    pub name: String,
    pub config: TableConfig,
    pub settings: TableSettings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitMeta {
    pub split_id: String,
    pub table_name: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub capacity: usize
}

impl NodeInfo {
    pub fn new(id: String, address: String) -> Self {
        Self::with_capacity(id, address, 1)
    }

    pub fn with_capacity(id: String, address: String, capacity: usize) -> Self {
        Self { id, address, capacity }
    }
}

impl HrwNode for NodeInfo {
    fn capacity(&self) -> usize {
        self.capacity
    }
}
