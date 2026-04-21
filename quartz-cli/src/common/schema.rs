use std::collections::HashMap;
use std::sync::Arc;

use tantivy::schema::{self as tantivy_schema};
use datafusion::common::arrow::datatypes::{self as datafusion_schema};
use datafusion::common::ScalarValue;
use anyhow::{Ok, Result};
use json_dotpath::DotPaths;
use serde_json::{Value as JsonValue};


use crate::common::index::FieldConfig;
use crate::common::{document::Document, index::{self, FieldType, FieldValue, IndexConfig}};


const QUARTZDB_ID_FIELD_NAME: &'static str = "__qtz_id";
const QUARTZDB_VALUE_FIELD_NAME: &'static str = "__qtz_value";
const QUARTZDB_TIMESTAMP_FIELD_NAME: &'static str = "__qtz_timestamp";
const QUARTZDB_SOURCE_FIELD_NAME: &'static str = "__qtz_source";


pub struct Schema{}

impl Schema {
    pub fn get_primary_schema(index_config: &IndexConfig) -> datafusion_schema::Schema {
        let capacity = index_config.fields.len() + 3;
        let mut fields = Vec::with_capacity(capacity);
        fields.push(datafusion_schema::Field::new(QUARTZDB_ID_FIELD_NAME, datafusion_schema::DataType::UInt64, false));
        fields.push(datafusion_schema::Field::new(QUARTZDB_TIMESTAMP_FIELD_NAME, datafusion_schema::DataType::Timestamp(datafusion_schema::TimeUnit::Nanosecond, Some("UTC".into())), false));
        fields.push(datafusion_schema::Field::new(QUARTZDB_SOURCE_FIELD_NAME, datafusion_schema::DataType::LargeUtf8, false));

        for field in index_config.fields.iter() {
            let arrow_type = match field.field_type {
                FieldType::String => datafusion_schema::DataType::Utf8,
                FieldType::Int => datafusion_schema::DataType::Int64,
                FieldType::Float => datafusion_schema::DataType::Float64,
                FieldType::Bool => datafusion_schema::DataType::Boolean,
            };
            let arrow_field = datafusion_schema::Field::new(field.name.as_str(), arrow_type, true);
            fields.push(arrow_field);
        }
        datafusion_schema::Schema::new(fields)
    }

    pub fn get_fts_schema() -> tantivy_schema::Schema {
        let mut schema_builder = tantivy_schema::Schema::builder();
        
        // row_id field
        schema_builder.add_u64_field(QUARTZDB_ID_FIELD_NAME, tantivy_schema::INDEXED);
        
        // row fts object field
        let json_options = tantivy_schema::JsonObjectOptions::default()
            .set_expand_dots_enabled()
            .set_indexing_options(tantivy_schema::TEXT.get_indexing_options().unwrap().clone());
        schema_builder.add_json_field(QUARTZDB_VALUE_FIELD_NAME, json_options);
        schema_builder.build()
    }

    pub fn extract_primary_value(index_config: &IndexConfig, document: &Document) -> Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        // id
        let id: u64 = 0; // default id (we could have a config to extract)
        values.push(ScalarValue::UInt64(Some(id))); 
        
        // timestamp
        let timestamp_nanoseconds = document.json_value.dot_get::<JsonValue>(index_config.timestamp.as_str())?
            .ok_or_else(|| anyhow::anyhow!("Timestamp field must be defined"))
            .and_then(|value| {
                let nanoseconds = match value {
                    JsonValue::Number(n) => n.as_i64().unwrap(),
                    JsonValue::String(s) => 1, //TODO: convert to i64 via chrono datatime.
                    _ => return Err(anyhow::anyhow!("Timestamps field is of wrong type, expected:  Number or String"))
                };
                Ok(nanoseconds)
            })?;
        values.push(ScalarValue::TimestampNanosecond(Some(timestamp_nanoseconds), Some(Arc::from("UTC")))); 
        
        // source
        let source = document.json_value.to_string();
        values.push(ScalarValue::LargeUtf8(Some(source))); 

        // dynamic fields
        for field in index_config.fields.iter() {
            let scalar_value = extract_scalar_value_from_json_value(&document.json_value, &field)?;
            values.push(scalar_value);
        }

        Ok(values)
    }

    pub fn extract_fts_value(index_config: &IndexConfig, document: &Document) -> Result<JsonValue> {
        if index_config.labels.len() == 0 {
            return Ok(document.json_value.clone())
        }

        let object = serde_json::Map::new();
        for labels in index_config.labels.iter() {
            //TODO more complex algorithm
            // walk the object
        }
        Ok(JsonValue::Object(object))
    }

    pub fn extract_tag_values(index_config: &IndexConfig, document: &Document) -> Result<Vec<String>> {
        let mut tags = Vec::with_capacity(index_config.tags.len());
        for tag in index_config.tags.iter() {
            let value = document.json_value.dot_get::<JsonValue>(tag.as_str())?
                .ok_or_else(|| anyhow::anyhow!("Tag field must be defined"))
                .map(|value| {
                    match value {
                        JsonValue::Null => "".to_string(),
                        JsonValue::String(s) => s,
                        v => v.to_string(),
                    }
                })?;
            if !value.is_empty() {
                tags.push(value);
            }
        }
        Ok(tags)
    }
    
}


fn extract_scalar_value_from_json_value(value: &JsonValue, field: &FieldConfig) -> Result<ScalarValue> {
    let json_val: JsonValue = value.dot_get(field.name.as_str())?
        .unwrap_or_else(|| JsonValue::Null);

    if matches!(json_val, JsonValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let scalar_value = match (&field.field_type, json_val) {
        (FieldType::String, JsonValue::String(value)) => ScalarValue::LargeUtf8(Some(value)),
        (FieldType::Int, JsonValue::Number(value)) => ScalarValue::Int64(value.as_i64()),
        (FieldType::Float, JsonValue::Number(value)) => ScalarValue::Float64(value.as_f64()),
        (FieldType::Bool, JsonValue::Bool(value)) => ScalarValue::Boolean(Some(value)),
        (t1, t2) => return Err(anyhow::anyhow!("Field mishmacth: expected `{:?}`, but found `{:?}`", t1, t2)),
    };
    Ok(scalar_value)
}
