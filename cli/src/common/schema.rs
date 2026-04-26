use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::common::arrow::datatypes::{self as datafusion_schema};
use jiff::Timestamp;
use json_dotpath::DotPaths;
use proto::quartzdb::FieldValue;
use serde_json::Value as JsonValue;
use tantivy::schema::{self as tantivy_schema};

use crate::common::index::FieldConfig;
use crate::common::{
    document::Document,
    index::{FieldType, IndexConfig},
};

pub const QUARTZDB_ROW_INDEX_FIELD_NAME: &'static str = "__qtz_index";
pub const QUARTZDB_LABELS_FIELD_NAME: &'static str = "__qtz_labels";
pub const QUARTZDB_TIMESTAMP_FIELD_NAME: &'static str = "__qtz_timestamp";
pub const QUARTZDB_SOURCE_FIELD_NAME: &'static str = "__qtz_source";

//TODO: Future
// add support for lance format
// https://docs.rs/lance/latest/lance/

pub struct Schema {}

impl Schema {
    pub fn get_primary_schema(index_config: &IndexConfig) -> Arc<datafusion_schema::Schema> {
        let capacity = index_config.fields.len() + 3;
        let mut fields = Vec::with_capacity(capacity);
        fields.push(datafusion_schema::Field::new(
            QUARTZDB_TIMESTAMP_FIELD_NAME,
            datafusion_schema::DataType::Timestamp(
                datafusion_schema::TimeUnit::Nanosecond,
                Some("UTC".into()),
            ),
            false,
        ));
        fields.push(datafusion_schema::Field::new(
            QUARTZDB_SOURCE_FIELD_NAME,
            datafusion_schema::DataType::LargeUtf8,
            false,
        ));

        for field in index_config.fields.iter() {
            let arrow_type = match field.field_type {
                FieldType::Uint => datafusion_schema::DataType::UInt64,
                FieldType::Int => datafusion_schema::DataType::Int64,
                FieldType::Float => datafusion_schema::DataType::Float64,
                FieldType::Bool => datafusion_schema::DataType::Boolean,
                FieldType::String => datafusion_schema::DataType::Utf8,
            };
            let arrow_field = datafusion_schema::Field::new(field.name.as_str(), arrow_type, true);
            fields.push(arrow_field);
        }
        Arc::new(datafusion_schema::Schema::new(fields))
    }

    pub fn get_fts_schema() -> tantivy_schema::Schema {
        let mut schema_builder = tantivy_schema::Schema::builder();

        // row_id field
        schema_builder.add_u64_field(QUARTZDB_ROW_INDEX_FIELD_NAME, tantivy_schema::INDEXED | tantivy_schema::FAST);

        // row fts object field
        let json_options = tantivy_schema::JsonObjectOptions::default()
            .set_expand_dots_enabled()
            .set_indexing_options(tantivy_schema::TEXT.get_indexing_options().unwrap().clone());
        schema_builder.add_json_field(QUARTZDB_LABELS_FIELD_NAME, json_options);
        schema_builder.build()
    }

    pub fn extract_timestamp(index_config: &IndexConfig, document: &Document) -> Result<i64> {
        let ts_nanoseconds = document
            .json_object
            .dot_get::<JsonValue>(index_config.timestamp.as_str())?
            .ok_or_else(|| anyhow!("Timestamp field must be defined"))
            .and_then(|value| {
                let ts_nanoseconds = match value {
                    JsonValue::Number(n) => {
                        n.as_i64().ok_or_else(|| anyhow!("Timestamp parse error"))?
                    }
                    JsonValue::String(date_str) => {
                        let timestamp = date_str.parse::<Timestamp>().map_err(|err| {
                            anyhow!("Timestamp cannot be parsed: {}", err.to_string())
                        })?;
                        i64::try_from(timestamp.as_nanosecond()).map_err(|err| {
                            anyhow!("Timestamp cannot be interpreted: {}", err.to_string())
                        })?
                    }
                    _ => {
                        return Err(anyhow!(
                            "Timestamp field is of wrong type, expected:  Number or String"
                        ));
                    }
                };
                Ok(ts_nanoseconds)
            })?;
        Ok(ts_nanoseconds)
    }

    pub fn extract_field_values(
        index_config: &IndexConfig,
        document: &Document,
    ) -> Result<Vec<FieldValue>> {
        let mut values = Vec::new();
        for field in index_config.fields.iter() {
            let field_value = extract_field_value_from_json_value(&document.json_object, &field)?;
            values.push(field_value);
        }
        Ok(values)
    }

    pub fn extract_label_values_as_object(
        index_config: &IndexConfig,
        document: &Document,
    ) -> Result<JsonValue> {
        if index_config.labels.len() == 0 {
            return Ok(document.json_object.clone());
        }

        let mut object = serde_json::json!({});
        for label in index_config.labels.iter() {
            let value = match document.json_object.dot_get::<JsonValue>(label.as_str())? {
                Some(v) => v,
                None => continue,
            };

            let segments = label.segments();
            let mut current_object = &mut object;
            for (i, segment) in segments.iter().enumerate() {
                if i == segments.len() - 1 {
                    // Last segment - set the value
                    current_object[segment] = value;
                    break;
                }
                // Intermediate segment - navigate or create
                if !current_object[segment].is_object() {
                    current_object[segment] = serde_json::json!({});
                }
                current_object = &mut current_object[segment];
            }
        }
        Ok(object)
    }

    pub fn extract_tag_values(
        index_config: &IndexConfig,
        document: &Document,
    ) -> Result<Vec<String>> {
        let mut tags = Vec::with_capacity(index_config.tags.len());
        for tag in index_config.tags.iter() {
            if let Some(value) = document.json_object.dot_get::<JsonValue>(tag.as_str())? {
                let string_value = match value {
                    JsonValue::Null => continue,
                    JsonValue::String(s) => s,
                    v => v.to_string(),
                };
                if !string_value.is_empty() {
                    tags.push(string_value);
                }
            }
        }
        Ok(tags)
    }
}

fn extract_field_value_from_json_value(
    value: &JsonValue,
    field: &FieldConfig,
) -> Result<FieldValue> {
    let json_val: JsonValue = value
        .dot_get(field.name.as_str())?
        .unwrap_or_else(|| JsonValue::Null);

    if matches!(json_val, JsonValue::Null) {
        return Ok(FieldValue::null());
    }

    let scalar_value = match (&field.field_type, json_val) {
        (FieldType::String, JsonValue::String(value)) => FieldValue::string(value),
        (FieldType::Int, JsonValue::Number(value)) => FieldValue::int(value.as_i64().unwrap()),
        (FieldType::Float, JsonValue::Number(value)) => FieldValue::float(value.as_f64().unwrap()),
        (FieldType::Bool, JsonValue::Bool(value)) => FieldValue::bool(value),
        (expected_t, v) => {
            return Err(anyhow::anyhow!(
                "Field mishmacth: expected `{:?}`, but found `{:?}`",
                expected_t,
                v
            ));
        }
    };
    Ok(scalar_value)
}
