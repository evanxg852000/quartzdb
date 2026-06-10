use std::sync::Arc;
use datafusion::catalog::TableFunctionImpl;
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use datafusion::error::{DataFusionError, Result};
use datafusion::datasource::TableProvider;
use storage::Storage;

use crate::search::table_provider::SplitSearchTableProvider;

pub const QUARTZDB_SEARCH_FUNCTION_NAME: &str = "qtz_search";

pub fn quartzdb_udf_functions() -> Vec<Arc<ScalarUDF>> {
    vec![]
}

#[derive(Debug)]
pub struct SplitSearchTableTableFunction{
    storage: Arc<dyn Storage>,
    schema: SchemaRef,
    split_ids: Vec<String>,
}

impl SplitSearchTableTableFunction {
    pub fn new(storage: Arc<dyn Storage>, schema: SchemaRef, split_ids: Vec<String>) -> Self {
        Self { storage, schema, split_ids }
    }
}

impl TableFunctionImpl for SplitSearchTableTableFunction {
    // args[0] = table name ('logs')
    // optional args[1] = fts_expr query ('foo:*ali')
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table_name = match args.get(0) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(val)), _)) => val.to_string(),
            Some(Expr::Column(col)) => col.name.clone(),
            _ => return Err(DataFusionError::Plan("Invalid table name".to_string())),
        };
        let fts_expr = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(val)), _)) => Some(val.to_string()),
            Some(Expr::Literal(ScalarValue::Null, _)) => None, // null 
            None => None, // omitted
            _ => return Err(DataFusionError::Plan("Invalid search query".to_string())),
        };

        let table_provider = SplitSearchTableProvider::new(
            self.storage.clone(),
            table_name,
            self.schema.clone(),
            self.split_ids.clone(),
            fts_expr,
        );
        Ok(Arc::new(table_provider) as Arc<dyn TableProvider>)
    }
}
