use std::sync::Arc;
use common::catalog::SplitMeta;
use datafusion::catalog::TableFunctionImpl;
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::scalar::ScalarValue;

use datafusion::error::{DataFusionError, Result};
use datafusion::datasource::TableProvider;

use crate::search::context::TableSearchContext;
use crate::search::table_provider::SplitSearchTableProvider;

pub const QUARTZDB_SEARCH_FUNCTION_NAME: &str = "qtz_search";

pub fn quartzdb_udf_functions() -> Vec<Arc<ScalarUDF>> {
    vec![]
}

#[derive(Debug)]
pub struct SplitSearchTableFunction {
    context: Arc<TableSearchContext>,
    splits: Vec<SplitMeta>,
}

impl SplitSearchTableFunction {
    pub fn new(
        context: Arc<TableSearchContext>,
        splits: Vec<SplitMeta>,
    ) -> Self {
        Self { context, splits }
    }
}

impl TableFunctionImpl for SplitSearchTableFunction {
    // args[0] = table name ('logs')
    // optional args[1] = fts_expr query ('foo:*ali')
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // get table name & validate
        let table_name = match args.get(0) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(val)), _)) => val.to_string(),
            Some(Expr::Column(col)) => col.name.clone(),
            _ => return Err(DataFusionError::Plan("Invalid table name".to_string())),
        };
        let context_table_name = self.context.get_table_meta().name.clone();
        if table_name != context_table_name {
            return Err(DataFusionError::Plan(format!("Invalid table name: expected `{}`, got {}", context_table_name, table_name)));
        }

        let fts_expr = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(val)), _)) => {
                if val.is_empty() { 
                    None 
                } else { 
                    Some(val.to_string()) 
                }
            },
            Some(Expr::Literal(ScalarValue::Null, _)) => None, // null 
            None => None, // omitted
            _ => return Err(DataFusionError::Plan("Invalid search query".to_string())),
        };

        let table_provider = SplitSearchTableProvider::new(
            self.context.clone(),
            self.splits.clone(),
            fts_expr,
        );
        Ok(Arc::new(table_provider) as Arc<dyn TableProvider>)
    }
}
