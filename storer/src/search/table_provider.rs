use std::{any::Any, sync::Arc};

use common::catalog::SplitMeta;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::union::UnionExec;
use datafusion::{arrow::datatypes::SchemaRef, catalog::TableProvider, datasource::TableType, logical_expr::Expr, physical_plan::ExecutionPlan};
use datafusion::common::Result;
use tonic::async_trait;

use crate::search::context::TableSearchContext;
use crate::search::execution_plan::SplitSearchExec;


#[derive(Debug)]
pub struct SplitSearchTableProvider {
    context: Arc<TableSearchContext>,
    schema: Arc<Schema>,
    splits: Vec<SplitMeta>,
    fts_expr: Option<String>,
}

impl SplitSearchTableProvider {
    pub fn new(
        context: Arc<TableSearchContext>,
        splits: Vec<SplitMeta>,
        fts_expr: Option<String>,
    ) -> Self {
        let schema = context.get_table_primary_schema();
        Self { 
            context,
            schema,
            splits, 
            fts_expr,
        }
    }

    /// Prune split based on tags and filter exprs
    fn prune_splits(&self, _filters: &[Expr]) -> Vec<SplitMeta> {
        let active_splits = self.splits.clone();
        //TODO: 
        // - prune based on time_range filter
        // - prune based on tag

        // for filter in filters {
        //     // Basic example: look for "split_id = X" in the SQL WHERE clause
        //     if let Expr::BinaryExpr(binary_expr) = filter {
        //         if binary_expr.op == Operator::Eq {
        //             let left_is_col = matches!(*binary_expr.left, Expr::Column(ref c) if c.name == "split_id");
        //             if left_is_col {
        //                 if let Expr::Literal(ScalarValue::Int32(Some(target_id))) = *binary_expr.right {
        //                     // Keep only the split that matches the requested ID
        //                     active_splits.retain(|s| s.split_id == target_id);
        //                 }
        //             }
        //         }
        //     }
        // }
        active_splits
    }
}

#[async_trait]
impl TableProvider for SplitSearchTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }
    
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
         &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_projection = projection
            .map(|indices| indices
                .iter()
                .map(|i|*i as u64)
                .collect::<Vec<_>>()
            ).unwrap_or_default();
        let split_plans = self
            .prune_splits(filters)
            .into_iter()
            .map(|split| Arc::new(SplitSearchExec::new(
                self.context.clone(),
                self.schema.clone(), 
                split.split_id, 
                plan_projection.clone(), 
                self.fts_expr.clone()
            )) as Arc<dyn ExecutionPlan>)
            .collect::<Vec<_>>();
        Ok(UnionExec::try_new(split_plans)?)
    }
}
