use std::{any::Any, sync::Arc};

use datafusion::{arrow::datatypes::{Schema, SchemaRef}, execution::{SendableRecordBatchStream, TaskContext}, physical_expr::EquivalenceProperties, physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, execution_plan::{Boundedness, EmissionType}, stream::RecordBatchStreamAdapter}};
use datafusion::common::{Result, };
use futures::stream::TryStreamExt;

use crate::search::{context::TableSearchContext, split_searcher::SplitSearcher};

#[derive(Debug)]
pub struct SplitSearchExec {
    context: Arc<TableSearchContext>,
    /// table base schema (latest config)
    schema: Arc<Schema>,
    /// output schema (derived from projection)
    projected_schema: Arc<Schema>,
    split_id: String,
    projection: Option<Vec<usize>>,
    /// Full-Text-Search experession is the tantivy query 
    /// extracted from SQL.
    /// example:
    /// `SELECT* from qtz_search(logs, 'description:*ali') 
    ///     WHERE severity = 'error'
    /// ` -> description:*ali
    fts_expr: Option<String>, 
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl SplitSearchExec {
    pub fn new(
        context: Arc<TableSearchContext>,
        schema: Arc<Schema>, 
        split_id: String,
        projection: Option<Vec<usize>>,
        fts_expr: Option<String>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = Arc::new(schema
            .project(&projection.clone().unwrap_or(vec![])).unwrap());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Self { context, schema, projected_schema, split_id, projection, fts_expr, limit, properties }
    }

    pub fn get_context(&self) -> &Arc<TableSearchContext> {
        &self.context
    }

    pub fn get_split_id(&self) -> &str {
        &self.split_id
    }

    pub fn get_projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    pub fn get_fts_expr(&self) -> &Option<String> {
        &self.fts_expr
    }

    pub fn get_limit(&self) -> &Option<usize> {
        &self.limit
    }

}

impl ExecutionPlan for SplitSearchExec {
    fn name(&self) -> &str { "SplitSearchExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.projected_schema.clone() }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // This method runs locally on your workers/executors
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let context = self.context.clone();
        let split_id = self.split_id.clone();
        let projection = self.projection.clone();
        let fts_expr = self.fts_expr.clone();
        let limit = self.limit.clone();
        
        let base_schema = self.schema.clone();
        let future_stream = async move {
            SplitSearcher::search(context, base_schema, split_id, projection, fts_expr, limit).await
        };
        let inner_stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.projected_schema.clone(), inner_stream)))
    }
}

impl DisplayAs for SplitSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SplitSearchExec: split={}", self.split_id)
    }
}
