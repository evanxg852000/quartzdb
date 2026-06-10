use std::{any::Any, sync::Arc};

use datafusion::{arrow::datatypes::SchemaRef, execution::{SendableRecordBatchStream, TaskContext}, physical_expr::EquivalenceProperties, physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, execution_plan::{Boundedness, EmissionType}, stream::RecordBatchStreamAdapter}};
use datafusion::common::{Result, };
use storage::Storage;
use futures::stream::TryStreamExt;

use crate::search::split_searcher::SplitSearcher;

#[derive(Debug)]
pub struct SplitSearchExec {
    table_name: String,
    storage: Arc<dyn Storage>,
    schema: SchemaRef, // latest table schema
    split_id: String,
    projection: Vec<u64>,
    /// Full-Text-Search experession is the tantivy query 
    /// extracted from SQL.
    /// example:
    /// `SELECT* from products 
    ///     WHERE quartzdb.search("description:*ali")
    /// ` -> description:*ali
    fts_expr: Option<String>, 
    properties: Arc<PlanProperties>,
}

impl SplitSearchExec {
    pub fn new(
        table_name: String,
        storage: Arc<dyn Storage>,
        schema: SchemaRef, 
        split_id: String,
        projection: Vec<u64>,
        fts_expr: Option<String>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Self { table_name, storage, schema, split_id, projection, fts_expr,  properties }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub fn get_split_id(&self) -> &str {
        &self.split_id
    }

    pub fn get_projection(&self) -> &Vec<u64> {
        &self.projection
    }

    pub fn get_fts_expr(&self) -> &Option<String> {
        &self.fts_expr
    }
}

impl ExecutionPlan for SplitSearchExec {
    fn name(&self) -> &str { "SplitSearchExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // This method runs locally on your workers/executors
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let storage = self.storage.clone();
        let schema = self.schema.clone();
        let split_id = self.split_id.clone();
        let projection = self.projection.clone();
        let fts_expr = self.fts_expr.clone();
        
        let moved_schema = schema.clone();
        let future_stream = async move {
            SplitSearcher::search(storage, table_name, moved_schema, split_id, projection, fts_expr).await
        };
        let inner_stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, inner_stream)))
    }
}

impl DisplayAs for SplitSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SplitSearchExec: split={}", self.split_id)
    }
}
