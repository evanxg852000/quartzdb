use std::sync::Arc;

use anyhow::Result;
use datafusion::{arrow::datatypes::SchemaRef, execution::{SessionStateBuilder, context::SessionContext}};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt};
use storage::Storage;

use crate::search::{ execution_codec::SplitSearchExecCodec, funtions::{QUARTZDB_SEARCH_FUNCTION_NAME, SplitSearchTableTableFunction, quartzdb_udf_functions}, task_estimator::SplitSearchTaskEstimator, worker_manager::SearchWorkerResolver};

pub struct SearchCoordinator{}

impl SearchCoordinator {
    pub fn create_distributed_execution_context(
        schema: SchemaRef,
        split_ids: Vec<String>,
        storage: Arc<dyn Storage>,
        worker_resolver: SearchWorkerResolver,
    ) -> Result<SessionContext>  {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_scalar_functions(quartzdb_udf_functions())
            .with_distributed_planner()
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_files_per_task(1)?
            .with_distributed_user_codec(SplitSearchExecCodec::new(storage.clone()))
            .with_distributed_task_estimator(SplitSearchTaskEstimator{})
            .build();

        let ctx = SessionContext::from(state);
        let table_funtion = Arc::new(SplitSearchTableTableFunction::new(storage, schema, split_ids));
        ctx.register_udtf(QUARTZDB_SEARCH_FUNCTION_NAME, table_funtion);
        Ok(ctx)
    }
}
