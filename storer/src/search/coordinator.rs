use std::sync::Arc;

use anyhow::Result;
use common::catalog::{SplitMeta, TableMeta};
use datafusion::{execution::{SessionStateBuilder, context::SessionContext}};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt};

use crate::search::{ context::{SearchContext, TableSearchContext}, execution_codec::SplitSearchExecCodec, funtions::{QUARTZDB_SEARCH_FUNCTION_NAME, SplitSearchTableFunction, quartzdb_udf_functions}, task_estimator::SplitSearchTaskEstimator, worker_manager::SearchWorkerResolver};

pub struct SearchCoordinator{}

impl SearchCoordinator {
    pub fn create_distributed_execution_context(
        table_meta: Arc<TableMeta>,
        search_context: Arc<SearchContext>,
        worker_resolver: SearchWorkerResolver,
        splits: Vec<SplitMeta>,
    ) -> Result<SessionContext>  {
        
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_scalar_functions(quartzdb_udf_functions())
            .with_distributed_planner()
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_files_per_task(1)?
            .with_distributed_user_codec(SplitSearchExecCodec::new(search_context.clone()))
            .with_distributed_task_estimator(SplitSearchTaskEstimator{})
            .build();

        let ctx = SessionContext::from(state);
        let context = Arc::new(TableSearchContext::try_new(table_meta, search_context)?);
        let table_funtion = Arc::new(SplitSearchTableFunction::new(context, splits));
        ctx.register_udtf(QUARTZDB_SEARCH_FUNCTION_NAME, table_funtion);
        Ok(ctx)
    }
}
