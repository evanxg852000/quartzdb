use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::{SessionState}};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, Worker, WorkerQueryContext, WorkerSessionBuilder};
use storage::Storage;
use tonic::async_trait;

use crate::search::{context::SearchContext, execution_codec::SplitSearchExecCodec, funtions::quartzdb_udf_functions, tags_filter::SearchTagsFilterCache, task_estimator::SplitSearchTaskEstimator};


pub struct SearchWorkerBuilder {
    context: Arc<SearchContext>,
}

impl SearchWorkerBuilder {
    pub fn new(context: Arc<SearchContext>) -> Self {
        Self { context }
    }

    pub fn build(
        storage: Arc<dyn Storage>,
        tags_filter_cache: Arc<SearchTagsFilterCache>,
    ) -> Worker {
        let context = Arc::new(SearchContext::new(storage, tags_filter_cache));
        let session_builder = Self::new(context);
        Worker::from_session_builder(session_builder)
    }
}

#[async_trait]
impl WorkerSessionBuilder for SearchWorkerBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_default_features()
            .with_scalar_functions(quartzdb_udf_functions())
            .with_distributed_planner()
            .with_distributed_user_codec(SplitSearchExecCodec::new(self.context.clone()))
            .with_distributed_task_estimator(SplitSearchTaskEstimator{})
            .build())
    }
}
