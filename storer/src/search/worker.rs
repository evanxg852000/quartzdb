use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::{SessionState}};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, Worker, WorkerQueryContext, WorkerSessionBuilder};
use storage::Storage;
use tonic::async_trait;

use crate::search::{execution_codec::SplitSearchExecCodec, funtions::quartzdb_udf_functions, task_estimator::SplitSearchTaskEstimator};


pub struct SearchWorkerBuilder {
    storage: Arc<dyn Storage>
}

impl SearchWorkerBuilder {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub fn build(storage: Arc<dyn Storage>) -> Worker {
        let session_builder = Self::new(storage);
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
            .with_distributed_user_codec(SplitSearchExecCodec::new(self.storage.clone()))
            .with_distributed_task_estimator(SplitSearchTaskEstimator{})
            .build())
    }
}
