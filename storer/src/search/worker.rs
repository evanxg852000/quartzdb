use datafusion::{error::DataFusionError, execution::{SessionState, SessionStateBuilder, context::SessionContext}};
use datafusion_distributed::{Worker, WorkerQueryContext};

pub struct SearchWorkerBuilder{}

impl SearchWorkerBuilder {
    pub fn build() -> Worker {
        Worker::from_session_builder(Self::build_state)
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_scalar_functions(vec![])
            .build())
    }
}
