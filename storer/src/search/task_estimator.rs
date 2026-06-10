use std::sync::Arc;

use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use datafusion::common::Result;
use datafusion_distributed::{TaskEstimation, TaskEstimator, TaskRoutingContext};
use hrw_hash::HrwNodes;
use url::Url;

use crate::search::execution_plan::SplitSearchExec;

/// TaskEstimator that tells the planner how to distribute NumbersExec.
#[derive(Debug)]
pub struct SplitSearchTaskEstimator;

impl TaskEstimator for SplitSearchTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<TaskEstimation> {
        plan.as_any().downcast_ref::<SplitSearchExec>()?;
        Some(TaskEstimation::desired(1))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        plan.as_any().downcast_ref::<SplitSearchExec>()?;
        Some(plan.clone())
    }

    fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>> {
        match routing_ctx.plan.as_any().downcast_ref::<SplitSearchExec>() {
            Some(exec_plan) => {
                let split_id = exec_plan.get_split_id();
                if routing_ctx.task_count > 1 {
                    println!("Task count: {}", routing_ctx.task_count);
                }

                let urls = routing_ctx.available_urls
                    .iter()
                    .map(|url| url.to_string())
                    .collect::<Vec<_>>();
                let urls_cluster = HrwNodes::new(urls);
                let chosen_urls = urls_cluster
                    .sorted(&split_id)
                    .take(routing_ctx.task_count)
                    .collect::<Vec<_>>();
                let chosen_urls = chosen_urls.into_iter()
                    .map(|url| Url::parse(url))
                    .collect::<Result<Vec<_>,_>>()
                    .map_err(|err| DataFusionError::Internal(err.to_string()))?;
                Ok(Some(chosen_urls))
            },
            _ => Ok(None)
        }
    }
}
