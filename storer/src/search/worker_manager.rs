
use std::{sync::Arc, time::Duration};

use anyhow::Result;
use datafusion::error::DataFusionError;
use datafusion_distributed::WorkerResolver;
use hrw_hash::{HrwNode, HrwNodes};
use metastore::client::MetastoreClient;
use serde::{Deserialize, Serialize};
use tokio::{sync::RwLock, task::JoinHandle};
use tonic::async_trait;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerNode {
    pub id: String,
    pub address: String,
    pub capacity: usize
}

impl WorkerNode {
    pub fn new(id: String, address: String) -> Self {
        Self::with_capacity(id, address, 1)
    }

    pub fn with_capacity(id: String, address: String, capacity: usize) -> Self {
        WorkerNode { id, address, capacity }
    }
}

impl HrwNode for WorkerNode {
    fn capacity(&self) -> usize {
        self.capacity
    }
}


#[derive(Debug)]
pub struct SearchWorkerManager {
    workers: Arc<RwLock<Vec<WorkerNode>>>,
    join_handle: JoinHandle<Result<()>>,
}

impl SearchWorkerManager {
    pub fn try_new(metastore_client: MetastoreClient) -> Result<Self> {
        let workers = Arc::new(RwLock::new(vec![]));
        let join_handle = Self::start_background_worker_fetcher(metastore_client.clone(), workers.clone());
        Ok(Self {
            workers,
            join_handle,
        })
    }

    pub async fn get_available_workers(&self) ->  Result<Vec<WorkerNode>> {
        let workers = self.workers.read().await;
        Ok(workers
            .iter()
            .map(|node| node.clone())
            .collect())
    }

    fn start_background_worker_fetcher(
        metastore_client: MetastoreClient,
        workers: Arc<RwLock<Vec<WorkerNode>>>,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                //TODO: fetch from metastore
                _ = metastore_client;
                let mut workers_guard = workers.write().await;
                workers_guard.clear();
                workers_guard.push(WorkerNode::new("node-1".into(), "127.0.0.1:8081".into()));
                tokio::time::sleep(Duration::from_mins(10)).await;
            }
        })
    }
}


#[derive(Clone)]
pub struct SearchWorkerResolver {
    table_name: String,
    num_executor: usize,
    available_nodes: Vec<WorkerNode>,
}

impl SearchWorkerResolver {
    pub fn for_table(
        table_name: String,
        num_executor: usize,
        available_nodes: Vec<WorkerNode>,
    ) -> Self {
        Self {
            table_name,
            num_executor,
            available_nodes,
        }
    }
}

#[async_trait]
impl WorkerResolver for SearchWorkerResolver {
    /// pick the top(num_executor) eligible search nodes via rendez-vous hashing
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        let search_cluster = HrwNodes::new(self.available_nodes.clone());
        let chosen_nodes = search_cluster
            .sorted(&self.table_name)
            .take(self.num_executor)
            .collect::<Vec<_>>();
        let chosen_urls = chosen_nodes.into_iter()
            .map(|node| Url::parse(&format!("http://{}", node.address)))
            .collect::<Result<Vec<_>,_>>()
            .map_err(|err| DataFusionError::Internal(err.to_string()))?;
        Ok(chosen_urls)
    }
}



