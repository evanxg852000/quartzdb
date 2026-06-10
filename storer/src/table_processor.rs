use std::sync::Arc;

use anyhow::Result;

use common::catalog::TableMeta;
use datafusion::arrow::{array::RecordBatch, compute::concat_batches, util::pretty::print_batches};
use datafusion_distributed::display_plan_ascii;
use metastore::{client::MetastoreClient, service::MetastoreService};
use storage::Storage;
use futures::TryStreamExt;

use crate::{document::StorerBatch, search::{coordinator::SearchCoordinator, worker_manager::{SearchWorkerManager, SearchWorkerResolver}}, split::writter::SplitWriter};

// The number of workers cooperating to execute a query
const NUM_QUERY_EXECUTOR: usize = 3;

#[derive(Debug, Clone)]
pub struct StorerContext {
    table_meta: Arc<TableMeta>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    worker_manager: Arc<SearchWorkerManager>,
}

impl StorerContext {
    pub async fn try_new(
        table_meta: Arc<TableMeta>,
        storage: Arc<dyn Storage>,
        metastore_client: MetastoreClient,
    ) -> Result<Self> {
        let mut table_storage = storage;
        if let Some(index_storage_settings) = &table_meta.settings.storage {
            table_storage = table_storage
                .derive_remote(&index_storage_settings.url)
                .await?;
        }
        let worker_manager = Arc::new(SearchWorkerManager::try_new(metastore_client.clone())?);
        Ok(Self {
            table_meta,
            storage: table_storage,
            metastore_client,
            worker_manager,
        })
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_meta.name
    }
}

#[derive(Debug)]
pub struct TableProcessor {
    context: Arc<StorerContext>,
}

impl TableProcessor {
    pub fn new(context: Arc<StorerContext>) -> Self {
        Self { context }
    }

    pub fn get_context(&self) -> &StorerContext {
        &self.context
    }

    pub async fn put(&self, record_batch: RecordBatch) -> Result<()> {
        let context = self.context.clone();
        let storage = context.storage.clone();
        let table_name = context.table_meta.name.clone();
        let table_config = &context.table_meta.config;

        // build sorted batch
        let mut storer_batch = StorerBatch::try_from_record_batch(table_config, record_batch)?;
        storer_batch.sort();

        // build split & upload it
        let mut split_writer = SplitWriter::try_new(table_name, storage.clone()).await?;
        split_writer.write(table_config, storer_batch).await?;
        let split_meta = split_writer.finalize().await?;

        // publish it
        context.metastore_client.put_split(split_meta).await?;
        Ok(())
    }

    /// Steps:
    /// 1. indentify splits with (timestamp & tags) prunning if possible
    /// 2. fetch available nodes
    /// 3. rendez-vous hash nodes with split to indentify worker nodes
    /// 4. create a resolver based on the indentified worker nodes 
    /// 5. Create distributed execution session context
    /// 6. Execute the query
    pub async fn search(&self, query: &str) -> Result<RecordBatch> {
        let table_name = self.context.table_meta.name.clone();
        let worker_resolver = SearchWorkerResolver::try_for_table(
            table_name.clone(),
            NUM_QUERY_EXECUTOR, // can be a param
            self.context.worker_manager.clone(),
        ).await?;

        //TODO: fetch matching splits using metastore_client
        let split_ids: Vec<String> = vec![
            "019e97de-ca9a-79b1-8675-f9aeee6d4364".into(),
            "019e97f7-8459-7fa3-bc13-aff547844078".into(),
        ];

        let schema = common::schema::Schema::get_primary_schema(&self.context.table_meta.config);
        let execution_context = SearchCoordinator::create_distributed_execution_context(
            schema.clone(),
            split_ids,
            self.context.storage.clone(),
            worker_resolver,
        )?;

        // debug logical-plan
        // {
        //     println!("LOGICAL PLAN");
        //     let plan = execution_context.state().create_logical_plan(&query).await?;
        //     println!("{}", plan.display_indent_schema());
        // }

        // debug physical-plan
        // {
        //     let data_frame = execution_context.sql(&query).await?;
        //     println!("PHYSIACL PLAN");
        //     let plan = data_frame.create_physical_plan().await?;
        //     println!("{}", display_plan_ascii(plan.as_ref(), false));
        // }

        let data_frame = execution_context.sql(&query).await?;
        let stream = data_frame.execute_stream().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        
        // debug∂ record-batch
        // println!("RESULT TABLE");
        // print_batches(&batches).unwrap();

        let batch = match batches.len() {
            0 => RecordBatch::new_empty(schema),
            1 => batches.into_iter().next().unwrap(),
            _ => {
                let schema = batches[0].schema();
                concat_batches(&schema, &batches)?
            }  
        };
        Ok(batch)
    }
}
