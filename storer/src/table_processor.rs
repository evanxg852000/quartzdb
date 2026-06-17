use std::sync::Arc;

use anyhow::Result;

use common::catalog::{SplitMeta, TableMeta};
use datafusion::arrow::{array::RecordBatch, compute::concat_batches, util::pretty::print_batches};
use datafusion_distributed::display_plan_ascii;
use metastore::{client::MetastoreClient, service::MetastoreService};
use storage::Storage;
use futures::TryStreamExt;

use crate::{document::StorerBatch, search::{context::SearchContext, coordinator::SearchCoordinator, tags_filter::SearchTagsFilterCache, worker_manager::{SearchWorkerManager, SearchWorkerResolver}}, split::writter::SplitWriter};

// The number of workers cooperating to execute a query
const NUM_QUERY_EXECUTOR: usize = 3;

#[derive(Debug, Clone)]
pub struct StorerContext {
    table_meta: Arc<TableMeta>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
    worker_manager: Arc<SearchWorkerManager>,
    tags_filter_cache: Arc<SearchTagsFilterCache>,
}

impl StorerContext {
    pub async fn try_new(
        table_meta: Arc<TableMeta>,
        storage: Arc<dyn Storage>,
        metastore_client: MetastoreClient,
        worker_manager: Arc<SearchWorkerManager>,
        tags_filter_cache: Arc<SearchTagsFilterCache>,
    ) -> Result<Self> {
        Ok(Self {
            table_meta,
            storage,
            metastore_client,
            worker_manager,
            tags_filter_cache,
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
        //self.context.metastore_client.list_splits(&table_name, start_timestamp, end_timestamp)
        let splits: Vec<SplitMeta> = vec![
            create_split("019ec073-3a36-7ce2-aad6-2a8b9721b4cb", &table_name),
            create_split("019ec073-1543-7d32-884d-1f56019ca413", &table_name),
        ];

        let search_context = Arc::new(SearchContext::new(
            self.context.storage.clone(),
            self.context.tags_filter_cache.clone(),
        ));
        let schema = common::schema::Schema::get_primary_schema(&self.context.table_meta.config);
        let execution_context = SearchCoordinator::create_distributed_execution_context(
            self.context.table_meta.clone(),
            search_context,
            worker_resolver,
            splits,
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


fn create_split(id: &str, table_name: &str) -> SplitMeta {
    SplitMeta { 
        split_id: id.to_string(), 
        table_name: table_name.to_string(), 
        min_timestamp: 10, max_timestamp: 20 
    }
}
