use std::sync::Arc;

use anyhow::Result;

use common::catalog::TableMeta;
use datafusion::arrow::array::RecordBatch;
use metastore::{client::MetastoreClient, service::MetastoreService};
use storage::Storage;

use crate::{document::StorerBatch, split::writter::SplitWriter};

#[derive(Debug, Clone)]
pub struct StorerContext {
    table_meta: Arc<TableMeta>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
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
        Ok(Self {
            table_meta,
            storage: table_storage,
            metastore_client,
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

    pub async fn search(&self, query: &str) -> Result<RecordBatch> {
        // TODO:
        // // 1. Create your standard session context
        // let ctx = SessionContext::new();
    
        // // 2. Turn on distributed capabilities and pass your custom table router
        // let distributed_ctx = ctx
        //     .with_distributed_capabilities()?
        //     .with_worker_resolver(resolver);

        // // 3. Execute the query. 
        // // This automatically runs your WorkerResolver, splits the plan, 
        // // sends fragments to workers, and gathers the final results.
        // let df = distributed_ctx.sql(sql).await?;
        // let results = df.collect().await?;
        
        
        Ok(fixture::execute_query(query))
    }
}

mod fixture {
    use std::sync::Arc;
    use datafusion::arrow::{array::{BooleanArray, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
    
    pub fn execute_query(query: &str) -> RecordBatch {
        println!("executing query: {query}");

        // Define the arrays (columns)
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
        let active_array = BooleanArray::from(vec![true, false, true]);

        // Define the schema matching the columns
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);

        // Create the RecordBatch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        ).unwrap()
    }

}
