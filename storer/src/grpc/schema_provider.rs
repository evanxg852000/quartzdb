use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap; // High-performance concurrent cache
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};

pub struct MetastoreSchemaProvider {
    // Shared gRPC metastore client clone
    metastore_client: MyMetastoreGrpcClient, 
    // In-memory cache to ensure you only hit the gRPC service once per table
    table_cache: DashMap<String, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for MetastoreSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any { self }

    // DataFusion asks your provider for a list of all tables
    fn table_names(&self) -> Vec<String> {
        // Option A: Block and ask gRPC metastore for the full dataset list
        // Option B: Return an empty vec if you want purely dynamic lookup
        todo!("Return dataset/table names")
    }

    // CRITICAL: DataFusion calls this lazily when parsing the query SQL!
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // 1. Check if we already built this table's schema in memory
        if let Some(cached_table) = self.table_cache.get(name) {
            return Ok(Some(cached_table.clone()));
        }

        // 2. Cache miss: Lazily query your external gRPC metastore service
        match self.metastore_client.fetch_dataset_metadata(name).await {
            Ok(metadata) => {
                // Construct your custom TableProvider using the fetched Arrow schema/URI
                let table_provider = Arc::new(MyDatasetTableProvider::new(metadata));
                
                // Cache it so subsequent queries are instant
                self.table_cache.insert(name.to_string(), table_provider.clone());
                
                Ok(Some(table_provider))
            }
            Err(_) => {
                // If it doesn't exist in the gRPC metastore, return None
                Ok(None) 
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_cache.contains_key(name)
    }
    
    // Stub these out unless your worker modifies the metastore schema via DDL
    fn register_table(&self, _name: String, _table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented("Read-only schema".to_string()))
    }
    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> { Ok(None) }
}
