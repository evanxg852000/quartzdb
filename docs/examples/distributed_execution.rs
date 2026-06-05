use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile};
use datafusion::prelude::*;
use datafusion_distributed::worker_resolver::WorkerResolver;
use std::collections::HashMap;
use std::sync::Arc;

// 1. THE RESOLVER: Maps file paths to worker network addresses
#[derive(Clone)]
pub struct LocalShardResolver {
    pub mapping: HashMap<String, String>,
}

#[async_trait]
impl WorkerResolver for LocalShardResolver {
    async fn resolve(&self, file: &PartitionedFile) -> String {
        // choose the node responsive for this split (rendez-vous hash)
        
        // ensure this node has the split or downloads it

        // return the node url

        let path = file.path().to_string();
        // Check which worker prefix the file path matches
        self.mapping.iter()
            .find(|(id, _)| path.contains(*id))
            .map(|(_, addr)| addr.clone())
            .unwrap_or_else(|| "127.0.0.1:50050".to_string()) // Fallback
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // 2. CONFIGURE WORKER TOPOLOGY
    let mut worker_ips = HashMap::new();
    worker_ips.insert("worker-01".to_string(), "10.0.0.1:50050".to_string());
    worker_ips.insert("worker-02".to_string(), "10.0.0.2:50050".to_string());

    let resolver = LocalShardResolver { mapping: worker_ips };

    // 3. DEFINE UNIFIED SCHEMA (Solves the mismatched "size" column)
    // Files missing "size" will produce NULLs because the field is nullable
    let unified_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("size", DataType::Int64, true), // The mismatched column
    ]));

    // 4. EXPLICIT FILE LISTING (list all necessary splits with prunning)
    // We tell the scheduler exactly where the shards are, even though they are on different nodes
    let shards = vec![
        // Shard on Worker 1 (has size column)
        PartitionedFile::new("worker-storage://shard-01".to_string(), 100), 
        // Shard on Worker 2 (missing size column)
        PartitionedFile::new("worker-storage://shard-02".to_string(), 100),
    ];

    // 5. SETUP LISTING TABLE
    let table_url = ListingTableUrl::parse("worker-storage://")?;
    let mut config = ListingTableConfig::new(table_url)
        .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())))
        .with_schema(unified_schema);
    
    // Manually assign the files so the scheduler doesn't try to scan its local disk
    config.partitions = vec![shards];

    let provider = Arc::new(ListingTable::try_new(config)?);

    // 6. INITIALIZE DISTRIBUTED CONTEXT
    // This is where you'd use datafusion-distributed specific session state
    let ctx = SessionContext::new(); // In practice, use SessionState with the resolver
    ctx.register_table("sharded_table", provider)?;

    // 7. EXECUTE
    // The scheduler will use the Resolver to send the task for 'shard_a' to 10.0.0.1
    // and 'shard_b' to 10.0.0.2.
    let df = ctx.sql("SELECT id, name, size FROM sharded_table WHERE id > 10").await?;
    df.show().await?;

    Ok(())
}




use object_store::{ObjectStore, path::Path, GetResult};
use async_trait::async_trait;

#[derive(Debug)]
pub struct WorkerLocalStorage {
    // A simple map of "Logical Name" -> "Local Actual Path"
    pub local_manifest: HashMap<String, String>,
}

#[async_trait]
impl ObjectStore for WorkerLocalStorage {
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        // 'location' will be something like "shard-101"
        let local_path = self.local_manifest.get(location.as_ref())
            .ok_or_else(|| object_store::Error::NotFound { 
                path: location.to_string(), 
                source: "Shard not found on this worker".into() 
            })?;
            
        // Open the ACTUAL local file
        let file = tokio::fs::File::open(local_path).await?;
        Ok(GetResult::File(file, local_path.into()))
    }
    // ... implement other required methods (head, list, etc.) by delegating to LocalFileSystem
}
