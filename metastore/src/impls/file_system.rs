/// A simple file system based metastore implementation. Useful for testing and
/// debugging during local development.
///  
/// It stores each table's metadata and splits in a separate directory under
/// the specified data directory.
/// For example, if the data directory is `/data`, and there is a table named
/// `users`, then the metadata and splits for the `users` table will be stored in
/// the `/data/metastore/users` directory. The metadata will be stored in a file
/// named `meta.json` in that directory.
///
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use common::catalog::{SplitMeta, TableMeta};
use hashbrown::HashMap;
use tokio::{fs, sync::Mutex};

use crate::{events::MetastoreEvent, service::MetastoreService};


// const METASTORE_DIR: &str = "metastore";
const TABLE_META_FILE: &str = "meta.json";

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct TableEntry {
    table: TableMeta,
    splits: Vec<SplitMeta>,
}

#[derive(Debug)]
pub struct FileSystemMetastoreServiceImpl {
    directory: PathBuf,
    entries: Arc<Mutex<HashMap<String, TableEntry>>>,
}

impl FileSystemMetastoreServiceImpl {
    pub async fn try_new(directory: &PathBuf) -> Result<Self> {
        // let directory = data_dir.join(METASTORE_DIR);
        // tokio::fs::create_dir_all(&directory).await?;
        let entries = Self::load_tables_from_file_system(&directory).await?;
        Ok(FileSystemMetastoreServiceImpl {
            directory: directory.clone(),
            entries: Arc::new(Mutex::new(entries)),
        })
    }

    pub async fn list_tables(&self) -> Result<Vec<TableMeta>> {
        let mut tables = Vec::new();
        let entries = self.entries.lock().await;
        for table_entry in entries.values() {
            tables.push(table_entry.table.clone());
        }
        Ok(tables)
    }

    pub async fn put_table(&self, table_meta: TableMeta) -> Result<()> {
        let mut entries = self.entries.lock().await;
        let table_entry = entries
            .entry(table_meta.name.clone())
            .or_insert(TableEntry {
                table: TableMeta::default(),
                splits: Vec::new(),
            });
        table_entry.table = table_meta;

        self.save_entry_to_file_system(&table_entry).await?;
        Ok(())
    }

    pub async fn get_table(&self, table_name: &str) -> Result<TableMeta> {
        let entries = self.entries.lock().await;
        let table_entry = entries
            .get(table_name)
            .ok_or_else(|| anyhow!("table '{}' does not exist", table_name))?;
        Ok(table_entry.table.clone())
    }

    pub async fn delete_table(&self, table_name: &str) -> Result<()> {
        let mut entries = self.entries.lock().await;
        if !entries.contains_key(table_name) {
            return Err(anyhow!("table '{}' does not exist", table_name));
        }
        entries.remove(table_name);
        let table_path = self.directory.join(table_name);
        fs::remove_dir_all(&table_path).await?;
        Ok(())
    }

    pub async fn put_split(&self, split_meta: SplitMeta) -> anyhow::Result<()> {
        let mut entries = self.entries.lock().await;
        let table_entry = entries
            .get_mut(&split_meta.table_name)
            .ok_or_else(|| anyhow!("table '{}' does not exist", split_meta.table_name))?;
        table_entry.splits.push(split_meta);
        self.save_entry_to_file_system(&table_entry).await?;
        Ok(())
    }

    async fn save_entry_to_file_system(&self, table_entry: &TableEntry) -> Result<()> {
        let table_path = self.directory.join(&table_entry.table.name);
        fs::create_dir_all(&table_path).await?;
        let meta_json = serde_json::to_string(table_entry)?;
        fs::write(table_path.join(TABLE_META_FILE), meta_json).await?;
        Ok(())
    }

    async fn load_tables_from_file_system(
        directory: &PathBuf,
    ) -> Result<HashMap<String, TableEntry>> {
        let mut entries = HashMap::new();
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let metastore_file_path = entry.path().join(TABLE_META_FILE);
                let data = fs::read(&metastore_file_path).await?;
                let table_entry = serde_json::from_slice::<TableEntry>(&data)?;
                entries.insert(table_entry.table.name.clone(), table_entry);
            }
        }
        Ok(entries)
    }

}

#[tonic::async_trait]
impl MetastoreService for FileSystemMetastoreServiceImpl {
    async fn fetch_events(&self, last_checkin: Option<u64>) -> Result<Vec<MetastoreEvent>> {
        //TODO:
        Ok(vec![])
    }
    
    async fn list_tables( &self) -> Result<Vec<TableMeta>> {
        self.list_tables().await
    }

    async fn put_table(&self, table_meta: TableMeta) -> Result<()> {
        self.put_table(table_meta).await
    }

    async fn get_table(&self, table_name: &str) -> Result<TableMeta> {
        self.get_table(table_name).await
    }

    async fn delete_table(&self, table_name: &str) -> Result<()> {
        self.delete_table(table_name).await
    }

    async fn put_split(&self, split_meta: SplitMeta) -> Result<()> {
        self.put_split(split_meta).await
    }
}
