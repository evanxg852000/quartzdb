use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{Ok, Result, anyhow};
use iceberg::{io::{InputFile, LocalFsStorage, OutputFile}, puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter}};

pub struct PackedFileWriter {
    writer: PuffinWriter,    
}

impl PackedFileWriter {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file_path = path.as_ref().to_str()
            .ok_or_else(|| anyhow!("Could not convert path to string"))?
            .to_string();
        let file = OutputFile::new(Arc::new(LocalFsStorage::new()), file_path);
        let writer = PuffinWriter::new(&file, HashMap::default(), false).await?;
        Ok(Self{writer})
    }

    pub async fn add(&mut self, path: impl AsRef<Path>, data:  Vec<u8>) -> Result<()> {
        let entry_path = path.as_ref().to_str()
            .ok_or_else(|| anyhow!("Could not convert path to string"))?
            .to_string();
        let blob =Blob::builder()
            .r#type(entry_path)
            .data(data)
            .fields(vec![])
            .snapshot_id(0)
            .sequence_number(0)
            .properties(HashMap::new())
            .build();
        self.writer.add(blob, CompressionCodec::None).await?;
        Ok(())
    }

    pub async fn finilize(self) -> Result<()> {
        self.writer.close().await?;
        Ok(())
    }
}

pub struct PackedFileReader {
    reader: PuffinReader,
}

impl PackedFileReader {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file_path = path.as_ref().to_str()
            .ok_or_else(|| anyhow!("Could not convert path to string"))?
            .to_string();
        let file = InputFile::new(Arc::new(LocalFsStorage::new()), file_path);
        let reader = PuffinReader::new(file);
        Ok(Self{reader})
    }

    pub async fn get(&self, path: impl AsRef<Path>) -> Result<Vec<u8>> {
        let entry_path = path.as_ref().to_str()
            .ok_or_else(|| anyhow!("Could not convert path to string"))?
            .to_string();
        let blob_metadata = self.reader.file_metadata()
            .await?
            .blobs()
            .iter()
            .find(|m| m.blob_type() == entry_path)
            .ok_or_else(|| anyhow!("Could not find entry: `{}`", entry_path))?;
        let data = self.reader.blob(blob_metadata).await?.data().to_vec();
        Ok(data)
    } 

     pub async fn exists(&self, path: impl AsRef<Path>) -> Result<bool> {
        let entry_path = path.as_ref().to_str()
            .ok_or_else(|| anyhow!("Could not convert path to string"))?
            .to_string();
        let exist = self.reader.file_metadata()
            .await?
            .blobs()
            .iter()
            .find(|m| m.blob_type() == entry_path)
            .is_some();
            Ok(exist)
    } 
}
