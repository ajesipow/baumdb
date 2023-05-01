use crate::memtable::{MemTable, MemValue};
use anyhow::Result;
use async_trait::async_trait;
use std::mem;
use std::path::{Path, PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;

#[async_trait]
pub(crate) trait DB {
    async fn get(&self, key: &str) -> Result<Option<&String>>;

    async fn put(&mut self, key: String, value: String) -> Result<()>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}

#[derive(Debug)]
pub struct BaumDb {
    memtable: MemTable,
    max_memtable_size: usize,
    sst_dir_path: Path,
}

impl BaumDb {
    async fn flush_memtable(&mut self) -> Result<()> {
        let previous_memtable = mem::take(&mut self.memtable);
        // Write to disk in separate thread:
        // 1. Index
        // 2. Data
        // TODO: logic for filenaming
        // TODO: error handling of file writing?
        tokio::task::spawn(Self::flush_memtable_inner(
            self.sst_dir_path.to_path_buf(),
            previous_memtable,
        ));
        Ok(())
    }

    async fn flush_memtable_inner(
        mut file_path: PathBuf,
        previous_memtable: MemTable,
    ) -> Result<()> {
        let file_name = format!("L0-{}", Uuid::new_v4());
        file_path.push(file_name);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(file_path)
            .await?;
        let mut buffer = BufWriter::new(file);
        for (key, value) in previous_memtable {
            // TODO: implement proper serialization
            // Encode the key length and value length first for easier parsing
            let key_len = key.len() as u64;
            buffer.write_u64(key_len).await?;
            buffer.write_all(key.as_bytes()).await?;
            match value {
                MemValue::Delete => {
                    buffer.write_u8(0).await?;
                }
                MemValue::Put(value_str) => {
                    buffer.write_u8(1).await?;
                    buffer.write_u64(value_str.len() as u64).await?;
                    buffer.write_all(value_str.as_bytes()).await?;
                }
            }
        }
        buffer.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl DB for BaumDb {
    async fn get(&self, key: &str) -> Result<Option<&String>> {
        match self.memtable.get(key).await? {
            Some(value) => Ok(Some(value)),
            None => {
                // TODO check SSTables on disk
                Ok(None)
            }
        }
    }

    async fn put(&mut self, key: String, value: String) -> Result<()> {
        // TODO check if memtable grew too much, if so, swap with new and flush to disk
        self.memtable.put(key, value).await?;
        if self.memtable.len() >= self.max_memtable_size {
            self.flush_memtable().await?;
        }
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        // TODO check if memtable grew too much, if so, swap with new and flush to disk
        self.memtable.delete(key).await
    }
}
