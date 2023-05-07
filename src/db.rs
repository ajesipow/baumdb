use crate::memtable::{MemTable, MemValue};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::mem;
use std::path::{Path, PathBuf};
use tokio::fs::{create_dir, read_dir};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use uuid::Uuid;

#[async_trait]
pub trait DB {
    async fn get(&self, key: &str) -> Result<Option<String>>;

    async fn put(&mut self, key: String, value: String) -> Result<()>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}

#[derive(Debug)]
pub struct BaumDb {
    // The main memtable for reading from and writing to.
    rw_table: MemTable,
    // A secondary table that must only be read from. It is used to support reads while the main table
    // is flushed to disk.
    read_table: MemTable,
    max_memtable_size: usize,
    sst_dir_path: PathBuf,
}

#[async_trait]
impl DB for BaumDb {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.rw_table.get(key).await? {
            return Ok(Some(value));
        }
        match self.read_table.get(key).await? {
            Some(value) => Ok(Some(value)),
            None => {
                // TODO check SSTables newest to oldest instead of just going over all files
                // TODO Don't have to check
                let mut stream = read_dir(&self.sst_dir_path).await?;
                while let Some(entry) = stream.next_entry().await? {
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    if path
                        .file_stem()
                        .and_then(|stem| stem.to_str().map(|s| s.starts_with("L0-")))
                        .unwrap_or(false)
                    {
                        let file = File::open(path).await?;
                        // TODO: implement proper deserialization
                        let mut buffer = BufReader::new(file);
                        while let Ok((existing_key, existing_value)) = read_value(&mut buffer).await
                        {
                            if existing_key == key {
                                match existing_value {
                                    MemValue::Put(existing_value_str) => {
                                        return Ok(Some(existing_value_str))
                                    }
                                    MemValue::Delete => (),
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    async fn put(&mut self, key: String, value: String) -> Result<()> {
        self.rw_table.put(key, value).await?;
        self.maybe_flush_memtable().await
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        // TODO handle read memtable
        self.rw_table.delete(key).await?;
        self.maybe_flush_memtable().await
    }
}

impl BaumDb {
    pub async fn new<P>(sst_dir_path: P, max_memtable_size: usize) -> Self
    where
        P: AsRef<Path>,
        P: Into<PathBuf>,
    {
        let path: PathBuf = sst_dir_path.into();
        if !path.exists() {
            create_dir(&path)
                .await
                .expect("be able to create directory");
        }
        Self {
            rw_table: Default::default(),
            read_table: Default::default(),
            max_memtable_size,
            sst_dir_path: path,
        }
    }

    // TODO accidental concurrent flushes must be avoided
    async fn maybe_flush_memtable(&mut self) -> Result<()> {
        if self.rw_table.len() >= self.max_memtable_size {
            self.flush_memtable().await?;
        }
        Ok(())
    }

    async fn flush_memtable(&mut self) -> Result<()> {
        // Can be safely reset because it only contains data that has already been flushed to disk
        self.read_table = Default::default();
        mem::swap(&mut self.read_table, &mut self.rw_table);
        let previous_memtable = self.read_table.clone();
        // TODO: logic for file naming
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
        let file_name = format!("L0-{}.baum", Uuid::new_v4());
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

async fn read_value(file_buffer: &mut BufReader<File>) -> Result<(String, MemValue)> {
    let key_len = file_buffer.read_u64().await? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    let _n = file_buffer.read_exact(&mut buf).await?;
    let key = String::from_utf8(buf)?;
    let value_type = file_buffer.read_u8().await?;
    match value_type {
        0 => Ok((key, MemValue::Delete)),
        1 => {
            let value_len = file_buffer.read_u64().await? as usize;
            let mut buf: Vec<u8> = vec![0; value_len];
            let _n = file_buffer.read_exact(&mut buf).await?;
            let value = String::from_utf8(buf)?;
            Ok((key, MemValue::Put(value)))
        }
        _ => Err(anyhow!("Wrong value type byte: {value_type}")),
    }
}
