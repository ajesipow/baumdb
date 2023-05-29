use crate::deserialization::{read_key_offset, read_value};
use crate::file_handler::{FileHandling, SstFileBundle, SstFileHandler};
use crate::memtable::{MemTable, MemValue};
use anyhow::Result;
use async_trait::async_trait;
use std::io::SeekFrom;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{create_dir, File};
use tokio::io::{AsyncSeekExt, BufReader};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};

#[async_trait]
pub trait DB {
    async fn get(&self, key: &str) -> Result<Option<String>>;

    async fn put(&mut self, key: String, value: String) -> Result<()>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}

#[derive(Debug)]
pub struct BaumDb {
    // The main memtable for reading from and writing to.
    main_table: MemTable,
    // A secondary table that must only be read from. It is used to support reads while the main table
    // is flushed to disk.
    secondary_table: MemTable,
    max_memtable_size: usize,
    file_handler: Arc<RwLock<SstFileHandler>>,
    flush_sender: Sender<MemTable>,
}

#[async_trait]
impl DB for BaumDb {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        println!(" --- looking for key: {:?}", key);
        if let Some(value) = self.main_table.get(key).await? {
            return Ok(Some(value));
        }
        // Check the secondary table (representing the previous memtable)
        match self.secondary_table.get(key).await? {
            Some(value) => Ok(Some(value)),
            None => {
                let read_lock = self.file_handler.read().await;
                let file_path_bundles = read_lock.file_path_bundles();
                drop(read_lock);
                for SstFileBundle {
                    main_data_file_path,
                    index_file_path,
                } in file_path_bundles
                {
                    let index_file = File::open(index_file_path).await?;
                    // TODO load entire index into memory
                    let mut buffer = BufReader::new(index_file);
                    while let Ok((indexed_key, offset)) = read_key_offset(&mut buffer).await {
                        if indexed_key == key {
                            let mut main_data_file = File::open(&main_data_file_path).await?;
                            main_data_file.seek(SeekFrom::Start(offset)).await?;
                            if let Ok((existing_key, existing_value)) =
                                read_value(&mut main_data_file).await
                            {
                                if existing_key == key {
                                    return match existing_value {
                                        MemValue::Put(existing_value_str) => {
                                            Ok(Some(existing_value_str))
                                        }
                                        MemValue::Delete => Ok(None),
                                    };
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
        self.main_table.put(key, value).await?;
        self.maybe_flush_memtable().await
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        // TODO handle read memtable
        self.main_table.delete(key).await?;
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
        let file_handler = Arc::new(RwLock::new(SstFileHandler::new(path)));
        let (tx, mut rx) = mpsc::channel(1);
        let file_handler_clone = file_handler.clone();
        tokio::spawn(async move {
            while let Some(memtable) = rx.recv().await {
                let mut lock = file_handler_clone.write().await;
                let _ = lock.flush(memtable).await;
                drop(lock)
            }
        });

        Self {
            main_table: Default::default(),
            secondary_table: Default::default(),
            max_memtable_size,
            file_handler,
            flush_sender: tx,
        }
    }

    async fn maybe_flush_memtable(&mut self) -> Result<()> {
        if self.main_table.len() >= self.max_memtable_size {
            self.flush_memtable().await?;
        }
        Ok(())
    }

    async fn flush_memtable(&mut self) -> Result<()> {
        // Can be safely reset because it only contains data that has already been flushed to disk
        self.secondary_table = Default::default();
        mem::swap(&mut self.secondary_table, &mut self.main_table);
        let previous_memtable = self.secondary_table.clone();
        self.flush_sender.send(previous_memtable).await?;
        Ok(())
    }
}
