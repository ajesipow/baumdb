use crate::bloom_filter::{BloomFilter, DefaultBloomFilter};
use crate::deserialization::{read_key_offset, read_value};
use crate::file_handling::{DataHandling, FileHandling, SstFileBundle, SstFileHandler};
use crate::memtable::{MemTable, MemTableRead, MemTableReadOnly, MemTableWrite, MemValue};
use anyhow::Result;
use async_trait::async_trait;
use flate2::read::GzDecoder;
use std::io::SeekFrom;
use std::io::{Cursor, Read};
use std::mem;
use std::path::{Path, PathBuf};
use tokio::fs::{create_dir, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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
    // A secondary table that can only be read from.
    // It is corresponds to the previous main table and is needed to support
    // reads while the previous main table is still flushed to disk.
    secondary_table: MemTableReadOnly,
    max_memtable_size: usize,
    file_handler: SstFileHandler,
}

#[async_trait]
impl DB for BaumDb {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.main_table.get(key)? {
            return Ok(Some(value));
        }
        // Check the secondary table (representing the previous memtable)
        match self.secondary_table.get(key)? {
            Some(value) => Ok(Some(value)),
            None => {
                let file_path_bundles = self.file_handler.file_path_bundles();
                // TODO can skip first SStable (because it is equivalent to secondary table)
                // But need to make sure no tables are currently flushed
                for SstFileBundle {
                    main_data_file_path,
                    index_file_path,
                    bloom_filter_file_path,
                } in &*file_path_bundles.inner().read().await
                {
                    let bloom_filter =
                        DefaultBloomFilter::try_from_file(bloom_filter_file_path).await?;
                    if bloom_filter.may_contain_key(key) {
                        let mut index_file = File::open(index_file_path).await?;
                        let mut index_as_bytes = Vec::<u8>::new();
                        index_file.read_to_end(&mut index_as_bytes).await?;

                        // Invariant here is that the index is already sorted
                        // TODO: we're over-allocating here - is there a better way?
                        let mut index_vec = Vec::with_capacity(index_as_bytes.len());
                        let mut cursor = Cursor::new(index_as_bytes);
                        while let Ok((indexed_key, offset)) = read_key_offset(&mut cursor) {
                            index_vec.push((indexed_key, offset));
                        }
                        let mut index_iter = index_vec.iter().peekable();
                        if let Some((_idx, offset)) = index_iter
                            .next_if(|(idx, _)| idx.as_str() < key)
                            .or_else(|| index_iter.next())
                        {
                            let mut main_data_file = File::open(&main_data_file_path).await?;
                            main_data_file.seek(SeekFrom::Start(*offset)).await?;
                            let encoded_block_length = main_data_file.read_u64().await? as usize;
                            let mut raw_block = vec![0; encoded_block_length];
                            main_data_file.read_exact(&mut raw_block).await?;

                            let mut decoder = GzDecoder::new(raw_block.as_slice());
                            // The vec will very likely end up larger than the `n_bytes_to_read`,
                            // but that's the best we know at this point and it'll save some reallocations.
                            let mut decompressed_block = Vec::with_capacity(raw_block.len());
                            decoder.read_to_end(&mut decompressed_block)?;
                            let mut decompressed_cursor = Cursor::new(decompressed_block);
                            while let Ok((existing_key, existing_value)) =
                                read_value(&mut decompressed_cursor)
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
        self.main_table.put(key, value)?;
        self.maybe_flush_memtable().await
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        self.main_table.delete(key)?;
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
            main_table: Default::default(),
            secondary_table: Default::default(),
            max_memtable_size,
            file_handler: SstFileHandler::new(path),
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
        let mut previous_memtable = Default::default();
        mem::swap(&mut previous_memtable, &mut self.main_table);
        self.secondary_table = previous_memtable.clone().into();
        self.file_handler.flush(previous_memtable).await?;
        Ok(())
    }
}
