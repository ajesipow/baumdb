use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Buf;
use flate2::read::GzDecoder;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::deserialization::read_key_value;
use crate::deserialization::KeyValue;
use crate::file_handling::DataHandling;

#[non_exhaustive]
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum MemValue {
    Put(String),
    Delete,
}

type MemTableBase = BTreeMap<String, MemValue>;

/// The main MemTable struct.
#[derive(Default, Debug, Clone)]
pub(crate) struct MemTable(MemTableBase);

/// A secondary MemTable struct that only allows reading from.
#[derive(Default, Debug, Clone)]
pub(crate) struct MemTableReadOnly(MemTableBase);

impl MemTable {
    /// The size of the MemTable
    pub(crate) fn len(&self) -> usize {
        // TODO adjust this to take the value size of the table into account, not just the
        // number of entries.
        self.0.len()
    }

    pub(crate) fn into_inner(self) -> MemTableBase {
        self.0
    }
}

impl IntoIterator for MemTable {
    type IntoIter = std::collections::btree_map::IntoIter<String, MemValue>;
    type Item = (String, MemValue);

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl IntoIterator for MemTableReadOnly {
    type IntoIter = std::collections::btree_map::IntoIter<String, MemValue>;
    type Item = (String, MemValue);

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub(crate) trait MemTableGet {
    fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>>;
}

impl MemTableGet for MemTable {
    fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>> {
        memtable_get_inner(&self.0, key)
    }
}

impl MemTableGet for MemTableReadOnly {
    fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>> {
        memtable_get_inner(&self.0, key)
    }
}

pub(crate) trait MemTableWrite {
    fn put(
        &mut self,
        key: String,
        value: String,
    ) -> Result<()>;

    fn delete(
        &mut self,
        key: &str,
    ) -> Result<()>;
}

impl MemTableWrite for MemTable {
    fn put(
        &mut self,
        key: String,
        value: String,
    ) -> Result<()> {
        self.0.insert(key, MemValue::Put(value));
        Ok(())
    }

    fn delete(
        &mut self,
        key: &str,
    ) -> Result<()> {
        if self.0.remove(key).is_none() {
            // Key was not present in the memtable, but may be present in the SSTables on disk, so
            // let's add a tombstone just in case.
            self.0.insert(key.to_string(), MemValue::Delete);
        }
        Ok(())
    }
}

fn memtable_get_inner(
    base_table: &MemTableBase,
    key: &str,
) -> Result<Option<String>> {
    Ok(base_table.get(key).and_then(|v| match v {
        MemValue::Put(str) => Some(str.to_string()),
        MemValue::Delete => None,
    }))
}

impl From<MemTable> for MemTableReadOnly {
    fn from(value: MemTable) -> Self {
        Self(value.0)
    }
}

impl From<MemTableBase> for MemTable {
    fn from(value: MemTableBase) -> Self {
        Self(value)
    }
}

#[async_trait]
impl DataHandling for MemTable {
    async fn try_from_file<P>(path: P) -> Result<MemTable>
    where
        Self: Sized,
        P: AsRef<Path>,
        P: Into<PathBuf>,
        P: Send,
    {
        let mut memtable_file = File::open(path).await?;
        let mut memtable_bytes = Vec::<u8>::new();
        memtable_file.read_to_end(&mut memtable_bytes).await?;
        let mut raw_table: MemTableBase = BTreeMap::new();

        let mut memtable_bytes = Cursor::new(memtable_bytes);

        while memtable_bytes.has_remaining() {
            let encoded_block_length = memtable_bytes.read_u64().await? as usize;
            let mut raw_block = vec![0; encoded_block_length];
            std::io::Read::read_exact(&mut memtable_bytes, &mut raw_block)?;

            let mut decoder = GzDecoder::new(raw_block.as_slice());
            // The vec will very likely end up larger than the `n_bytes_to_read`,
            // but that's the best we know at this point and it'll save some reallocations.
            let mut decompressed_block = Vec::with_capacity(raw_block.len());
            decoder.read_to_end(&mut decompressed_block)?;
            let mut decompressed_cursor = Cursor::new(decompressed_block);
            while let Ok(KeyValue { key, value }) = read_key_value(&mut decompressed_cursor) {
                raw_table.insert(key, value);
            }
        }
        Ok(MemTable(raw_table))
    }
}
