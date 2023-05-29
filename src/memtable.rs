use anyhow::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;

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
        // TODO adjust this to take the effective size of the table into account, not just the
        // number of entries.
        self.0.len()
    }
}

impl IntoIterator for MemTable {
    type Item = (String, MemValue);
    type IntoIter = std::collections::btree_map::IntoIter<String, MemValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl IntoIterator for MemTableReadOnly {
    type Item = (String, MemValue);
    type IntoIter = std::collections::btree_map::IntoIter<String, MemValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[async_trait]
pub(crate) trait MemTableRead {
    async fn get(&self, key: &str) -> Result<Option<String>>;
}

#[async_trait]
impl MemTableRead for MemTable {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        memtable_get_inner(&self.0, key)
    }
}

#[async_trait]
impl MemTableRead for MemTableReadOnly {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        memtable_get_inner(&self.0, key)
    }
}

#[async_trait]
pub(crate) trait MemTableWrite {
    async fn put(&mut self, key: String, value: String) -> Result<()>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}

#[async_trait]
impl MemTableWrite for MemTable {
    async fn put(&mut self, key: String, value: String) -> Result<()> {
        self.0.insert(key, MemValue::Put(value));
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        if self.0.remove(key).is_none() {
            // Key was not present in the memtable, but may be present in the SSTables on disk, so
            // let's add a tombstone just in case.
            self.0.insert(key.to_string(), MemValue::Delete);
        }
        Ok(())
    }
}

fn memtable_get_inner(base_table: &MemTableBase, key: &str) -> Result<Option<String>> {
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
