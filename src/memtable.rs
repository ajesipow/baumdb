use crate::db::DB;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[non_exhaustive]
#[derive(Debug, Clone)]
pub(crate) enum MemValue {
    Put(String),
    Delete,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct MemTable(BTreeMap<String, MemValue>);

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

// TODO really needed?
#[async_trait]
impl DB for MemTable {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.0.get(key).and_then(|v| match v {
            MemValue::Put(str) => Some(str.to_string()),
            MemValue::Delete => None,
        }))
    }

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
