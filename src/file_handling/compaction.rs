use crate::file_handling::file_bundle::{FileBundles, Level};
use crate::file_handling::flushing::flush;
use crate::file_handling::DataHandling;
use crate::memtable::{MemTable, MemValue};
use async_trait::async_trait;
use std::ops::DerefMut;

#[async_trait]
pub(super) trait Compaction {
    async fn compact(&self);
}

#[async_trait]
impl Compaction for FileBundles {
    async fn compact(&self) {
        let arc = self.inner();
        // TODO not great that we have to get a write lock because we're doing a make contiguous call in `.l0()`
        let mut read_lock = arc.write().await;
        let file_bundles = read_lock.deref_mut();
        let mut l0_bundles = file_bundles.l0().to_owned();
        drop(read_lock);

        if l0_bundles.is_empty() {
            return;
        }
        // Invariant is that they are sorted in order, reversing then is oldest to newest.
        l0_bundles.reverse();

        let mut iter = l0_bundles.iter();
        // Unwrap is OK here because at least one element must exist as checked above
        let oldest_bundle = iter.next().unwrap();
        // The oldest table serves as the table we merge newer data into
        let mut merger_table = MemTable::try_from_file(oldest_bundle.main_data_file_path())
            .await
            .unwrap()
            .into_inner();

        for newer_bundle in iter {
            let newer_table = MemTable::try_from_file(newer_bundle.main_data_file_path())
                .await
                .unwrap();
            for (key, value) in newer_table.into_iter() {
                match value {
                    MemValue::Put(_) => {
                        merger_table.insert(key, value);
                    }
                    MemValue::Delete => {
                        merger_table.remove(&key);
                    }
                }
            }
        }

        println!("merger table: {:#?}", merger_table);
        println!("-----");

        if !merger_table.is_empty() {
            let _ = flush(MemTable::from(merger_table), self.clone(), Level::L1).await;
        }

        // build indexes and bloom filter
        // save new level files to disk
        // set compacted flag for affected files and commit new level files
        // delete compacted files

        // TODO insert new file at level 1
        // TODO delete compacted files
        // repeat
        // TODO: is vecdeque the best choice here?
    }
}
