use crate::file_handling::file_bundle::{FileBundleHandle, FileBundles, Level};
use crate::file_handling::flushing::flush;
use crate::file_handling::DataHandling;
use crate::memtable::{MemTable, MemValue};
use async_trait::async_trait;
use std::collections::HashSet;
use uuid::Uuid;

#[async_trait]
pub(super) trait Compaction {
    async fn compact(&self);
}

#[async_trait]
impl Compaction for FileBundles {
    async fn compact(&self) {
        let arc = self.inner();
        let read_lock = arc.read().await;
        let mut l0_bundles = read_lock.l0.clone();
        drop(read_lock);

        if l0_bundles.is_empty() {
            return;
        }
        let mut compacted_bundle_ids: HashSet<Uuid> = HashSet::new();
        // Invariant is that they are sorted in order, reversing then is oldest to newest.
        l0_bundles.make_contiguous().reverse();

        let mut iter = l0_bundles.iter();
        // Unwrap is OK here because at least one element must exist as checked above
        let oldest_bundle = iter.next().unwrap();
        // The oldest table serves as the table we merge newer data into
        let mut merger_table = MemTable::try_from_file(oldest_bundle.main_data_file_path())
            .await
            .unwrap()
            .into_inner();
        compacted_bundle_ids.insert(oldest_bundle.id());

        for newer_bundle in iter {
            // TODO handle unwraps
            let newer_table = MemTable::try_from_file(newer_bundle.main_data_file_path())
                .await
                .unwrap();
            compacted_bundle_ids.insert(newer_bundle.id());
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

        if !merger_table.is_empty() {
            let _ = flush(MemTable::from(merger_table), self.clone(), Level::L1).await;
            self.clone().remove_bundles(&compacted_bundle_ids).await;
        }
    }
}
