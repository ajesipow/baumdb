use std::collections::HashSet;

use anyhow::Result;
use async_trait::async_trait;

use crate::file_handling::file_bundle::FileBundleHandle;
use crate::file_handling::file_bundle::FileBundleId;
use crate::file_handling::file_bundle::FileBundles;
use crate::file_handling::file_bundle::Level;
use crate::file_handling::file_bundle::ShouldCompact;
use crate::file_handling::flushing::flush;
use crate::file_handling::DataHandling;
use crate::memtable::MemTable;
use crate::memtable::MemValue;

#[async_trait]
pub(super) trait Compaction {
    async fn compact(&self) -> Result<()>;
}

#[async_trait]
impl Compaction for FileBundles {
    async fn compact(&self) -> Result<()> {
        let mut level_to_compact = Level::L0;
        loop {
            let Some(next_level) = level_to_compact.next_level() else {
                // Cannot compact last level
                return Ok(());
            };

            let arc = self.inner();
            let read_lock = arc.read().await;
            let mut bundles = match level_to_compact {
                Level::L0 => read_lock.l0.clone(),
                Level::L1 => read_lock.l1.clone(),
                Level::L2 => unreachable!(),
            };
            drop(read_lock);

            if bundles.is_empty() {
                return Ok(());
            }
            let mut compacted_bundle_ids: HashSet<FileBundleId> = HashSet::new();
            // Invariant is that they are sorted in order, reversing then is oldest to newest.
            bundles.make_contiguous().reverse();

            let mut iter = bundles.iter();
            // Unwrap is OK here because at least one element must exist as checked above
            let oldest_bundle = iter.next().unwrap();
            // The oldest table serves as the table we merge newer data into
            let mut merger_table = MemTable::try_from_file(oldest_bundle.main_data_file_path())
                .await?
                .into_inner();
            compacted_bundle_ids.insert(oldest_bundle.id());

            for newer_bundle in iter {
                let newer_table =
                    MemTable::try_from_file(newer_bundle.main_data_file_path()).await?;
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

            if merger_table.is_empty() {
                return Ok(());
            }

            let should_compact =
                flush(MemTable::from(merger_table), self.clone(), next_level).await?;
            self.clone().remove_bundles(&compacted_bundle_ids).await;
            if should_compact == ShouldCompact::Yes {
                level_to_compact = next_level
            } else {
                return Ok(());
            }
        }
    }
}
