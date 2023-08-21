use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::Path;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs::remove_file;
use tokio::sync::RwLock;
use uuid::Uuid;

// The number of file bundles per level before compaction is started
const LEVEL_COMPACTION_THRESHOLD: usize = 4;

#[derive(Debug, Clone)]
pub(crate) struct FileBundlesLevelled {
    base_path: PathBuf,
    pub(crate) l0: VecDeque<FileBundle>,
    pub(crate) l1: VecDeque<FileBundle>,
    pub(crate) l2: VecDeque<FileBundle>,
}

impl FileBundlesLevelled {
    fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            l0: VecDeque::with_capacity(LEVEL_COMPACTION_THRESHOLD),
            l1: VecDeque::with_capacity(LEVEL_COMPACTION_THRESHOLD),
            l2: VecDeque::with_capacity(LEVEL_COMPACTION_THRESHOLD),
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_> {
        let (i01, i02) = self.l0.as_slices();
        let (i11, i12) = self.l1.as_slices();
        let (i21, i22) = self.l2.as_slices();

        Iter {
            i01: i01.iter(),
            i02: i02.iter(),
            i11: i11.iter(),
            i12: i12.iter(),
            i21: i21.iter(),
            i22: i22.iter(),
        }
    }
}

pub(crate) struct Iter<'a> {
    i01: slice::Iter<'a, FileBundle>,
    i02: slice::Iter<'a, FileBundle>,
    i11: slice::Iter<'a, FileBundle>,
    i12: slice::Iter<'a, FileBundle>,
    i21: slice::Iter<'a, FileBundle>,
    i22: slice::Iter<'a, FileBundle>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = SstFileBundle<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.i01
            .next()
            .or_else(|| self.i02.next())
            .or_else(|| self.i11.next())
            .or_else(|| self.i12.next())
            .or_else(|| self.i21.next())
            .or_else(|| self.i22.next())
            .map(Into::<SstFileBundle<'a>>::into)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SstFileBundle<'a> {
    pub main_data_file_path: &'a Path,
    pub index_file_path: &'a Path,
    pub bloom_filter_file_path: &'a Path,
}

#[derive(Debug, Clone)]
pub(crate) struct FileBundle {
    id: FileBundleId,
    main_data_file_path: PathBuf,
    index_file_path: PathBuf,
    bloom_filter_file_path: PathBuf,
    level: Level,
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) struct FileBundleId(Uuid);

impl FileBundleId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub(crate) enum Level {
    L0,
    L1,
    L2,
}

impl Level {
    pub(crate) fn next_level(&self) -> Option<Self> {
        match self {
            Level::L0 => Some(Level::L1),
            Level::L1 => Some(Level::L2),
            Level::L2 => None,
        }
    }
}

impl FileBundle {
    pub(crate) fn id(&self) -> FileBundleId {
        self.id
    }

    pub(crate) fn main_data_file_path(&self) -> &Path {
        &self.main_data_file_path
    }
}

impl<'a> From<&'a FileBundle> for SstFileBundle<'a> {
    fn from(value: &'a FileBundle) -> Self {
        SstFileBundle {
            main_data_file_path: &value.main_data_file_path,
            index_file_path: &value.index_file_path,
            bloom_filter_file_path: &value.bloom_filter_file_path,
        }
    }
}

impl From<UncommittedFileBundle> for FileBundle {
    fn from(value: UncommittedFileBundle) -> Self {
        Self {
            id: value.0.id,
            main_data_file_path: value.0.main_data_file_path,
            index_file_path: value.0.index_file_path,
            bloom_filter_file_path: value.0.bloom_filter_file_path,
            level: value.0.level,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UncommittedFileBundle(FileBundle);

impl UncommittedFileBundle {
    fn into_inner(self) -> FileBundle {
        self.0
    }

    pub(crate) fn main_data_file_path(&self) -> &PathBuf {
        &self.0.main_data_file_path
    }

    pub(crate) fn index_file_path(&self) -> &PathBuf {
        &self.0.index_file_path
    }

    pub(crate) fn bloom_filter_file_path(&self) -> &PathBuf {
        &self.0.bloom_filter_file_path
    }
}

/// Signals whether or not compaction should be performed
#[derive(Debug, PartialEq)]
pub(crate) enum ShouldCompact {
    Yes,
    No,
}

#[async_trait]
pub(crate) trait FileBundleHandle {
    /// Gets a uncommitted new file bundle on level 0.
    /// Uncommitted means it is not yet visible to the outside.
    async fn new_file_bundle(
        &self,
        level: Level,
    ) -> UncommittedFileBundle;

    /// Commit and uncommitted file bundle and make it therefore visible to the outside.
    async fn commit_file_bundle(
        &self,
        uncommitted_bundle: UncommittedFileBundle,
    ) -> ShouldCompact;

    /// Remove bundles from `level`.
    /// Returns the number of deleted file bundles.
    async fn remove_bundles(
        &mut self,
        bundles_to_remove: &HashSet<FileBundleId>,
    ) -> usize;
}

#[derive(Debug, Clone)]
pub(crate) struct FileBundles(Arc<RwLock<FileBundlesLevelled>>);

impl FileBundles {
    pub fn new(base_path: PathBuf) -> Self {
        Self(Arc::new(RwLock::new(FileBundlesLevelled::new(base_path))))
    }

    pub fn inner(&self) -> Arc<RwLock<FileBundlesLevelled>> {
        self.0.clone()
    }
}

#[async_trait]
impl FileBundleHandle for FileBundles {
    async fn new_file_bundle(
        &self,
        level: Level,
    ) -> UncommittedFileBundle {
        let read_lock = self.0.read().await;
        let bundle_length = match level {
            Level::L0 => read_lock.l0.len(),
            Level::L1 => read_lock.l1.len(),
            Level::L2 => read_lock.l2.len(),
        };
        let base_path = read_lock.base_path.clone();
        drop(read_lock);

        // TODO better naming convention?
        let main_data_file_name = PathBuf::from(&format!("{:?}-data-{}.db", level, bundle_length));
        let index_file_name = PathBuf::from(&format!("{:?}-index-{}.db", level, bundle_length));
        let bloom_filter_file_name =
            PathBuf::from(&format!("{:?}-bloom-{}.db", level, bundle_length));

        let main_data_file_path = Path::join(&base_path, main_data_file_name);
        let index_file_path = Path::join(&base_path, index_file_name);
        let bloom_filter_file_path = Path::join(&base_path, bloom_filter_file_name);

        let bundle = FileBundle {
            id: FileBundleId::new(),
            main_data_file_path,
            index_file_path,
            bloom_filter_file_path,
            level,
        };
        UncommittedFileBundle(bundle)
    }

    async fn commit_file_bundle(
        &self,
        uncommitted_bundle: UncommittedFileBundle,
    ) -> ShouldCompact {
        let mut lock = self.0.write().await;
        let should_compact = match uncommitted_bundle.0.level {
            Level::L0 => {
                lock.l0.push_front(uncommitted_bundle.into_inner());
                lock.l0.len() >= 4
            }
            Level::L1 => {
                lock.l1.push_front(uncommitted_bundle.into_inner());
                lock.l1.len() >= 8
            }
            Level::L2 => {
                lock.l2.push_front(uncommitted_bundle.into_inner());
                // Never compact the last level
                false
            }
        };

        if should_compact {
            ShouldCompact::Yes
        } else {
            ShouldCompact::No
        }
    }

    async fn remove_bundles(
        &mut self,
        bundles_to_remove: &HashSet<FileBundleId>,
    ) -> usize {
        let mut files_to_delete = Vec::with_capacity(bundles_to_remove.len());
        let mut lock = self.0.write().await;
        // TODO: DRY
        let mut i = 0;
        while i < lock.l0.len() {
            if bundles_to_remove.contains(&lock.l0[i].id) {
                files_to_delete.push(lock.l0.remove(i).unwrap())
            } else {
                i += 1;
            }
        }
        let mut i = 0;
        while i < lock.l1.len() {
            if bundles_to_remove.contains(&lock.l1[i].id) {
                files_to_delete.push(lock.l1.remove(i).unwrap())
            } else {
                i += 1;
            }
        }
        let mut i = 0;
        while i < lock.l2.len() {
            if bundles_to_remove.contains(&lock.l2[i].id) {
                files_to_delete.push(lock.l2.remove(i).unwrap())
            } else {
                i += 1;
            }
        }
        drop(lock);

        let n_deleted_files = files_to_delete.len();
        for FileBundle {
            id: _,
            main_data_file_path,
            index_file_path,
            bloom_filter_file_path,
            level: _,
        } in files_to_delete
        {
            remove_file(bloom_filter_file_path).await.unwrap();
            remove_file(index_file_path).await.unwrap();
            remove_file(main_data_file_path).await.unwrap();
        }
        n_deleted_files
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_bundle_iterator_works_correctly() {
        impl FileBundle {
            fn new_with_path_level(
                main_path: &Path,
                level: Level,
            ) -> Self {
                Self {
                    id: FileBundleId::new(),
                    main_data_file_path: main_path.to_path_buf(),
                    index_file_path: Default::default(),
                    bloom_filter_file_path: Default::default(),
                    level,
                }
            }
        }

        let mut l0 = VecDeque::new();
        let mut l1 = VecDeque::new();
        let mut l2 = VecDeque::new();

        let path_1 = PathBuf::from("1");
        let path_2 = PathBuf::from("2");
        let path_3 = PathBuf::from("3");
        let path_4 = PathBuf::from("4");
        let path_5 = PathBuf::from("5");
        let path_6 = PathBuf::from("6");

        let bundle_1 = FileBundle::new_with_path_level(&path_1, Level::L0);
        let bundle_2 = FileBundle::new_with_path_level(&path_2, Level::L0);
        let bundle_3 = FileBundle::new_with_path_level(&path_3, Level::L1);
        let bundle_4 = FileBundle::new_with_path_level(&path_4, Level::L1);
        let bundle_5 = FileBundle::new_with_path_level(&path_5, Level::L2);
        let bundle_6 = FileBundle::new_with_path_level(&path_6, Level::L2);

        l0.push_back(bundle_2);
        l0.push_front(bundle_1);
        l1.push_back(bundle_4);
        l1.push_front(bundle_3);
        l2.push_back(bundle_6);
        l2.push_front(bundle_5);

        let bundles = FileBundlesLevelled {
            base_path: Default::default(),
            l0,
            l1,
            l2,
        };

        for (bundle, path) in bundles
            .iter()
            .zip_eq([path_1, path_2, path_3, path_4, path_5, path_6])
        {
            println!("{:?}", bundle);
            assert_eq!(bundle.main_data_file_path, path);
        }
    }
}
