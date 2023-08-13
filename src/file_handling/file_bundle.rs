use async_trait::async_trait;
use itertools::Itertools;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
}

impl<'a> IntoIterator for &'a FileBundlesLevelled {
    type Item = SstFileBundle<'a>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let sorted_bundles = self
            .l0
            .iter()
            .map(Into::<SstFileBundle<'a>>::into)
            .chain(self.l1.iter().map(Into::<SstFileBundle<'a>>::into))
            .chain(self.l2.iter().map(Into::<SstFileBundle<'a>>::into))
            .collect_vec();
        sorted_bundles.into_iter()
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
    async fn new_file_bundle(&self, level: Level) -> UncommittedFileBundle;

    /// Commit and uncommitted file bundle and make it therefore visible to the outside.
    async fn commit_file_path_bundle(
        &self,
        uncommitted_bundle: UncommittedFileBundle,
    ) -> ShouldCompact;

    /// Remove bundles from `level`.
    /// Returns the number of deleted file bundles.
    async fn remove_bundles(&mut self, bundles_to_remove: &HashSet<FileBundleId>) -> usize;
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
    async fn new_file_bundle(&self, level: Level) -> UncommittedFileBundle {
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

    async fn commit_file_path_bundle(
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

    async fn remove_bundles(&mut self, bundles_to_remove: &HashSet<FileBundleId>) -> usize {
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
