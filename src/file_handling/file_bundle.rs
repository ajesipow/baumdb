use async_trait::async_trait;
use itertools::Itertools;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

const LEVEL_COMPACTION_THRESHOLD: usize = 4;

#[derive(Debug, Clone)]
pub(crate) struct FileBundlesLevelled {
    base_path: PathBuf,
    l0: VecDeque<FileBundle>,
    l1: VecDeque<FileBundle>,
    l2: VecDeque<FileBundle>,
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

    pub fn l0(&mut self) -> &[FileBundle] {
        self.l0.make_contiguous();
        self.l0.as_slices().0
    }
}

impl<'a> IntoIterator for &'a FileBundlesLevelled {
    type Item = SstFileBundle<'a>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let sorted_bundles = self
            .l0
            .iter()
            .filter_map(Into::<Option<SstFileBundle<'a>>>::into)
            .chain(
                self.l1
                    .iter()
                    .filter_map(Into::<Option<SstFileBundle<'a>>>::into),
            )
            .chain(
                self.l2
                    .iter()
                    .filter_map(Into::<Option<SstFileBundle<'a>>>::into),
            )
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
    main_data_file_path: PathBuf,
    index_file_path: PathBuf,
    bloom_filter_file_path: PathBuf,
    compacted: bool,
    level: Level,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Level {
    L0,
    L1,
    L2,
}

impl FileBundle {
    pub(crate) fn main_data_file_path(&self) -> &Path {
        &self.main_data_file_path
    }
}

impl<'a> From<&'a FileBundle> for Option<SstFileBundle<'a>> {
    fn from(value: &'a FileBundle) -> Self {
        if value.compacted {
            None
        } else {
            Some(SstFileBundle {
                main_data_file_path: &value.main_data_file_path,
                index_file_path: &value.index_file_path,
                bloom_filter_file_path: &value.bloom_filter_file_path,
            })
        }
    }
}

impl From<UncommittedFileBundle> for FileBundle {
    fn from(value: UncommittedFileBundle) -> Self {
        Self {
            main_data_file_path: value.0.main_data_file_path,
            index_file_path: value.0.index_file_path,
            bloom_filter_file_path: value.0.bloom_filter_file_path,
            compacted: false,
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
            main_data_file_path,
            index_file_path,
            bloom_filter_file_path,
            compacted: false,
            level,
        };
        UncommittedFileBundle(bundle)
    }

    async fn commit_file_path_bundle(
        &self,
        uncommitted_bundle: UncommittedFileBundle,
    ) -> ShouldCompact {
        let mut lock = self.0.write().await;
        let number_of_bundles_in_committed_level = match uncommitted_bundle.0.level {
            Level::L0 => {
                lock.l0.push_front(uncommitted_bundle.into_inner());
                lock.l0.len()
            }
            Level::L1 => {
                lock.l1.push_front(uncommitted_bundle.into_inner());
                lock.l1.len()
            }
            Level::L2 => {
                lock.l2.push_front(uncommitted_bundle.into_inner());
                lock.l2.len()
            }
        };

        if number_of_bundles_in_committed_level >= LEVEL_COMPACTION_THRESHOLD {
            ShouldCompact::Yes
        } else {
            ShouldCompact::No
        }
    }
}
