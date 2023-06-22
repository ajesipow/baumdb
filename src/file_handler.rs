use crate::serialization::{Serialize, SerializedTableData};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub(crate) trait FileHandling {
    async fn flush<S>(&mut self, data: S) -> Result<()>
    where
        S: Serialize,
        S: Send;
}

#[derive(Debug)]
pub(crate) struct SstFileHandler {
    sst_dir_path: PathBuf,
    file_paths: VecDeque<SstFileBundle>,
}

#[derive(Debug, Clone)]
pub(crate) struct SstFileBundle {
    pub main_data_file_path: PathBuf,
    pub index_file_path: PathBuf,
}

impl SstFileHandler {
    pub(crate) fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
        P: Into<PathBuf>,
    {
        Self {
            sst_dir_path: path.into(),
            file_paths: VecDeque::new(),
        }
    }

    /// Returns the SStable file paths in the order of most recent to least recent.
    pub(crate) fn file_path_bundles(&self) -> &VecDeque<SstFileBundle> {
        &self.file_paths
    }

    fn new_file_path_bundle(&mut self) -> &SstFileBundle {
        let main_data_file_name = PathBuf::from(&format!("L0-data-{}.db", self.file_paths.len()));
        let index_file_name = PathBuf::from(&format!("L0-index-{}.db", self.file_paths.len()));
        let main_data_file_path = Path::join(&self.sst_dir_path, main_data_file_name);
        let index_file_path = Path::join(&self.sst_dir_path, index_file_name);

        self.file_paths.push_front(SstFileBundle {
            main_data_file_path,
            index_file_path,
        });
        self.file_paths.front().unwrap()
    }
}

#[async_trait]
impl FileHandling for SstFileHandler {
    async fn flush<S>(&mut self, data: S) -> Result<()>
    where
        S: Serialize,
        S: Send,
    {
        let SerializedTableData { main_data, offsets } = data.serialize()?;
        let SstFileBundle {
            main_data_file_path,
            index_file_path,
        } = self.new_file_path_bundle();
        let mut main_data_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(main_data_file_path)
            .await?;
        main_data_file.write_all(&main_data).await?;
        let mut index_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(index_file_path)
            .await?;
        index_file.write_all(&offsets).await?;
        Ok(())
    }
}
