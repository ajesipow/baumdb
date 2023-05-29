use crate::serialization::Serialize;
use anyhow::Result;
use async_trait::async_trait;
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
    file_paths: Vec<PathBuf>,
}

impl SstFileHandler {
    pub(crate) fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
        P: Into<PathBuf>,
    {
        Self {
            sst_dir_path: path.into(),
            file_paths: Vec::new(),
        }
    }

    /// Returns the sstable file paths in the order of most recent to least recent.
    pub(crate) fn file_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.file_paths.clone();
        paths.reverse();
        paths
    }

    fn new_file_path(&mut self) -> &Path {
        let new_file_name = PathBuf::from(&format!("L0-{}.baum", self.file_paths.len()));
        let path = Path::join(&self.sst_dir_path, new_file_name);
        self.file_paths.push(path);
        self.file_paths.last().unwrap()
    }
}

#[async_trait]
impl FileHandling for SstFileHandler {
    async fn flush<S>(&mut self, data: S) -> Result<()>
    where
        S: Serialize,
        S: Send,
    {
        let blob = data.serialize();
        let file_path = self.new_file_path();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(file_path)
            .await?;
        file.write_all(&blob).await?;

        Ok(())
    }
}
