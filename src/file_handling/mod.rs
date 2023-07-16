use crate::file_handling::file_bundle::FileBundles;
use crate::file_handling::flushing::flush;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

mod compaction;
mod file_bundle;
mod flushing;

use crate::memtable::MemTable;
pub(crate) use file_bundle::SstFileBundle;

#[async_trait]
pub(crate) trait FileHandling {
    async fn flush(&mut self, data: MemTable) -> Result<()>;

    fn file_path_bundles(&self) -> &FileBundles;
}

#[derive(Debug)]
struct FlushData {
    data: MemTable,
    response_channel: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub(crate) struct SstFileHandler {
    file_bundles: FileBundles,
    flush_sender: mpsc::Sender<FlushData>,
}

impl SstFileHandler {
    pub(crate) fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
        P: Into<PathBuf>,
    {
        let file_bundles = FileBundles::new(path.into());
        let file_bundles_clone = file_bundles.clone();
        let (flush_tx, mut flush_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(flush_data) = flush_rx.recv().await {
                let FlushData {
                    data,
                    response_channel: tx,
                } = flush_data;
                let _ = flush(data, file_bundles_clone.clone()).await;
                let _ = tx.send(Ok(()));
            }
        });

        Self {
            file_bundles,
            flush_sender: flush_tx,
        }
    }
}

#[async_trait]
impl FileHandling for SstFileHandler {
    async fn flush(&mut self, data: MemTable) -> Result<()> {
        // TODO add oneshot channel and await reply
        let (tx, rx) = oneshot::channel::<Result<()>>();
        let flush_data = FlushData {
            data,
            response_channel: tx,
        };
        self.flush_sender.send(flush_data).await?;
        let _ = rx.await?;
        Ok(())
    }

    fn file_path_bundles(&self) -> &FileBundles {
        &self.file_bundles
    }
}
