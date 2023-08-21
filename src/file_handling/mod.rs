use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::file_handling::file_bundle::FileBundles;
use crate::file_handling::file_bundle::Level;
use crate::file_handling::file_bundle::ShouldCompact;
use crate::file_handling::flushing::flush;

mod compaction;
mod file_bundle;
mod flushing;

pub(crate) use file_bundle::SstFileBundle;

use crate::file_handling::compaction::Compaction;
use crate::memtable::MemTable;

#[async_trait]
pub(crate) trait FileHandling {
    async fn flush(
        &mut self,
        data: MemTable,
    ) -> Result<()>;

    fn file_bundles(&self) -> &FileBundles;
}

#[async_trait]
pub(crate) trait DataHandling {
    async fn try_from_file<P>(path: P) -> Result<Self>
    where
        Self: Sized,
        P: AsRef<Path>,
        P: Into<PathBuf>,
        P: Send;
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
        let file_bundles_clone_1 = file_bundles.clone();
        let file_bundles_clone_2 = file_bundles.clone();
        let (flush_tx, mut flush_rx) = mpsc::channel::<FlushData>(1);
        // TODO investigate impact of buffer size here
        let (compaction_tx, mut compaction_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            while compaction_rx.recv().await.is_some() {
                // TODO handle errors
                let _ = file_bundles_clone_1.compact().await;
            }
        });

        tokio::spawn(async move {
            while let Some(flush_data) = flush_rx.recv().await {
                let FlushData {
                    data,
                    response_channel: tx,
                } = flush_data;
                let flush_result = flush(data, file_bundles_clone_2.clone(), Level::L0).await;
                let result = match flush_result {
                    Ok(should_compact) => {
                        if should_compact == ShouldCompact::Yes {
                            let _ = compaction_tx.send(()).await;
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = tx.send(result);
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
    async fn flush(
        &mut self,
        data: MemTable,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel::<Result<()>>();
        let flush_data = FlushData {
            data,
            response_channel: tx,
        };
        self.flush_sender.send(flush_data).await?;
        let _ = rx.await?;
        Ok(())
    }

    fn file_bundles(&self) -> &FileBundles {
        &self.file_bundles
    }
}
