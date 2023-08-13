use crate::file_handling::file_bundle::{FileBundleHandle, Level, ShouldCompact};
use crate::serialization::{Serialize, SerializedTableData};
use anyhow::Result;
use std::fmt::Debug;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub(super) async fn flush<S, B>(data: S, handler: B, level: Level) -> Result<ShouldCompact>
where
    S: Serialize,
    S: Send,
    S: Debug,
    B: FileBundleHandle,
{
    let SerializedTableData {
        main_data,
        offsets,
        bloom_filter,
    } = data.serialize()?;
    let uncommited_bundle = handler.new_file_bundle(level).await;
    let mut main_data_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(uncommited_bundle.main_data_file_path())
        .await?;
    main_data_file.write_all(&main_data).await?;
    let mut index_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(uncommited_bundle.index_file_path())
        .await?;
    index_file.write_all(&offsets).await?;
    let mut bloom_filter_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(uncommited_bundle.bloom_filter_file_path())
        .await?;
    let bloom_bytes: Vec<u8> = bloom_filter.into();
    bloom_filter_file.write_all(&bloom_bytes).await?;

    // We can only commit and thus make visible the files after they were successfully written
    let should_compact = handler.commit_file_bundle(uncommited_bundle).await;
    Ok(should_compact)
}
