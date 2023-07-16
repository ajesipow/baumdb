// use async_trait::async_trait;
// use crate::file_handling::{SstFileHandler};
//
// #[async_trait]
// pub(super) trait Compaction {
//     async fn compact(&mut self);
// }
//
//
// #[async_trait]
// impl Compaction for SstFileHandler {
//     async fn compact(&mut self) {
//         // TODO start compaction on level 0
//         // TODO insert new file at level 1
//         // TODO delete compacted files
//         // repeat
//         // TODO: is vecdeque the best choice here?
//
//     }
// }
