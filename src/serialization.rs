use crate::memtable::{MemTable, MemValue};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use std::mem;

#[derive(Debug, Default)]
pub(crate) struct SerializedTableData {
    pub main_data: Vec<u8>,
    pub offsets: Vec<u8>,
}

pub(crate) trait Serialize {
    fn serialize(self) -> SerializedTableData;
}

#[derive(Debug)]
struct SerializedFoldState {
    table_data: SerializedTableData,
    encoder: GzEncoder<Vec<u8>>,
    encoded_bytes: usize,
    offset_counter: usize,
}

impl SerializedFoldState {
    fn new() -> Self {
        Self {
            table_data: Default::default(),
            encoder: GzEncoder::new(Vec::new(), Compression::default()),
            encoded_bytes: 0,
            offset_counter: 0,
        }
    }
}

impl Serialize for MemTable {
    fn serialize(self) -> SerializedTableData {
        let n_elements = self.len();
        let fold_result = self.into_iter().enumerate().fold(
            SerializedFoldState::new(),
            |mut state, (idx, (key, value))| {
                // Encode the key length and value length first for easier parsing
                let key_len = key.len() as u64;
                let key_len_bytes = key_len.to_be_bytes();
                let key_bytes = key.as_bytes();

                if state.encoded_bytes == 0 {
                    state.table_data.offsets.extend(&key_len_bytes);
                    state.table_data.offsets.extend(key_bytes);
                }

                // FIXME handle errors
                state.encoded_bytes += state.encoder.write(&key_len_bytes).unwrap();
                state.encoded_bytes += state.encoder.write(key_bytes).unwrap();

                match value {
                    MemValue::Delete => {
                        state.encoded_bytes += state.encoder.write(&[0]).unwrap();
                    }
                    MemValue::Put(value_str) => {
                        state.encoded_bytes += state.encoder.write(&[1]).unwrap();
                        state.encoded_bytes +=
                            state.encoder.write(&value_str.len().to_be_bytes()).unwrap();
                        state.encoded_bytes += state.encoder.write(value_str.as_bytes()).unwrap();
                    }
                };

                // Encode data above threshold or when it's the last element
                if state.encoded_bytes >= 4096 || idx == n_elements - 1 {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    mem::swap(&mut state.encoder, &mut encoder);
                    state.encoded_bytes = 0;

                    let encoded_data = encoder.finish().unwrap();
                    let encoded_len = encoded_data.len();
                    state.table_data.main_data.extend(encoded_data);
                    state
                        .table_data
                        .offsets
                        .extend((state.offset_counter as u64).to_be_bytes());
                    state.offset_counter += encoded_len
                }
                state
            },
        );
        fold_result.table_data
    }
}
