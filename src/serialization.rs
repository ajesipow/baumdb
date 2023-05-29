use crate::memtable::{MemTable, MemValue};

#[derive(Debug, Default)]
pub(crate) struct SerializedTableData {
    pub main_data: Vec<u8>,
    pub offsets: Vec<u8>,
}

pub(crate) trait Serialize {
    fn serialize(self) -> SerializedTableData;
}

#[derive(Debug, Default)]
struct SerializedFoldState {
    table_data: SerializedTableData,
    offset_counter: usize,
}

impl Serialize for MemTable {
    fn serialize(self) -> SerializedTableData {
        let fold_result =
            self.into_iter()
                .fold(SerializedFoldState::default(), |mut buf, (key, value)| {
                    // Encode the key length and value length first for easier parsing
                    let key_len = key.len() as u64;
                    let key_len_bytes = key_len.to_be_bytes();
                    let key_bytes = key.as_bytes();
                    buf.table_data.main_data.extend(key_len_bytes);
                    buf.table_data.main_data.extend(key_bytes);

                    buf.table_data.offsets.extend(key_len_bytes);
                    buf.table_data.offsets.extend(key_bytes);
                    buf.table_data
                        .offsets
                        .extend((buf.offset_counter as u64).to_be_bytes());
                    let value_bytes_len = match value {
                        MemValue::Delete => {
                            buf.table_data.main_data.push(0);
                            // Just the single byte from the delete flag
                            1
                        }
                        MemValue::Put(value_str) => {
                            buf.table_data.main_data.push(1);
                            buf.table_data
                                .main_data
                                .extend(value_str.len().to_be_bytes());
                            buf.table_data.main_data.extend(value_str.as_bytes());
                            // The single byte from the put flag + 8 bytes of the key length + the length of the value
                            9 + value_str.len()
                        }
                    };
                    // The encoded key length + the key length itself + the value encoded + the value itself
                    let offset = 8 + key.len() + value_bytes_len;
                    buf.offset_counter += offset;
                    buf
                });
        fold_result.table_data
    }
}
