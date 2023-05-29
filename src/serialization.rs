use crate::memtable::{MemTable, MemValue};
use anyhow::Result;

pub(crate) trait Serialize {
    fn serialize(self) -> Vec<u8>;
}

pub(crate) trait Deserialize: Sized {
    fn deserialize(&self) -> Result<Self>;
}

impl Serialize for MemTable {
    fn serialize(self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.len());

        for (key, value) in self {
            // Encode the key length and value length first for easier parsing
            let key_len = key.len() as u64;
            buffer.extend(key_len.to_be_bytes());
            buffer.extend(key.as_bytes());
            match value {
                MemValue::Delete => {
                    buffer.push(0);
                }
                MemValue::Put(value_str) => {
                    buffer.push(1);
                    buffer.extend(value_str.len().to_be_bytes());
                    buffer.extend(value_str.as_bytes());
                }
            }
        }
        buffer
    }
}
