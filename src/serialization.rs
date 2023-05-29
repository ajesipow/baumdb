use crate::memtable::{MemTable, MemValue};

pub(crate) trait Serialize {
    fn serialize(self) -> Vec<u8>;
}

impl Serialize for MemTable {
    fn serialize(self) -> Vec<u8> {
        self.into_iter().fold(vec![], |mut buf, (key, value)| {
            // Encode the key length and value length first for easier parsing
            let key_len = key.len() as u64;
            buf.extend(key_len.to_be_bytes());
            buf.extend(key.as_bytes());
            match value {
                MemValue::Delete => {
                    buf.push(0);
                }
                MemValue::Put(value_str) => {
                    buf.push(1);
                    buf.extend(value_str.len().to_be_bytes());
                    buf.extend(value_str.as_bytes());
                }
            };
            buf
        })
    }
}
