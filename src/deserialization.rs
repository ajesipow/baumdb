use std::io::Cursor;
use std::io::Read;

use anyhow::anyhow;
use anyhow::Result;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;

use crate::memtable::MemValue;

/// A helper struct defining a simple key-value pair.
#[derive(Debug)]
pub(crate) struct KeyValue {
    pub key: String,
    pub value: MemValue,
}

/// A helper struct defining a the offset a key can be found at in a file.
#[derive(Debug)]
pub(crate) struct KeyOffset {
    pub key: String,
    pub offset: u64,
}

/// Reads a key, value pair from the reader.
pub(crate) fn read_key_value(reader: &mut Cursor<Vec<u8>>) -> Result<KeyValue> {
    let key_len = reader.read_u64::<BigEndian>()? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    reader.read_exact(&mut buf)?;
    let key = String::from_utf8(buf)?;
    let value_type = reader.read_u8()?;
    match value_type {
        0 => Ok(KeyValue {
            key,
            value: MemValue::Delete,
        }),
        1 => {
            let value_len = reader.read_u64::<BigEndian>()? as usize;
            let mut buf: Vec<u8> = vec![0; value_len];
            reader.read_exact(&mut buf)?;
            let value = String::from_utf8(buf)?;
            Ok(KeyValue {
                key,
                value: MemValue::Put(value),
            })
        }
        _ => Err(anyhow!("Wrong value type byte: {value_type}")),
    }
}

pub(crate) fn read_key_offset(buffer: &mut Cursor<Vec<u8>>) -> Result<KeyOffset> {
    let key_len = buffer.read_u64::<BigEndian>()? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    buffer.read_exact(&mut buf)?;
    let key = String::from_utf8(buf)?;
    let offset = buffer.read_u64::<BigEndian>()?;
    Ok(KeyOffset { key, offset })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_value_works_for_deleted_key() {
        let key = "foo";
        let value = MemValue::Delete;

        let mut bytes = vec![];
        bytes.extend(key.len().to_be_bytes());
        bytes.extend(key.as_bytes());
        // Delete is encoded as 0
        bytes.push(0);

        let mut cursor = Cursor::new(bytes);
        let KeyValue {
            key: extracted_key,
            value: extracted_value,
        } = read_key_value(&mut cursor).unwrap();
        assert_eq!(&extracted_key, key);
        assert_eq!(extracted_value, value);
    }

    #[tokio::test]
    async fn read_value_works_for_put_key() {
        let key = "foo";
        let value_inner = "bar".to_string();
        let value = MemValue::Put(value_inner.clone());

        let mut bytes = vec![];
        bytes.extend(key.len().to_be_bytes());
        bytes.extend(key.as_bytes());
        // Put is encoded as 1
        bytes.push(1);
        bytes.extend(value_inner.len().to_be_bytes());
        bytes.extend(value_inner.as_bytes());

        let mut cursor = Cursor::new(bytes);
        let KeyValue {
            key: extracted_key,
            value: extracted_value,
        } = read_key_value(&mut cursor).unwrap();
        assert_eq!(&extracted_key, key);
        assert_eq!(extracted_value, value);
    }
}
