use std::io::Cursor;
use std::io::Read;

use anyhow::anyhow;
use anyhow::Result;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;

use crate::memtable::MemValue;

// FIXME: make this nicer?
pub(crate) fn read_value(reader: &mut Cursor<Vec<u8>>) -> Result<(String, MemValue)> {
    let key_len = reader.read_u64::<BigEndian>()? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    reader.read_exact(&mut buf)?;
    let key = String::from_utf8(buf)?;
    let value_type = reader.read_u8()?;
    match value_type {
        0 => Ok((key, MemValue::Delete)),
        1 => {
            let value_len = reader.read_u64::<BigEndian>()? as usize;
            let mut buf: Vec<u8> = vec![0; value_len];
            reader.read_exact(&mut buf)?;
            let value = String::from_utf8(buf)?;
            Ok((key, MemValue::Put(value)))
        }
        _ => Err(anyhow!("Wrong value type byte: {value_type}")),
    }
}

// FIXME: make this nicer?
pub(crate) fn read_key_offset(buffer: &mut Cursor<Vec<u8>>) -> Result<(String, u64)> {
    let key_len = buffer.read_u64::<BigEndian>()? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    buffer.read_exact(&mut buf)?;
    let key = String::from_utf8(buf)?;
    let offset = buffer.read_u64::<BigEndian>()?;
    Ok((key, offset))
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
        let (extracted_key, extracted_value) = read_value(&mut cursor).unwrap();
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
        let (extracted_key, extracted_value) = read_value(&mut cursor).unwrap();
        assert_eq!(&extracted_key, key);
        assert_eq!(extracted_value, value);
    }
}
