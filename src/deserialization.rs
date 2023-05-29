use crate::memtable::MemValue;
use anyhow::{anyhow, Result};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

pub(crate) async fn read_value<R>(buffer: &mut BufReader<R>) -> Result<(String, MemValue)>
where
    R: AsyncRead,
    R: Unpin,
{
    let key_len = buffer.read_u64().await? as usize;
    let mut buf: Vec<u8> = vec![0; key_len];
    let _n = buffer.read_exact(&mut buf).await?;
    let key = String::from_utf8(buf)?;
    let value_type = buffer.read_u8().await?;
    match value_type {
        0 => Ok((key, MemValue::Delete)),
        1 => {
            let value_len = buffer.read_u64().await? as usize;
            let mut buf: Vec<u8> = vec![0; value_len];
            let _n = buffer.read_exact(&mut buf).await?;
            let value = String::from_utf8(buf)?;
            Ok((key, MemValue::Put(value)))
        }
        _ => Err(anyhow!("Wrong value type byte: {value_type}")),
    }
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

        let mut buffer = BufReader::new(bytes.as_slice());
        let (extracted_key, extracted_value) = read_value(&mut buffer).await.unwrap();
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

        let mut buffer = BufReader::new(bytes.as_slice());
        let (extracted_key, extracted_value) = read_value(&mut buffer).await.unwrap();
        assert_eq!(&extracted_key, key);
        assert_eq!(extracted_value, value);
    }
}
