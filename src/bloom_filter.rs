use crate::file_handling::DataHandling;
use anyhow::Error;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub(crate) trait BloomFilter {
    fn add_key(&mut self, key: &str);

    fn may_contain_key(&self, key: &str) -> bool;
}

#[derive(Debug)]
pub(crate) struct DefaultBloomFilter {
    filter: Vec<u8>,
    hasher: BloomHasher,
}

impl DefaultBloomFilter {
    pub(crate) fn new(size: usize, n_hashes: u8) -> Self {
        Self {
            filter: vec![0; size],
            hasher: BloomHasher { size, n_hashes },
        }
    }
}

#[async_trait]
impl DataHandling for DefaultBloomFilter {
    async fn try_from_file<P>(path: P) -> Result<DefaultBloomFilter>
    where
        Self: Sized,
        P: AsRef<Path>,
        P: Into<PathBuf>,
        P: Send,
    {
        let mut bloom_filter_file = File::open(path).await?;
        let mut bloom_filter_bytes = Vec::<u8>::new();
        bloom_filter_file
            .read_to_end(&mut bloom_filter_bytes)
            .await?;
        DefaultBloomFilter::try_from(bloom_filter_bytes)
    }
}

impl From<DefaultBloomFilter> for Vec<u8> {
    fn from(mut value: DefaultBloomFilter) -> Self {
        value.filter.push(value.hasher.n_hashes);
        value.filter
    }
}

impl TryFrom<Vec<u8>> for DefaultBloomFilter {
    type Error = Error;

    fn try_from(mut bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < 2 {
            return Err(anyhow!("Bytes for bloom filter construction too short."));
        }
        let n_hashes = bytes.remove(bytes.len() - 1);
        let size = bytes.len();
        Ok(Self {
            filter: bytes,
            hasher: BloomHasher { size, n_hashes },
        })
    }
}

impl Default for DefaultBloomFilter {
    fn default() -> Self {
        Self::new(65536, 5)
    }
}

impl BloomFilter for DefaultBloomFilter {
    fn add_key(&mut self, key: &str) {
        let bloom_filter_indices = self.hasher.hash_key(key);
        for idx in bloom_filter_indices {
            self.filter[idx] = 1;
        }
    }

    fn may_contain_key(&self, key: &str) -> bool {
        let bloom_filter_indices = self.hasher.hash_key(key);
        bloom_filter_indices
            .into_iter()
            .all(|idx| self.filter[idx] == 1)
    }
}

trait BloomHash {
    fn hash_key(&self, key: &str) -> Vec<usize>;
}

#[derive(Debug)]
struct BloomHasher {
    size: usize,
    n_hashes: u8,
}

impl BloomHash for BloomHasher {
    fn hash_key(&self, key: &str) -> Vec<usize> {
        let mut indices = vec![0; self.n_hashes as usize];
        let mut hasher = DefaultHasher::new();
        hasher.write(key.as_bytes());
        for idx in 0..self.n_hashes {
            hasher.write_u8(idx);
            indices.push(hasher.finish() as usize % self.size)
        }
        indices
    }
}
