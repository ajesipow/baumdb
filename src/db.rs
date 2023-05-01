use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait DB {
    async fn get(&self, key: &str) -> Result<Option<&String>>;

    async fn put(&mut self, key: String, value: String) -> Result<()>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}
