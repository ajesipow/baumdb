use baumdb::BaumDb;
use baumdb::DB;
use std::path::{Path, PathBuf};
use tokio::fs::{create_dir_all, remove_dir_all};
use uuid::Uuid;

static TEST_LOG_PATH: &str = "./test-logs";

async fn prepare_test() -> PathBuf {
    let path = PathBuf::from(format!("{TEST_LOG_PATH}/{:?}", Uuid::new_v4()));
    let _ = create_dir_all(&path).await;
    path
}

async fn test_clean_up(path: impl AsRef<Path>) {
    let _ = remove_dir_all(path).await;
}

#[tokio::test]
async fn test_basic_ops() {
    let path = prepare_test().await;
    let mut db = BaumDb::new(&path, 2).await;

    let key = "foo";
    let value = "value";
    db.put(key.to_string(), value.to_string()).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert_eq!(returned_value.as_deref(), Some(value));

    db.delete(key).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert!(returned_value.is_none());
    test_clean_up(&path).await;
}

#[tokio::test]
async fn test_basic_ops_with_many_keys() {
    let path = prepare_test().await;
    let mut db = BaumDb::new(&path, 2).await;

    let key_values = vec![("A", "1"), ("B", "2"), ("C", "3"), ("D", "3"), ("E", "4")];
    for (key, value) in key_values.iter() {
        db.put(key.to_string(), value.to_string()).await.unwrap();
    }

    // FIXME: while memtable is flushed to disk, we won't find some keys
    for (key, value) in key_values.iter() {
        let returned_value = db.get(key).await.unwrap();
        assert_eq!(returned_value.as_deref(), Some(*value));
    }

    for (key, _) in key_values.iter() {
        db.delete(key).await.unwrap();
    }

    // FIXME: we find old values here because we don't look at newest to oldest SSTables in L0
    for (key, _) in key_values {
        let returned_value = db.get(key).await.unwrap();
        assert!(returned_value.is_none());
    }
    test_clean_up(&path).await;
}
