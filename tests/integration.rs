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

#[tokio::test]
async fn test_updating_a_key_works() {
    let path = prepare_test().await;
    let mut db = BaumDb::new(&path, 5).await;

    let key = "SomeKey".to_string();
    let value = "1".to_string();

    db.put(key.clone(), value.clone()).await.unwrap();
    let returned_value = db.get(&key).await.unwrap();
    assert_eq!(returned_value, Some(value));

    // Add some random values into the db to ensure data has been flushed to disk
    for i in 0..1000 {
        db.put(i.to_string(), "SomeValue".to_string())
            .await
            .unwrap();
    }

    let value = "2".to_string();

    db.put(key.clone(), value.clone()).await.unwrap();

    // Add some random values into the db to ensure data has been flushed to disk
    for i in 1000..2000 {
        db.put(i.to_string(), "SomeValue".to_string())
            .await
            .unwrap();
    }

    let returned_value = db.get(&key).await.unwrap();
    assert_eq!(returned_value, Some(value));

    // test_clean_up(&path).await;
}
