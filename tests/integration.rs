use baumdb::BaumDb;
use baumdb::DB;
use std::collections::HashMap;
use std::fs::read_dir;
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

    let key_values = vec![
        ("Aa", "1"),
        ("Bbb", "2"),
        ("Cc", "3"),
        ("Ddddd", "3"),
        ("Eeeee", "4"),
    ];
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

    for (key, _) in key_values {
        let returned_value = db.get(key).await.unwrap();
        assert!(returned_value.is_none());
    }
    test_clean_up(&path).await;
}

#[tokio::test]
async fn test_updating_a_key_works() {
    let path = prepare_test().await;
    let mut db = BaumDb::new(&path, 128).await;

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

    test_clean_up(&path).await;
}

#[tokio::test]
async fn test_bloom_filter_is_used() {
    let path = prepare_test().await;
    let mut db = BaumDb::new(&path, 2).await;

    let key_values = vec![
        ("Aa", "1"),
        ("Bbb", "2"),
        ("Cc", "3"),
        ("Dd", "4"),
        ("Ee", "5"),
        ("Ff", "6"),
        ("Gg", "7"),
        ("Hh", "8"),
    ];
    for (key, value) in key_values.iter() {
        db.put(key.to_string(), value.to_string()).await.unwrap();
    }

    db.get("Aa").await.unwrap();
    let files: HashMap<_, _> = read_dir(&path)
        .unwrap()
        .flatten()
        .map(|entry| (entry.file_name().into_string().unwrap(), entry))
        .collect();

    for idx in 0..3 {
        let bloom = files.get(&format!("L0-bloom-{idx}.db")).unwrap();
        // Every bloom file gets accessed again after creation as the key looked for above is in the oldest file
        assert!(
            bloom.metadata().unwrap().accessed().unwrap()
                > bloom.metadata().unwrap().created().unwrap()
        );

        let index = files.get(&format!("L0-index-{idx}.db")).unwrap();
        let data = files.get(&format!("L0-data-{idx}.db")).unwrap();
        if idx == 0 {
            // The index and data files are accessed again after creation as the value is found in the oldest file
            assert!(
                index.metadata().unwrap().accessed().unwrap()
                    > index.metadata().unwrap().created().unwrap()
            );
            assert!(
                data.metadata().unwrap().accessed().unwrap()
                    > data.metadata().unwrap().created().unwrap()
            );
        } else {
            // The other index and data files that don't contain the value are not touched
            assert_eq!(
                index.metadata().unwrap().created().unwrap(),
                index.metadata().unwrap().accessed().unwrap()
            );
            assert_eq!(
                data.metadata().unwrap().created().unwrap(),
                data.metadata().unwrap().accessed().unwrap()
            );
        }
    }

    test_clean_up(&path).await;
}
