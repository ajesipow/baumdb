use baumdb::BaumDb;
use baumdb::DB;

#[tokio::test]
async fn test_basic_ops() {
    let mut db = BaumDb::new("./logs", 2).await;

    let key = "foo";
    let value = "value";
    db.put(key.to_string(), value.to_string()).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert_eq!(returned_value.as_deref(), Some(value));

    db.delete(key).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert!(returned_value.is_none());
}

#[tokio::test]
async fn test_basic_ops_with_many_keys() {
    let mut db = BaumDb::new("./logs", 2).await;

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
}
