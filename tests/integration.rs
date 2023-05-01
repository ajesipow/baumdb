use baumdb::BaumDb;
use baumdb::DB;

#[tokio::test]
async fn test_basic_ops() {
    let mut db = BaumDb::new("./logs").await;

    let key = "foo";
    let value = "value";
    db.put(key.to_string(), value.to_string()).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert_eq!(returned_value.as_deref(), Some(value));

    db.delete(key).await.unwrap();

    let returned_value = db.get(key).await.unwrap();
    assert!(returned_value.is_none());
}
