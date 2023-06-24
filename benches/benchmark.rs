use criterion::{criterion_group, criterion_main, Criterion};
use std::path::PathBuf;
use tokio::fs::create_dir_all;

use baumdb::{BaumDb, DB};
use uuid::Uuid;

static TEST_LOG_PATH: &str = "./test-logs";

async fn put_one_million() {
    let path = PathBuf::from(format!("{TEST_LOG_PATH}/{:?}", Uuid::new_v4()));
    let _ = create_dir_all(&path).await;
    let mut db = BaumDb::new(&path, 100_000).await;
    for i in 0..1_000_000 {
        db.put(i.to_string(), "MyValue".to_string()).await.unwrap();
    }
}

async fn put_and_get_one_thousand() {
    let path = PathBuf::from(format!("{TEST_LOG_PATH}/{:?}", Uuid::new_v4()));
    let _ = create_dir_all(&path).await;
    let mut db = BaumDb::new(&path, 100_000).await;
    for i in 0..100_000 {
        db.put(i.to_string(), "MyValue".to_string()).await.unwrap();
    }
    for i in 0..100_000 {
        db.get(&i.to_string()).await.unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("put one million entries into DB", |b| {
        b.to_async(&rt).iter(put_one_million)
    });
    c.bench_function("put and get one thousand entries into DB", |b| {
        b.to_async(&rt).iter(put_and_get_one_thousand)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
