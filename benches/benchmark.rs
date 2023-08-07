use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
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

async fn put_and_get_one_million() {
    let path = PathBuf::from(format!("{TEST_LOG_PATH}/{:?}", Uuid::new_v4()));
    let _ = create_dir_all(&path).await;
    let mut db = BaumDb::new(&path, 100_000).await;
    for i in 0..1_000_000 {
        db.put(i.to_string(), "MyValue".to_string()).await.unwrap();
    }
    for i in 0..1_000_000 {
        db.get(&i.to_string()).await.unwrap();
    }
}

async fn random_access() {
    let mut rng = ChaCha8Rng::seed_from_u64(2);
    let keys: Vec<String> = (0..1000)
        .map(|_| {
            let key_len = rng.gen_range(5..=32);
            Alphanumeric.sample_string(&mut rng, key_len)
        })
        .collect();

    let values: Vec<String> = (0..1000)
        .map(|_| {
            let value_len = rng.gen_range(32..=256);
            Alphanumeric.sample_string(&mut rng, value_len)
        })
        .collect();
    let path = PathBuf::from(format!("{TEST_LOG_PATH}/{:?}", Uuid::new_v4()));
    let _ = create_dir_all(&path).await;
    let mut db = BaumDb::new(&path, 100).await;

    for _ in 0..10_000 {
        let key_idx = rng.gen_range(0..keys.len());
        match rng.gen_range(0..=2) {
            0 => {
                let value_idx = rng.gen_range(0..values.len());
                db.put(keys[key_idx].clone(), values[value_idx].clone())
                    .await
                    .unwrap();
            }
            1 => {
                db.get(&keys[key_idx]).await.unwrap();
            }
            _ => {
                db.delete(&keys[key_idx]).await.unwrap();
            }
        }
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
        b.to_async(&rt).iter(put_and_get_one_million)
    });
    c.bench_function("random access", |b| b.to_async(&rt).iter(random_access));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
