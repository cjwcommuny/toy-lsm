mod common;

use crate::common::{build_rocks_db, Database};
use common::{iterate, populate, randread, remove_files};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::{
    ops::Add,
    time::{Duration, Instant},
};
use tempfile::TempDir;

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 160_000;
const CHUNK_SIZE: u64 = 10_000;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;

fn bench<D: Database>(c: &mut Criterion, name: &str, build_db: impl Fn(&TempDir) -> Arc<D>) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("{}-bench-small-value", name))
        .tempdir()
        .unwrap();
    let dir_path = dir.path();

    c.bench_function(
        &format!("{} sequentially populate small value", name),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::new(0, 0);

                (0..iters).for_each(|_| {
                    remove_files(dir_path);
                    let db = build_db(&dir);

                    let now = Instant::now();
                    populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, SMALL_VALUE_SIZE, true);
                    total = total.add(now.elapsed());
                });

                total
            });
        },
    );

    c.bench_function(&format!("{} randomly populate small value", name), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_db(&dir);

                let now = Instant::now();
                populate(
                    db,
                    KEY_NUMS,
                    CHUNK_SIZE,
                    BATCH_SIZE,
                    SMALL_VALUE_SIZE,
                    false,
                );
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = build_db(&dir);

    c.bench_function(&format!("{} randread small value", name), |b| {
        b.iter(|| {
            randread(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.bench_function(&format!("{} iterate small value", name), |b| {
        b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE));
    });

    dir.close().unwrap();
    let dir = tempfile::Builder::new()
        .prefix(&format!("{}-bench-large-value", name))
        .tempdir()
        .unwrap();
    let dir_path = dir.path();

    c.bench_function("rocks sequentially populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_db(&dir);

                let now = Instant::now();
                populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, LARGE_VALUE_SIZE, true);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    c.bench_function(&format!("{} randomly populate large value", name), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_db(&dir);

                let now = Instant::now();
                populate(
                    db,
                    KEY_NUMS,
                    CHUNK_SIZE,
                    BATCH_SIZE,
                    LARGE_VALUE_SIZE,
                    false,
                );
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = build_db(&dir);

    c.bench_function(&format!("{} randread large value", name), |b| {
        b.iter(|| {
            randread(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    c.bench_function(&format!("{} iterate large value", name), |b| {
        b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE));
    });

    dir.close().unwrap();
}

fn bench_rocks(c: &mut Criterion) {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    
    bench(c, "rocks", |dir| build_rocks_db(&opts, dir));
}

criterion_group! {
  name = benches_agate_rocks;
  config = Criterion::default();
  targets = bench_rocks
}

criterion_main!(benches_agate_rocks);
