mod common;

use std::{
    ops::Add,
    time::{Duration, Instant},
};

use crate::common::build_rocks_db;
use common::{remove_files, rocks_iterate, rocks_populate, rocks_randread};
use criterion::{criterion_group, criterion_main, Criterion};

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 160_000;
const CHUNK_SIZE: u64 = 10_000;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;

fn bench_rocks(c: &mut Criterion) {
    let dir = tempfile::Builder::new()
        .prefix("rocks-bench-small-value")
        .tempdir()
        .unwrap();
    let dir_path = dir.path();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::None);

    c.bench_function("rocks sequentially populate small value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_rocks_db(&opts, &dir);

                let now = Instant::now();
                rocks_populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, SMALL_VALUE_SIZE, true);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    c.bench_function("rocks randomly populate small value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_rocks_db(&opts, &dir);

                let now = Instant::now();
                rocks_populate(
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

    let db = build_rocks_db(&opts, &dir);

    c.bench_function("rocks randread small value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.bench_function("rocks iterate small value", |b| {
        b.iter(|| rocks_iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE));
    });

    dir.close().unwrap();
    let dir = tempfile::Builder::new()
        .prefix("rocks-bench-large-value")
        .tempdir()
        .unwrap();
    let dir_path = dir.path();

    c.bench_function("rocks sequentially populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_rocks_db(&opts, &dir);

                let now = Instant::now();
                rocks_populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, LARGE_VALUE_SIZE, true);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    c.bench_function("rocks randomly populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                remove_files(dir_path);
                let db = build_rocks_db(&opts, &dir);

                let now = Instant::now();
                rocks_populate(
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

    let db = build_rocks_db(&opts, &dir);

    c.bench_function("rocks randread large value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    c.bench_function("rocks iterate large value", |b| {
        b.iter(|| rocks_iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE));
    });

    dir.close().unwrap();
}

criterion_group! {
  name = benches_agate_rocks;
  config = Criterion::default();
  targets = bench_rocks
}

criterion_main!(benches_agate_rocks);
