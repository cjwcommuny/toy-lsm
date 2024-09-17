mod common;

use crate::common::{build_rocks_db, Database, MyDbWithRuntime};
use better_mini_lsm::lsm::core::Lsm;
use better_mini_lsm::persistent::LocalFs;
use better_mini_lsm::sst::SstOptions;
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
const KEY_NUMS: u64 = 160_00;
const CHUNK_SIZE: u64 = 10_00;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;
const SAMPLE_SIZE: usize = 10;

fn bench<D: Database>(c: &mut Criterion, name: &str, build_db: impl Fn(&TempDir) -> Arc<D>) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("{}-bench-small-value", name))
        .tempdir()
        .unwrap();
    let dir_path = dir.path();
    let mut c = c.benchmark_group("group");

    c.sample_size(SAMPLE_SIZE).bench_function(
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

    c.sample_size(SAMPLE_SIZE).bench_function(&format!("{} randomly populate small value", name), |b| {
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

    c.sample_size(SAMPLE_SIZE).bench_function(&format!("{} randread small value", name), |b| {
        b.iter(|| {
            randread(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.sample_size(SAMPLE_SIZE).bench_function(&format!("{} iterate small value", name), |b| {
        b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE));
    });

    // let dir = tempfile::Builder::new()
    //     .prefix(&format!("{}-bench-large-value", name))
    //     .tempdir()
    //     .unwrap();
    // let dir_path = dir.path();
    //
    // c.bench_function("rocks sequentially populate large value", |b| {
    //     b.iter_custom(|iters| {
    //         let mut total = Duration::new(0, 0);
    //
    //         (0..iters).for_each(|_| {
    //             remove_files(dir_path);
    //             let db = build_db(&dir);
    //
    //             let now = Instant::now();
    //             populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, LARGE_VALUE_SIZE, true);
    //             total = total.add(now.elapsed());
    //         });
    //
    //         total
    //     });
    // });

    // c.bench_function(&format!("{} randomly populate large value", name), |b| {
    //     b.iter_custom(|iters| {
    //         let mut total = Duration::new(0, 0);
    //
    //         (0..iters).for_each(|_| {
    //             remove_files(dir_path);
    //             let db = build_db(&dir);
    //
    //             let now = Instant::now();
    //             populate(
    //                 db,
    //                 KEY_NUMS,
    //                 CHUNK_SIZE,
    //                 BATCH_SIZE,
    //                 LARGE_VALUE_SIZE,
    //                 false,
    //             );
    //             total = total.add(now.elapsed());
    //         });
    //
    //         total
    //     });
    // });
    //
    // let db = build_db(&dir);
    //
    // c.bench_function(&format!("{} randread large value", name), |b| {
    //     b.iter(|| {
    //         randread(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
    //     });
    // });
    //
    // c.bench_function(&format!("{} iterate large value", name), |b| {
    //     b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE));
    // });

    dir.close().unwrap();
}

fn bench_rocks(c: &mut Criterion) {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::None);

    bench(c, "rocks", |dir| build_rocks_db(&opts, dir));
}

fn bench_mydb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let runtime = Arc::new(runtime);
    let options = SstOptions::builder()
        .target_sst_size(1024 * 1024 * 2)
        .block_size(4096)
        .num_memtable_limit(1000)
        .compaction_option(Default::default())
        .enable_wal(false)
        .enable_mvcc(true)
        .build();

    bench(c, "mydb", |dir| {
        let options = options.clone();
        let path = Arc::new(dir.path().to_path_buf());
        runtime.block_on(async {
            let persistent = LocalFs::new(path);
            let db = Lsm::new(options, persistent).await.unwrap();
            let db = MyDbWithRuntime::new(db, runtime.clone());
            Arc::new(db)
        })
    })
}

criterion_group! {
  name = bench_against_rocks;
  config = Criterion::default();
  targets = bench_mydb
}

criterion_main!(bench_against_rocks);
