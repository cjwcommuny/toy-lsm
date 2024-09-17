use better_mini_lsm::lsm::core::Lsm;
use better_mini_lsm::persistent::LocalFs;
use better_mini_lsm::sst::SstOptions;
use better_mini_lsm::test_utils::integration::common::{iterate, populate, randread, Database};
use better_mini_lsm::test_utils::integration::mydb::{build_sst_options, MyDbWithRuntime};
use better_mini_lsm::test_utils::integration::pair::DbPair;
use better_mini_lsm::test_utils::integration::rocksdb::{build_rocks_db, build_rocks_options};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tempfile::TempDir;

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 16_000;
const CHUNK_SIZE: u64 = 10_00;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;
const SAMPLE_SIZE: usize = 10;

fn bench<D: Database>(c: &mut Criterion, name: &str, build_db: impl Fn(&TempDir) -> Arc<D>) {
    let mut c = c.benchmark_group("group");
    c.sample_size(SAMPLE_SIZE);

    c.bench_function(format!("{} sequentially populate small value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-seq-populate-small-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);

        b.iter(|| {
            populate(
                db.clone(),
                KEY_NUMS,
                CHUNK_SIZE,
                BATCH_SIZE,
                SMALL_VALUE_SIZE,
                true,
            );
        });
    });

    c.bench_function(format!("{} randomly populate small value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-rand-populate-small-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);
        b.iter(|| {
            populate(
                db.clone(),
                KEY_NUMS,
                CHUNK_SIZE,
                BATCH_SIZE,
                SMALL_VALUE_SIZE,
                false,
            );
        });
    });

    c.bench_function(format!("{} randread small value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-rand-read-small-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            SMALL_VALUE_SIZE,
            true,
        );
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            SMALL_VALUE_SIZE,
            false,
        );
        b.iter(|| {
            randread(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.bench_function(format!("{} iterate small value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-iter-small-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            SMALL_VALUE_SIZE,
            true,
        );
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            SMALL_VALUE_SIZE,
            false,
        );
        b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE));
    });

    c.bench_function("rocks sequentially populate large value", |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-seq-populate-large-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);

        b.iter(|| {
            populate(
                db.clone(),
                KEY_NUMS,
                CHUNK_SIZE,
                BATCH_SIZE,
                LARGE_VALUE_SIZE,
                true,
            );
        });
    });

    c.bench_function(format!("{} randomly populate large value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-rand-populate-large-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);

        b.iter(|| {
            populate(
                db.clone(),
                KEY_NUMS,
                CHUNK_SIZE,
                BATCH_SIZE,
                LARGE_VALUE_SIZE,
                false,
            );
        });
    });

    c.bench_function(format!("{} randread large value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-rand-read-large-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);

        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            LARGE_VALUE_SIZE,
            true,
        );
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            LARGE_VALUE_SIZE,
            false,
        );

        b.iter(|| {
            randread(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    c.bench_function(format!("{} iterate large value", name), |b| {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{}-bench-rand-iter-large-value", name))
            .tempdir()
            .unwrap();
        let db = build_db(&dir);
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            LARGE_VALUE_SIZE,
            true,
        );
        populate(
            db.clone(),
            KEY_NUMS,
            CHUNK_SIZE,
            BATCH_SIZE,
            LARGE_VALUE_SIZE,
            false,
        );

        b.iter(|| iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE));
    });
}

fn bench_rocks(c: &mut Criterion) {
    let opts = build_rocks_options();

    bench(c, "rocks", |dir| Arc::new(build_rocks_db(&opts, dir)));
}

fn bench_mydb(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let runtime = Arc::new(runtime);
    let options = build_sst_options();

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

#[allow(dead_code)]
fn pair_test(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let runtime = Arc::new(runtime);
    let options = build_sst_options();
    let opts = build_rocks_options();

    bench(c, "pair_db", |dir| {
        let options = options.clone();
        // todo: create dir 应该由 db 完成
        let my_db_path = dir.path().join("mydb");
        std::fs::create_dir(my_db_path.as_path()).unwrap();
        let path = Arc::new(my_db_path);
        let my_db = runtime.block_on(async {
            let persistent = LocalFs::new(path);
            let db = Lsm::new(options, persistent).await.unwrap();

            MyDbWithRuntime::new(db, runtime.clone())
        });
        let rocksdb = build_rocks_db(&opts, dir.path().join("rocksdb"));
        let pair_db = DbPair::new(my_db, rocksdb);
        Arc::new(pair_db)
    })
}

criterion_group! {
  name = bench_against_rocks;
  config = Criterion::default();
  targets = bench_rocks, bench_mydb
}

criterion_main!(bench_against_rocks);
