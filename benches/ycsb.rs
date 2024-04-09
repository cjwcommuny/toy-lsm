use std::collections::HashMap;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use futures::FutureExt;
use itertools::Itertools;
use maplit::hashmap;
use nom::AsBytes;
use tempfile::tempdir;
use ycsb::db::DB;

use better_mini_lsm::persistent::LocalFs;
use better_mini_lsm::sst::SstOptions;
use better_mini_lsm::state::LsmStorageState;
use better_mini_lsm::state::Map;

#[derive(Clone)]
struct LsmStorageStateBench(Arc<LsmStorageState<LocalFs>>);

impl IsSend for LsmStorageStateBench {}
impl IsSync for LsmStorageStateBench {}

trait IsSend: Send {}
trait IsSync: Sync {}

impl DB for LsmStorageStateBench {
    fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn insert(
        &self,
        _table: String,
        key: String,
        values: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let value = values
            .into_iter()
            .map(|(key, value)| format!("{}:{}", key, value))
            .join(",");
        self.0.put(key, value).await
    }

    async fn read(&self, _table: &str, key: &str) -> anyhow::Result<HashMap<String, String>> {
        let value = self.0.get(key.as_bytes()).await;
        let x = value?
            .map(|v| {
                let value = String::from_utf8(v.to_vec()).unwrap();
                hashmap! { "".to_string() => value }
            })
            .unwrap_or_else(HashMap::new);
        Ok(x)
    }
}

fn ycsb_bench(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let persistent = LocalFs::new(dir.into_path());
    let options = SstOptions::builder()
        .target_sst_size(1024)
        .block_size(4096)
        .num_memtable_limit(1000)
        .compaction_option(Default::default())
        .build();
    let database = LsmStorageStateBench(Arc::new(LsmStorageState::new(options, persistent)));
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("ycsb", |b| {
        b.to_async(&runtime)
            .iter(|| async { ycsb::ycsb_main(database.clone()).await.unwrap() })
    });
}

criterion_group!(benches, ycsb_bench);
criterion_main!(benches);