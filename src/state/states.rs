use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use deref_ext::DerefExt;
use derive_getters::Getters;
use futures::StreamExt;
use nom::sequence::pair;
use tokio::sync::{Mutex, MutexGuard};

use crate::block::BlockCache;
use crate::iterators::LockedLsmIter;
use crate::manifest::Manifest;
use crate::memtable::MemTable;
use crate::persistent::Persistent;
use crate::sst::compact::leveled::force_compaction;
use crate::sst::{SsTableBuilder, SstOptions, Sstables};
use crate::state::inner::LsmStorageStateInner;
use crate::state::Map;
use crate::utils::vec::pop;

#[derive(Getters)]
pub struct LsmStorageState<P: Persistent> {
    pub(crate) inner: ArcSwap<LsmStorageStateInner<P>>,
    block_cache: Arc<BlockCache>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) persistent: P,
    pub(crate) options: SstOptions,
    pub(crate) sst_id: AtomicUsize,
}

impl<P> Debug for LsmStorageState<P>
where
    P: Persistent,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.inner.load();
        f.debug_struct("LsmStorageState")
            .field("memtable", snapshot.memtable())
            .field("imm_memtables", snapshot.imm_memtables())
            .field("sstables_state", snapshot.sstables_state())
            .finish()
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    pub fn new(options: SstOptions, persistent: P) -> Self {
        let snapshot = LsmStorageStateInner::builder()
            .memtable(Arc::new(MemTable::create(0)))
            .imm_memtables(Vec::new())
            .sstables_state(Arc::new(Sstables::new(&options)))
            .build();
        Self {
            inner: ArcSwap::new(Arc::new(snapshot)),
            block_cache: Arc::new(BlockCache::new(1024)),
            state_lock: Mutex::default(),
            persistent,
            options,
            sst_id: AtomicUsize::new(1),
        }
    }

    pub async fn recover(options: SstOptions, persistent: P) -> anyhow::Result<Self> {
        let (manifest, manifest_records) = Manifest::recover(&persistent).await?;
        let block_cache = Arc::new(BlockCache::new(1024));
        let (inner, next_sst_id) = LsmStorageStateInner::recover(
            &options,
            manifest_records,
            &persistent,
            Some(block_cache.clone()),
        )
        .await?;
        let sst_id = AtomicUsize::new(next_sst_id);
        let this = Self {
            inner: ArcSwap::new(Arc::new(inner)),
            block_cache,
            state_lock: Default::default(),
            persistent,
            options,
            sst_id,
        };
        Ok(this)
    }
}

// KV store
impl<P> Map for LsmStorageState<P>
where
    P: Persistent,
{
    type Error = anyhow::Error;

    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let guard = self.scan(Bound::Included(key), Bound::Included(key));
        let value = guard
            .iter()
            .await?
            .next()
            .await
            .transpose()?
            .map(|entry| entry.value);
        Ok(value)
    }

    async fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> anyhow::Result<()> {
        let snapshot = self.inner.load();
        snapshot.memtable().put(key.into(), value.into()).await?;
        self.try_freeze_memtable(&snapshot).await;
        Ok(())
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> anyhow::Result<()> {
        let snapshot = self.inner.load();
        snapshot.memtable().put(key.into(), Bytes::new()).await?;
        self.try_freeze_memtable(&snapshot).await;
        Ok(())
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    pub(crate) fn next_sst_id(&self) -> usize {
        self.sst_id()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn scan<'a>(&self, lower: Bound<&'a [u8]>, upper: Bound<&'a [u8]>) -> LockedLsmIter<'a, P> {
        let snapshot = self.inner.load();
        LockedLsmIter::new(snapshot, lower, upper)
    }
}

// flush
impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    async fn try_freeze_memtable(&self, snapshot: &LsmStorageStateInner<P>) {
        if self.exceed_memtable_size_limit(snapshot.memtable()) {
            let guard = self.state_lock.lock().await;
            if self.exceed_memtable_size_limit(snapshot.memtable()) {
                self.force_freeze_memtable(&guard);
            }
        }
    }

    fn exceed_memtable_size_limit<W>(&self, memtable: &impl Deref<Target = MemTable<W>>) -> bool {
        memtable.deref().approximate_size() > *self.options.target_sst_size()
    }

    pub(crate) fn force_freeze_memtable(&self, _guard: &MutexGuard<()>) {
        let snapshot = self.inner.load_full();
        let new = freeze_memtable(snapshot, self.next_sst_id());
        self.inner.store(new);
    }

    pub async fn may_flush_imm_memtable(&self) -> anyhow::Result<()> {
        let num_memtable_limit = *self.options.num_memtable_limit();
        if self.inner.load().imm_memtables.len() + 1 >= num_memtable_limit {
            let guard = self.state_lock.lock().await;
            if self.inner.load().imm_memtables.len() + 1 >= num_memtable_limit {
                self.force_flush_imm_memtable(&guard).await?;
            }
        }
        Ok(())
    }

    pub async fn force_flush_imm_memtable(
        &self,
        _guard: &MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        let new = {
            let cur = self.inner.load_full();
            flush_imm_memtable(
                cur,
                &self.block_cache,
                self.persistent(),
                *self.options.block_size(),
            )
            .await?
        };
        if let Some(new) = new {
            // todo: 封装 ArcSwap 和 Mutex
            self.inner.store(new);
        }
        Ok(())
    }

    pub async fn force_compact(&self, _guard: &MutexGuard<'_, ()>) -> anyhow::Result<()> {
        let new = {
            let cur = self.inner.load();
            let mut new = Clone::clone(cur.as_ref());
            let mut new_sstables = Clone::clone(new.sstables_state().as_ref());
            force_compaction(
                &mut new_sstables,
                || self.next_sst_id(),
                self.options(),
                self.persistent(),
            )
            .await?;
            new.sstables_state = Arc::new(new_sstables);
            new
        };
        self.inner.store(Arc::new(new));
        Ok(())
    }
}

fn freeze_memtable<P: Persistent>(
    old: Arc<LsmStorageStateInner<P>>,
    next_sst_id: usize,
) -> Arc<LsmStorageStateInner<P>> {
    let new_memtable = Arc::new(MemTable::create(next_sst_id));
    let new_imm_memtables = {
        let old_memtable = old.memtable().clone().into_imm();
        let mut new = Vec::with_capacity(old.imm_memtables().len() + 1);
        new.push(old_memtable);
        new.extend_from_slice(old.imm_memtables());
        new
    };
    let new_state = LsmStorageStateInner::builder()
        .memtable(new_memtable)
        .imm_memtables(new_imm_memtables)
        .sstables_state(old.sstables_state().clone())
        .build();
    Arc::new(new_state)
}

async fn flush_imm_memtable<P: Persistent>(
    old: Arc<LsmStorageStateInner<P>>,
    block_cache: &Arc<BlockCache>,
    persistent: &P,
    block_size: usize,
) -> anyhow::Result<Option<Arc<LsmStorageStateInner<P>>>> {
    let (imm, last_memtable) = pop(old.imm_memtables().clone());
    let Some(last_memtable) = last_memtable else {
        return Ok(None);
    };

    let sst = {
        let mut builder = SsTableBuilder::new(block_size);
        builder.flush(&last_memtable);
        builder
            .build(last_memtable.id(), Some(block_cache.clone()), persistent)
            .await
    }?;

    let state = {
        let mut state = (**old.sstables_state()).clone();
        state.insert_sst(Arc::new(sst));
        state
    };

    let new = LsmStorageStateInner::builder()
        .memtable(old.memtable().clone())
        .imm_memtables(imm)
        .sstables_state(Arc::new(state))
        .build();

    Ok(Some(Arc::new(new)))
}

#[cfg(test)]
impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    async fn get_for_test(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get(key).await
    }

    async fn put_for_test(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            .await
    }

    async fn delete_for_test(&self, key: &[u8]) -> anyhow::Result<()> {
        self.delete(Bytes::copy_from_slice(key)).await
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use std::collections::Bound;
    use std::ops::Bound::{Included, Unbounded};
    use std::time::Duration;

    use crate::entry::Entry;
    use futures::{stream, Stream, StreamExt};
    use tempfile::{tempdir, TempDir};
    use tokio::time::timeout;
    use tracing::{info, Instrument};
    use tracing_subscriber::fmt::format::FmtSpan;

    use crate::iterators::no_deleted::new_no_deleted_iter;
    use crate::iterators::two_merge::create_inner;
    use crate::iterators::utils::{assert_stream_eq, build_stream, build_tuple_stream};
    use crate::iterators::{create_two_merge_iter, eq};
    use crate::persistent::file_object::LocalFs;
    use crate::persistent::Persistent;
    use crate::sst::SstOptions;
    use crate::state::states::LsmStorageState;

    #[tokio::test]
    async fn test_task2_storage_integration() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);

        assert_eq!(None, storage.get_for_test(b"0").await.unwrap());

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        assert_eq!(
            &storage.get_for_test(b"1").await.unwrap().unwrap()[..],
            b"233"
        );
        assert_eq!(
            &storage.get_for_test(b"2").await.unwrap().unwrap()[..],
            b"2333"
        );
        assert_eq!(
            &storage.get_for_test(b"3").await.unwrap().unwrap()[..],
            b"23333"
        );

        storage.delete_for_test(b"2").await.unwrap();

        assert!(storage.get_for_test(b"2").await.unwrap().is_none());
        storage.delete_for_test(b"0").await.unwrap(); // should NOT report any error
    }

    #[tokio::test]
    async fn test_task3_storage_integration() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);

        assert_eq!(storage.inner.load().imm_memtables().len(), 0);

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }
        assert_eq!(storage.inner.load().imm_memtables().len(), 1);

        let previous_approximate_size = storage.inner.load().imm_memtables()[0].approximate_size();
        assert!(previous_approximate_size >= 15);

        storage.put_for_test(b"1", b"2333").await.unwrap();
        storage.put_for_test(b"2", b"23333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }
        assert_eq!(storage.inner.load().imm_memtables().len(), 2);
        assert_eq!(
            storage.inner.load().imm_memtables()[1].approximate_size(),
            previous_approximate_size,
            "wrong order of memtables?"
        );
        assert!(
            storage.inner.load().imm_memtables()[0].approximate_size() > previous_approximate_size
        );
    }

    #[tokio::test]
    async fn test_task3_freeze_on_capacity() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);
        for _ in 0..1000 {
            storage.put_for_test(b"1", b"2333").await.unwrap();
        }
        let num_imm_memtables = storage.inner.load().imm_memtables().len();
        println!("num_imm_memtables: {}", num_imm_memtables);
        assert!(num_imm_memtables >= 1, "no memtable frozen?");
        for _ in 0..1000 {
            storage.delete_for_test(b"1").await.unwrap();
        }
        assert!(
            storage.inner.load().imm_memtables().len() > num_imm_memtables,
            "no more memtable frozen?"
        );
    }

    #[tokio::test]
    async fn test_task4_storage_integration() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);

        assert_eq!(&storage.get_for_test(b"0").await.unwrap(), &None);
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"1", b"233333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();
        assert_eq!(storage.inner.load().imm_memtables().len(), 2);
        assert_eq!(
            &storage.get_for_test(b"1").await.unwrap().unwrap()[..],
            b"233333"
        );
        assert_eq!(&storage.get_for_test(b"2").await.unwrap(), &None);
        assert_eq!(
            &storage.get_for_test(b"3").await.unwrap().unwrap()[..],
            b"233333"
        );
        assert_eq!(
            &storage.get_for_test(b"4").await.unwrap().unwrap()[..],
            b"23333"
        );
    }

    #[tokio::test]
    async fn test_task4_integration() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"1", b"233333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();

        {
            let guard = storage.scan(Bound::Unbounded, Bound::Unbounded);
            assert!(
                eq(
                    guard.iter().await.unwrap().map(Result::unwrap),
                    build_stream([("1", "233333"), ("3", "233333"), ("4", "23333"),])
                )
                .await
            );
        }
        {
            let guard = storage.scan(Bound::Included(b"2"), Bound::Included(b"3"));
            assert!(
                eq(
                    guard.iter().await.unwrap().map(Result::unwrap),
                    build_stream([("3", "233333")])
                )
                .await
            );
        }
    }

    #[tokio::test]
    async fn test_task1_storage_scan() {
        // tracing_subscriber::fmt::fmt()
        //     // .with_span_events(FmtSpan::EXIT | FmtSpan::ENTER | FmtSpan::CLOSE)
        //     .with_line_number(true)
        //     .with_file(true)
        //     .with_target(false)
        //     .with_level(false)
        //     .init();
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);
        storage.put_for_test(b"0", b"2333333").await.unwrap();
        storage.put_for_test(b"00", b"2333333").await.unwrap();
        storage.put_for_test(b"4", b"23").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"4").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"00", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"3", b"23333").await.unwrap();
        storage.delete_for_test(b"1").await.unwrap();

        {
            let inner = storage.inner.load();
            assert_eq!(inner.sstables_state().l0_sstables().len(), 2);
            assert_eq!(inner.imm_memtables().len(), 2);
        }
        {
            let guard = storage.scan(Unbounded, Unbounded);
            let iter = guard
                .iter()
                .await
                .unwrap()
                .map(Result::unwrap)
                .map(Entry::into_tuple);
            assert_stream_eq(
                iter,
                build_tuple_stream([
                    ("0", "2333333"),
                    ("00", "2333"),
                    ("2", "2333"),
                    ("3", "23333"),
                ]),
            )
            .await;
        }

        {
            let iter = storage.scan(Bound::Included(b"1"), Bound::Included(b"2"));
            assert_stream_eq(
                iter.iter()
                    .await
                    .unwrap()
                    .map(Result::unwrap)
                    .map(Entry::into_tuple),
                build_tuple_stream([("2", "2333")]),
            )
            .await;
        }

        {
            let iter = storage.scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"));
            assert_stream_eq(
                iter.iter()
                    .await
                    .unwrap()
                    .map(Result::unwrap)
                    .map(Entry::into_tuple),
                build_tuple_stream([("2", "2333")]),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_task1_storage_get() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir);
        storage.put_for_test(b"0", b"2333333").await.unwrap();
        storage.put_for_test(b"00", b"2333333").await.unwrap();
        storage.put_for_test(b"4", b"23").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"4").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"00", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard);
        }

        storage.put_for_test(b"3", b"23333").await.unwrap();
        storage.delete_for_test(b"1").await.unwrap();

        {
            let inner = storage.inner.load();
            assert_eq!(inner.sstables_state().l0_sstables().len(), 2);
            assert_eq!(inner.imm_memtables().len(), 2);
        }

        assert_eq!(
            storage.get_for_test(b"0").await.unwrap(),
            Some(Bytes::from_static(b"2333333"))
        );
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let mut iter = guard.build_memtable_iter().await;
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let mut iter = guard.build_sst_iter().await.unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let a = guard.build_memtable_iter().await;
            let b = guard.build_sst_iter().await.unwrap();
            let iter = create_inner(a, b).await.unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333"), ("00", "2333333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let a = guard.build_memtable_iter().await;
            let b = guard.build_sst_iter().await.unwrap();
            let iter = create_two_merge_iter(a, b).await.unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let a = guard.build_memtable_iter().await;
            let b = guard.build_sst_iter().await.unwrap();
            let iter = create_two_merge_iter(a, b).await.unwrap();
            let iter = new_no_deleted_iter(iter);
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let iter = guard.iter().await.unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333")]),
            )
            .await;
        }
        assert_eq!(
            storage.get_for_test(b"00").await.unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get_for_test(b"2").await.unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get_for_test(b"3").await.unwrap(),
            Some(Bytes::from_static(b"23333"))
        );
        assert_eq!(storage.get_for_test(b"4").await.unwrap(), None);
        assert_eq!(storage.get_for_test(b"--").await.unwrap(), None);
        assert_eq!(storage.get_for_test(b"555").await.unwrap(), None);
    }

    fn build_storage(dir: &TempDir) -> LsmStorageState<impl Persistent> {
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .enable_wal(false)
            .build();
        LsmStorageState::new(options, persistent)
    }
}
