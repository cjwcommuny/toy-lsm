use anyhow::anyhow;
use arc_swap::ArcSwap;
use bytes::Bytes;
use derive_getters::Getters;
use futures::StreamExt;
use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tracing_futures::Instrument;

use crate::block::BlockCache;
use crate::iterators::LockedLsmIter;
use crate::manifest::{Flush, Manifest, ManifestRecord};
use crate::memtable::MemTable;
use crate::mvcc::core::LsmMvccInner;
use crate::mvcc::transaction::Transaction;
use crate::persistent::Persistent;
use crate::sst::compact::leveled::force_compact;
use crate::sst::{SsTableBuilder, SstOptions};
use crate::state::inner::LsmStorageStateInner;
use crate::state::Map;
use crate::utils::vec::pop;

#[derive(Getters)]
pub struct LsmStorageState<P: Persistent> {
    pub(crate) inner: ArcSwap<LsmStorageStateInner<P>>,
    block_cache: Arc<BlockCache>,
    manifest: Manifest<P::ManifestHandle>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) persistent: P,
    pub(crate) options: SstOptions,
    pub(crate) sst_id: AtomicUsize,
    mvcc: Option<LsmMvccInner>,
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
    pub async fn new(options: SstOptions, persistent: P) -> anyhow::Result<Self> {
        let (manifest, manifest_records) = Manifest::recover(&persistent).await?;
        let block_cache = Arc::new(BlockCache::new(1024));
        let (inner, next_sst_id) = LsmStorageStateInner::recover(
            &options,
            &manifest,
            manifest_records,
            &persistent,
            Some(block_cache.clone()),
        )
        .await?;
        let sst_id = AtomicUsize::new(next_sst_id);

        let this = Self {
            inner: ArcSwap::new(Arc::new(inner)),
            block_cache: Arc::new(BlockCache::new(1024)),
            manifest,
            state_lock: Mutex::default(),
            persistent,
            options,
            sst_id,
            mvcc: None, // todo
        };
        Ok(this)
    }

    pub fn new_txn(&self) -> anyhow::Result<Transaction<P>> {
        let mvcc = self.mvcc.as_ref().ok_or(anyhow!("no mvcc"))?;
        let tx = mvcc.new_txn(self.inner.load(), false);
        Ok(tx)
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
        snapshot
            .memtable()
            .put(key.into(), value.into())
            .instrument(tracing::info_span!("memtable_put"))
            .await?;
        self.try_freeze_memtable(&snapshot)
            .instrument(tracing::info_span!("try_freeze_memtable"))
            .await?;
        Ok(())
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> anyhow::Result<()> {
        let snapshot = self.inner.load();
        snapshot.memtable().put(key.into(), Bytes::new()).await?;
        self.try_freeze_memtable(&snapshot).await?;
        Ok(())
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    pub(crate) fn next_sst_id(&self) -> usize {
        self.sst_id().fetch_add(1, Ordering::Relaxed)
    }

    fn scan<'a>(&self, lower: Bound<&'a [u8]>, upper: Bound<&'a [u8]>) -> LockedLsmIter<'a, P> {
        let snapshot = self.inner.load();
        LockedLsmIter::new(snapshot, lower, upper, 0) // todo
    }
}

// flush
impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    async fn try_freeze_memtable(&self, snapshot: &LsmStorageStateInner<P>) -> anyhow::Result<()> {
        if self.exceed_memtable_size_limit(snapshot.memtable()) {
            let guard = self.state_lock.lock().await;
            if self.exceed_memtable_size_limit(snapshot.memtable()) {
                self.force_freeze_memtable(&guard).await?;
            }
        }
        Ok(())
    }

    fn exceed_memtable_size_limit<W>(&self, memtable: &impl Deref<Target = MemTable<W>>) -> bool {
        memtable.deref().approximate_size() > *self.options.target_sst_size()
    }

    // todo: 这个函数用到的 snapshot 不用 load？直接从 caller 传过来？
    pub(crate) async fn force_freeze_memtable(
        &self,
        _guard: &MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        let snapshot = self.inner.load_full();
        let new = freeze_memtable(
            snapshot,
            self.next_sst_id(),
            &self.persistent,
            &self.manifest,
        )
        .await?;
        self.inner.store(new);
        Ok(())
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
                &self.manifest,
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
            let mut new = Clone::clone(self.inner.load().as_ref());
            let mut new_sstables = Clone::clone(new.sstables_state().as_ref());

            force_compact(
                &mut new_sstables,
                || self.next_sst_id(),
                self.options(),
                self.persistent(),
                Some(&self.manifest),
            )
            .await?;

            new.sstables_state = Arc::new(new_sstables);
            new
        };
        self.inner.store(Arc::new(new));
        Ok(())
    }
}

async fn freeze_memtable<P: Persistent>(
    old: Arc<LsmStorageStateInner<P>>,
    next_sst_id: usize,
    persistent: &P,
    manifest: &Manifest<P::ManifestHandle>,
) -> anyhow::Result<Arc<LsmStorageStateInner<P>>> {
    let new_memtable = {
        let table = MemTable::create_with_wal(next_sst_id, persistent, manifest).await?;
        Arc::new(table)
    };
    let new_imm_memtables = {
        let old_memtable = old.memtable().clone().into_imm().await?;

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
    Ok(Arc::new(new_state))
}

async fn flush_imm_memtable<P: Persistent>(
    old: Arc<LsmStorageStateInner<P>>,
    block_cache: &Arc<BlockCache>,
    persistent: &P,
    manifest: &Manifest<P::ManifestHandle>,
    block_size: usize,
) -> anyhow::Result<Option<Arc<LsmStorageStateInner<P>>>> {
    let (imm, last_memtable) = pop(old.imm_memtables().clone());
    let Some(last_memtable) = last_memtable else {
        return Ok(None);
    };

    manifest
        .add_record(ManifestRecord::Flush(Flush(last_memtable.id())))
        .await?;

    let sst = {
        let mut builder = SsTableBuilder::new(block_size);
        builder.flush(&last_memtable);
        builder
            .build(last_memtable.id(), Some(block_cache.clone()), persistent)
            .await
    }?;

    persistent.delete_wal(last_memtable.id()).await?;

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

    // async fn write_batch_for_test(
    //     &self,
    //     records: impl IntoIterator<Item = Op<&[u8]>>,
    // ) -> anyhow::Result<()> {
    //     todo!()
    // }
}

#[cfg(test)]
mod test {
    use std::collections::Bound;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    use bytes::Bytes;
    use futures::StreamExt;
    use tempfile::{tempdir, TempDir};

    use crate::entry::Entry;
    use crate::iterators::create_two_merge_iter;
    use crate::iterators::no_deleted::new_no_deleted_iter;
    use crate::iterators::two_merge::create_inner;
    use crate::iterators::utils::test_utils::{
        assert_stream_eq, build_stream, build_tuple_stream, eq,
    };
    use crate::persistent::file_object::LocalFs;
    use crate::persistent::Persistent;
    use crate::sst::SstOptions;
    use crate::state::states::LsmStorageState;
    use crate::test_utils::iterator::unwrap_ts_stream;

    #[tokio::test]
    async fn test_task2_storage_integration() {
        let dir = tempdir().unwrap();
        let storage = build_storage(&dir).await.unwrap();

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
        let storage = build_storage(&dir).await.unwrap();

        assert_eq!(storage.inner.load().imm_memtables().len(), 0);

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }
        assert_eq!(storage.inner.load().imm_memtables().len(), 1);

        let previous_approximate_size = storage.inner.load().imm_memtables()[0].approximate_size();
        assert!(previous_approximate_size >= 15);

        storage.put_for_test(b"1", b"2333").await.unwrap();
        storage.put_for_test(b"2", b"23333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
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
        let storage = build_storage(&dir).await.unwrap();
        for _ in 0..1000 {
            storage.put_for_test(b"1", b"2333").await.unwrap();
        }
        let num_imm_memtables = storage.inner.load().imm_memtables().len();
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
        let storage = build_storage(&dir).await.unwrap();

        assert_eq!(&storage.get_for_test(b"0").await.unwrap(), &None);
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
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
        let storage = build_storage(&dir).await.unwrap();
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
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
        let storage = build_storage(&dir).await.unwrap();
        storage.put_for_test(b"0", b"2333333").await.unwrap();
        storage.put_for_test(b"00", b"2333333").await.unwrap();
        storage.put_for_test(b"4", b"23").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"4").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"00", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
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
        let storage = build_storage(&dir).await.unwrap();
        storage.put_for_test(b"0", b"2333333").await.unwrap();
        storage.put_for_test(b"00", b"2333333").await.unwrap();
        storage.put_for_test(b"4", b"23").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.delete_for_test(b"4").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"00", b"2333").await.unwrap();

        {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
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
            let iter = guard.build_memtable_iter().await;
            let iter = unwrap_ts_stream(iter);
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("00", "2333")]),
            )
            .await;
        }
        {
            let guard = storage.scan(Included(b"00"), Included(b"00"));
            let iter = guard.build_sst_iter().await.unwrap();
            let iter = unwrap_ts_stream(iter);
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
            let iter = unwrap_ts_stream(iter);
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
            let iter = unwrap_ts_stream(iter);
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
            let iter = unwrap_ts_stream(iter);
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

    async fn build_storage(dir: &TempDir) -> anyhow::Result<LsmStorageState<impl Persistent>> {
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .enable_wal(false)
            .build();
        LsmStorageState::new(options, persistent).await
    }

    #[tokio::test]
    async fn test_task2_memtable_mvcc() {
        test_task2_memtable_mvcc_helper(false).await;
    }

    #[tokio::test]
    async fn test_task2_lsm_iterator_mvcc() {
        test_task2_memtable_mvcc_helper(true).await;
    }

    async fn test_task2_memtable_mvcc_helper(flush: bool) {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .enable_wal(true)
            .build();
        let storage = LsmStorageState::new(options, persistent).await.unwrap();

        storage.put_for_test(b"a", b"1").await.unwrap();
        storage.put_for_test(b"b", b"1").await.unwrap();
        let snapshot1 = storage.new_txn().unwrap();
        storage.put_for_test(b"a", b"2").await.unwrap();
        let snapshot2 = storage.new_txn().unwrap();
        storage.delete_for_test(b"b").await.unwrap();
        storage.put_for_test(b"c", b"1").await.unwrap();
        let snapshot3 = storage.new_txn().unwrap();
        assert_eq!(
            snapshot1.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(
            snapshot1.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(snapshot1.get_for_test(b"c").await.unwrap(), None);

        {
            let iter = snapshot1.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "1"), ("b", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot2.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );
        assert_eq!(
            snapshot2.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(snapshot2.get_for_test(b"c").await.unwrap(), None);

        {
            let iter = snapshot2.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "2"), ("b", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot3.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );
        assert_eq!(snapshot3.get_for_test(b"b").await.unwrap(), None);
        assert_eq!(
            snapshot3.get_for_test(b"c").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        {
            let iter = snapshot3.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "2"), ("c", "1")]),
            )
            .await;
        }

        if !flush {
            let guard = storage.state_lock.lock().await;
            storage.force_freeze_memtable(&guard).await.unwrap();
        }

        storage.put_for_test(b"a", b"3").await.unwrap();
        storage.put_for_test(b"b", b"3").await.unwrap();
        let snapshot4 = storage.new_txn().unwrap();
        storage.put_for_test(b"a", b"4").await.unwrap();
        let snapshot5 = storage.new_txn().unwrap();
        storage.delete_for_test(b"b").await.unwrap();
        storage.put_for_test(b"c", b"5").await.unwrap();
        let snapshot6 = storage.new_txn().unwrap();

        if flush {
            let guard = storage.state_lock.lock().await;
            storage.force_flush_imm_memtable(&guard).await.unwrap();
        }

        assert_eq!(
            snapshot1.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(
            snapshot1.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(snapshot1.get_for_test(b"c").await.unwrap(), None);

        {
            let iter = snapshot1.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "1"), ("b", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot2.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );
        assert_eq!(
            snapshot2.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(snapshot2.get_for_test(b"c").await.unwrap(), None);

        {
            let iter = snapshot2.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "2"), ("b", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot3.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );
        assert_eq!(snapshot3.get_for_test(b"b").await.unwrap(), None);
        assert_eq!(
            snapshot3.get_for_test(b"c").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        {
            let iter = snapshot3.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "2"), ("c", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot4.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"3"))
        );
        assert_eq!(
            snapshot4.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"3"))
        );
        assert_eq!(
            snapshot4.get_for_test(b"c").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        {
            let iter = snapshot4.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "3"), ("b", "3"), ("c", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot5.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"4"))
        );
        assert_eq!(
            snapshot5.get_for_test(b"b").await.unwrap(),
            Some(Bytes::from_static(b"3"))
        );
        assert_eq!(
            snapshot5.get_for_test(b"c").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        {
            let iter = snapshot4.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "4"), ("b", "3"), ("c", "1")]),
            )
            .await;
        }

        assert_eq!(
            snapshot6.get_for_test(b"a").await.unwrap(),
            Some(Bytes::from_static(b"4"))
        );
        assert_eq!(snapshot6.get_for_test(b"b").await.unwrap(), None);
        assert_eq!(
            snapshot6.get_for_test(b"c").await.unwrap(),
            Some(Bytes::from_static(b"5"))
        );

        {
            let iter = snapshot6.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("a", "4"), ("c", "5")]),
            )
            .await;
        }

        if flush {
            {
                let iter = snapshot6.scan(Included(b"a"), Included(b"a")).unwrap();
                assert_stream_eq(
                    iter.map(Result::unwrap).map(Entry::into_tuple),
                    build_tuple_stream([("a", "4")]),
                )
                .await;
            }

            {
                let iter = snapshot6.scan(Excluded(b"a"), Excluded(b"c")).unwrap();
                assert_stream_eq(
                    iter.map(Result::unwrap).map(Entry::into_tuple),
                    build_tuple_stream([]),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn test_task2_snapshot_watermark() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .enable_wal(true)
            .build();
        let storage = LsmStorageState::new(options, persistent).await.unwrap();

        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        storage.put_for_test(b"233", b"23333").await.unwrap();
        let txn3 = storage.new_txn().unwrap();
        assert_eq!(storage.mvcc().as_ref().unwrap().watermark(), txn1.read_ts);
        drop(txn1);
        assert_eq!(storage.mvcc().as_ref().unwrap().watermark(), txn2.read_ts);
        drop(txn2);
        assert_eq!(storage.mvcc().as_ref().unwrap().watermark(), txn3.read_ts);
        drop(txn3);
        assert_eq!(
            storage.mvcc().as_ref().unwrap().watermark(),
            storage.mvcc().as_ref().unwrap().latest_commit_ts()
        );
    }

    // todo: add test
    // #[tokio::test]
    // async fn test_task3_mvcc_compaction() {
    //     use Op::{Put, Del};
    //     let dir = tempdir().unwrap();
    //     let persistent = LocalFs::new(dir.path().to_path_buf());
    //     let options = SstOptions::builder()
    //         .target_sst_size(1024)
    //         .block_size(4096)
    //         .num_memtable_limit(1000)
    //         .compaction_option(Default::default())
    //         .enable_wal(true)
    //         .build();
    //     let storage = LsmStorageState::new(options, persistent).await.unwrap();
    //
    //     let snapshot0 = storage.new_txn().unwrap();
    //     storage
    //         .write_batch_for_test(&[
    //             Put {key: b"a", value: b"1"},
    //             Put{key: b"b", value: b"1"},
    //         ]).await
    //         .unwrap();
    //     let snapshot1 = storage.new_txn().unwrap();
    //     storage
    //         .write_batch_for_test(&[
    //             Put{key: b"a", value: b"2"},
    //             Put{key: b"d", value: b"2"},
    //         ]).await
    //         .unwrap();
    //     let snapshot2 = storage.new_txn().unwrap();
    //     storage
    //         .write_batch_for_test(&[
    //             Put{key: b"a", value: b"3"},
    //             Del(b"d"),
    //         ]).await
    //         .unwrap();
    //     let snapshot3 = storage.new_txn().unwrap();
    //     storage
    //         .write_batch_for_test(&[
    //             Put{key: b"c", value: b"4"},
    //             Del(b"a"),
    //         ]).await
    //         .unwrap();
    //
    //     storage.force_flush().unwrap();
    //     storage.force_full_compaction().unwrap();
    //
    //     let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    //     check_iter_result_by_key(
    //         &mut iter,
    //         vec![
    //             (Bytes::from("a"), Bytes::new()),
    //             (Bytes::from("a"), Bytes::from("3")),
    //             (Bytes::from("a"), Bytes::from("2")),
    //             (Bytes::from("a"), Bytes::from("1")),
    //             (Bytes::from("b"), Bytes::from("1")),
    //             (Bytes::from("c"), Bytes::from("4")),
    //             (Bytes::from("d"), Bytes::new()),
    //             (Bytes::from("d"), Bytes::from("2")),
    //         ],
    //     );
    //
    //     drop(snapshot0);
    //     storage.force_full_compaction().unwrap();
    //
    //     let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    //     check_iter_result_by_key(
    //         &mut iter,
    //         vec![
    //             (Bytes::from("a"), Bytes::new()),
    //             (Bytes::from("a"), Bytes::from("3")),
    //             (Bytes::from("a"), Bytes::from("2")),
    //             (Bytes::from("a"), Bytes::from("1")),
    //             (Bytes::from("b"), Bytes::from("1")),
    //             (Bytes::from("c"), Bytes::from("4")),
    //             (Bytes::from("d"), Bytes::new()),
    //             (Bytes::from("d"), Bytes::from("2")),
    //         ],
    //     );
    //
    //     drop(snapshot1);
    //     storage.force_full_compaction().unwrap();
    //
    //     let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    //     check_iter_result_by_key(
    //         &mut iter,
    //         vec![
    //             (Bytes::from("a"), Bytes::new()),
    //             (Bytes::from("a"), Bytes::from("3")),
    //             (Bytes::from("a"), Bytes::from("2")),
    //             (Bytes::from("b"), Bytes::from("1")),
    //             (Bytes::from("c"), Bytes::from("4")),
    //             (Bytes::from("d"), Bytes::new()),
    //             (Bytes::from("d"), Bytes::from("2")),
    //         ],
    //     );
    //
    //     drop(snapshot2);
    //     storage.force_full_compaction().unwrap();
    //
    //     let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    //     check_iter_result_by_key(
    //         &mut iter,
    //         vec![
    //             (Bytes::from("a"), Bytes::new()),
    //             (Bytes::from("a"), Bytes::from("3")),
    //             (Bytes::from("b"), Bytes::from("1")),
    //             (Bytes::from("c"), Bytes::from("4")),
    //         ],
    //     );
    //
    //     drop(snapshot3);
    //     storage.force_full_compaction().unwrap();
    //
    //     let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    //     check_iter_result_by_key(
    //         &mut iter,
    //         vec![
    //             (Bytes::from("b"), Bytes::from("1")),
    //             (Bytes::from("c"), Bytes::from("4")),
    //         ],
    //     );
    // }

    #[tokio::test]
    async fn test_txn_integration() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .enable_wal(true)
            .build();
        let storage = LsmStorageState::new(options, persistent).await.unwrap();

        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn1.put_for_test(b"test1", b"233").await.unwrap();
        txn2.put_for_test(b"test2", b"233").await.unwrap();

        {
            let iter = txn1.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("test1", "233")]),
            )
            .await;
        }

        {
            let iter = txn2.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("test2", "233")]),
            )
            .await;
        }

        let txn3 = storage.new_txn().unwrap();

        {
            let iter = txn3.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([]),
            )
            .await;
        }

        txn1.commit().await.unwrap();
        txn2.commit().await.unwrap();

        {
            let iter = txn3.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([]),
            )
            .await;
        }

        drop(txn3);

        // todo: add this test
        // {
        //     let iter = storage.scan(Unbounded, Unbounded);
        //     assert_stream_eq(
        //         iter.iter().await.unwrap().map(Result::unwrap).map(Entry::into_tuple),
        //         build_tuple_stream([("test1", "233"), ("test2", "233")]),
        //     )
        //         .await;
        // }

        let txn4 = storage.new_txn().unwrap();

        assert_eq!(
            txn4.get_for_test(b"test1").await.unwrap(),
            Some(Bytes::from("233"))
        );
        assert_eq!(
            txn4.get_for_test(b"test2").await.unwrap(),
            Some(Bytes::from("233"))
        );

        {
            let iter = txn4.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("test1", "233"), ("test2", "233")]),
            )
            .await;
        }

        txn4.put_for_test(b"test2", b"2333").await.unwrap();
        assert_eq!(
            txn4.get_for_test(b"test1").await.unwrap(),
            Some(Bytes::from("233"))
        );
        assert_eq!(
            txn4.get_for_test(b"test2").await.unwrap(),
            Some(Bytes::from("2333"))
        );

        {
            let iter = txn4.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("test1", "233"), ("test2", "2333")]),
            )
            .await;
        }

        txn4.delete_for_test(b"test2").await.unwrap();

        assert_eq!(
            txn4.get_for_test(b"test1").await.unwrap(),
            Some(Bytes::from("233"))
        );
        assert_eq!(txn4.get_for_test(b"test2").await.unwrap(), None);

        {
            let iter = txn4.scan(Unbounded, Unbounded).unwrap();
            assert_stream_eq(
                iter.map(Result::unwrap).map(Entry::into_tuple),
                build_tuple_stream([("test1", "233")]),
            )
            .await;
        }
    }
}
