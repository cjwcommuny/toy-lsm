use std::collections::Bound;
use std::fmt::{Debug, Formatter};

use arc_swap::ArcSwap;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use bytes::Bytes;
use deref_ext::DerefExt;
use derive_getters::Getters;
use futures::StreamExt;
use parking_lot::{Mutex, MutexGuard};

use crate::iterators::LockedLsmIter;
use crate::memtable::MemTable;
use crate::persistent::Persistent;
use crate::sst::{SstOptions, Sstables};
use crate::state::inner::LsmStorageStateInner;
use crate::state::Map;

#[derive(Getters)]
pub struct LsmStorageState<P: Persistent> {
    inner: ArcSwap<LsmStorageStateInner<P>>,
    state_lock: Mutex<()>,
    persistent: P,
    options: SstOptions,
    sst_id: AtomicUsize,
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
            state_lock: Mutex::default(),
            persistent,
            options,
            sst_id: AtomicUsize::new(1),
        }
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
        snapshot.memtable().put(key.into(), value.into())?;
        self.try_freeze_memtable(&snapshot);
        Ok(())
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> anyhow::Result<()> {
        let snapshot = self.inner.load();
        snapshot.memtable().put(key.into(), Bytes::new())?;
        self.try_freeze_memtable(&snapshot);
        Ok(())
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    fn next_sst_id(&self) -> usize {
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
    fn try_freeze_memtable(&self, snapshot: &LsmStorageStateInner<P>) {
        if self.exceed_memtable_size_limit(snapshot.memtable()) {
            let guard = self.state_lock.lock();
            if self.exceed_memtable_size_limit(snapshot.memtable()) {
                self.force_freeze_memtable(&guard);
            }
        }
    }

    fn exceed_memtable_size_limit(&self, memtable: &impl Deref<Target = MemTable>) -> bool {
        memtable.deref().approximate_size() > *self.options.target_sst_size()
    }

    fn force_freeze_memtable(&self, _guard: &MutexGuard<()>) {
        self.inner
            .rcu(|snapshot| freeze_memtable(snapshot, self.next_sst_id()));
    }
}

fn freeze_memtable<P: Persistent>(
    old: &Arc<LsmStorageStateInner<P>>,
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
    use crate::iterators::{build_stream, eq};
    use bytes::Bytes;
    use futures::FutureExt;
    use futures::StreamExt;
    use std::collections::Bound;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::persistent::file_object::LocalFs;
    use crate::persistent::Persistent;
    use crate::sst::SstOptions;
    use crate::state::states::LsmStorageState;

    #[tokio::test]
    async fn test_task2_storage_integration() {
        let storage = build_storage();

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
        let storage = build_storage();

        assert_eq!(storage.inner.load().imm_memtables().len(), 0);

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
            storage.force_freeze_memtable(&guard);
        }
        assert_eq!(storage.inner.load().imm_memtables().len(), 1);

        let previous_approximate_size = storage.inner.load().imm_memtables()[0].approximate_size();
        assert!(previous_approximate_size >= 15);

        storage.put_for_test(b"1", b"2333").await.unwrap();
        storage.put_for_test(b"2", b"23333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
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
        let storage = build_storage();
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
        let storage = build_storage();

        assert_eq!(&storage.get_for_test(b"0").await.unwrap(), &None);
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
            storage.force_freeze_memtable(&guard);
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
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
        let storage = build_storage();
        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
            storage.force_freeze_memtable(&guard);
        }

        storage.delete_for_test(b"1").await.unwrap();
        storage.delete_for_test(b"2").await.unwrap();
        storage.put_for_test(b"3", b"2333").await.unwrap();
        storage.put_for_test(b"4", b"23333").await.unwrap();

        {
            let guard = storage.state_lock.lock();
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

    fn build_storage() -> LsmStorageState<impl Persistent> {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.into_path());
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .build();
        LsmStorageState::new(options, persistent)
    }
}
