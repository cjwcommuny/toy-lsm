use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use derive_getters::Getters;
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::iterators::LockedLsmIter;
use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::{SstOptions, Sstables};
use crate::state::Map;

#[derive(Getters)]
pub struct LsmStorageState<P: Persistent> {
    /// The current memtable.
    memtable: RwLock<MemTable>,
    /// Immutable memtables, from latest to earliest.
    imm_memtables: RwLock<Vec<ImmutableMemTable>>,
    sstables_state: RwLock<Sstables<P::Handle>>,
    persistent: P,
    options: SstOptions,
    sst_id: AtomicUsize,
}

impl<P> Debug for LsmStorageState<P>
where
    P: Persistent,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmStorageState")
            .field("memtable", &self.memtable)
            .field("imm_memtables", &self.imm_memtables)
            .field("sstables_state", &self.sstables_state)
            .finish()
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    pub fn new(options: SstOptions, persistent: P) -> Self {
        Self {
            memtable: RwLock::new(MemTable::create(0)),
            imm_memtables: Default::default(),
            sstables_state: RwLock::new(Sstables::new(&options)),
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
        let guard = self.scan(Bound::Included(key), Bound::Included(key)).await;
        let value = guard
            .iter()
            .await?
            .next()
            .await
            .transpose()?
            .map(|entry| entry.value);
        Ok(value)
    }

    async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> anyhow::Result<()> {
        self.try_freeze_memtable().await;
        self.memtable.read().await.put(key.into(), value.into())
    }

    async fn delete(&self, key: impl Into<Bytes>) -> anyhow::Result<()> {
        self.try_freeze_memtable().await;
        self.memtable.read().await.put(key.into(), Bytes::new())
    }
}

impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    pub async fn scan<'a, 'bound>(
        &'a self,
        lower: Bound<&'bound [u8]>,
        upper: Bound<&'bound [u8]>,
    ) -> LockedLsmIter<'a, 'bound, P::Handle> {
        // 目前需要同时持有三个 read lock，可以减少锁冲突
        LockedLsmIter::new(
            self.memtable.read().await,
            self.imm_memtables.read().await,
            self.sstables_state.read().await,
            lower,
            upper,
        )
    }

    fn next_sst_id(&self) -> usize {
        self.sst_id()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

// flush
impl<P> LsmStorageState<P>
where
    P: Persistent,
{
    async fn try_freeze_memtable(&self) {
        // 这里希望 exceed_memtable_size_limit 和 freeze_memtable 的调用是一个 transaction，没有其他
        // transaction 插入执行的中间，所以需要 double check
        if self.exceed_memtable_size_limit(&self.memtable.read().await) {
            let mut memtable = self.memtable.write().await;
            if self.exceed_memtable_size_limit(&memtable) {
                let mut imm_memtable = self.imm_memtables.write().await;
                self.force_freeze_memtable(&mut memtable, &mut imm_memtable);
            }
        }
    }

    fn exceed_memtable_size_limit(&self, memtable: &impl Deref<Target = MemTable>) -> bool {
        memtable.deref().approximate_size() > *self.options.target_sst_size()
    }

    fn force_freeze_memtable(
        &self,
        memtable: &mut impl DerefMut<Target = MemTable>,
        imm_memtables: &mut impl DerefMut<Target = Vec<ImmutableMemTable>>,
    ) {
        let empty_memtable = MemTable::create(self.next_sst_id());
        let old_memtable = mem::replace(memtable.deref_mut(), empty_memtable);

        imm_memtables.deref_mut().insert(0, old_memtable.into());
    }
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
    use crate::persistent::{Memory, Persistent};
    use crate::sst::SstOptions;
    use crate::state::states::LsmStorageState;
    use std::sync::Arc;
    use tempfile::tempdir;
    use crate::persistent::file_object::LocalFs;

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

        assert_eq!(storage.imm_memtables.read().await.len(), 0);

        storage.put_for_test(b"1", b"233").await.unwrap();
        storage.put_for_test(b"2", b"2333").await.unwrap();
        storage.put_for_test(b"3", b"23333").await.unwrap();

        force_freeze_memtable(&storage).await;
        assert_eq!(storage.imm_memtables.read().await.len(), 1);

        let previous_approximate_size = storage.imm_memtables.read().await[0].approximate_size();
        assert!(previous_approximate_size >= 15);

        storage.put_for_test(b"1", b"2333").await.unwrap();
        storage.put_for_test(b"2", b"23333").await.unwrap();
        storage.put_for_test(b"3", b"233333").await.unwrap();

        force_freeze_memtable(&storage).await;
        assert_eq!(storage.imm_memtables.read().await.len(), 2);
        assert_eq!(
            storage.imm_memtables.read().await[1].approximate_size(),
            previous_approximate_size,
            "wrong order of memtables?"
        );
        assert!(
            storage.imm_memtables.read().await[0].approximate_size() > previous_approximate_size
        );
    }

    #[tokio::test]
    async fn test_task3_freeze_on_capacity() {
        let storage = build_storage();
        for _ in 0..1000 {
            storage.put_for_test(b"1", b"2333").await.unwrap();
        }
        let num_imm_memtables = storage.imm_memtables.read().await.len();
        println!("num_imm_memtables: {}", num_imm_memtables);
        assert!(num_imm_memtables >= 1, "no memtable frozen?");
        for _ in 0..1000 {
            storage.delete_for_test(b"1").await.unwrap();
        }
        assert!(
            storage.imm_memtables.read().await.len() > num_imm_memtables,
            "no more memtable frozen?"
        );
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

    pub async fn force_freeze_memtable<P: Persistent>(storage: &LsmStorageState<P>) {
        let mut memtable = storage.memtable.write().await;
        let mut imm_memtable = storage.imm_memtables.write().await;
        storage.force_freeze_memtable(&mut memtable, &mut imm_memtable);
    }
}
