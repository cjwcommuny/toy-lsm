use std::fmt::{Debug, Formatter};

use std::ops::Bound;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytemuck::TransparentWrapperAlloc;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use derive_getters::Getters;
use ref_cast::RefCast;

use crate::bound::BytesBound;
use crate::entry::Entry;
use crate::iterators::NonEmptyStream;
use crate::key::{KeyBytes, KeySlice};
use crate::manifest::{Manifest, ManifestRecord, NewMemtable};
use crate::memtable::immutable::ImmutableMemTable;
use crate::memtable::iterator::{new_memtable_iter, MaybeEmptyMemTableIterRef};
use crate::persistent::interface::{ManifestHandle, WalHandle};
use crate::persistent::Persistent;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
/// todo: MemTable 本质是 Map，可以抽象为 trait
/// todo: memtable 和 wal 捆绑在一起
#[derive(Getters)]
pub struct MemTable {
    pub(self) map: SkipMap<KeyBytes, Bytes>,
    id: usize,

    #[getter(skip)]
    approximate_size: Arc<AtomicUsize>,
}

impl Debug for MemTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let first = self.map.iter().next();
        let first = first.as_ref().map(|entry| entry.key());
        let last = self.map.iter().last();
        let last = last.as_ref().map(|entry| entry.key());

        f.debug_struct("MemTable")
            .field("id", &self.id)
            .field("first", &first)
            .field("last", &last)
            .field("size", &self.approximate_size())
            .finish()
    }
}

impl MemTable {
    /// Create a new mem-table.
    #[cfg(test)]
    pub fn create(id: usize) -> Self {
        Self::new(id, SkipMap::new())
    }

    fn new(id: usize, map: SkipMap<KeyBytes, Bytes>) -> Self {
        Self {
            map,
            id,
            approximate_size: Arc::default(),
        }
    }

    pub fn as_immutable_ref(&self) -> &ImmutableMemTable {
        ImmutableMemTable::ref_cast(self)
    }
}

impl MemTable {
    // todo: rename
    pub async fn create_with_wal<File: ManifestHandle>(
        id: usize,
        manifest: &Manifest<File>,
    ) -> anyhow::Result<Self> {
        let this = Self::new(id, SkipMap::new());

        {
            let manifest_record = ManifestRecord::NewMemtable(NewMemtable(id));
            manifest.add_record(manifest_record).await?;
        }

        Ok(this)
    }

    /// Create a memtable from WAL
    pub async fn recover_from_wal<P: Persistent>(
        id: usize,
        persistent: &P,
    ) -> anyhow::Result<Self> {
        // todo: last memtable should be put to mutable memtable
        let (_, map) = Wal::recover(id, persistent).await?;
        let this = Self::new(id, map);
        Ok(this)
    }

    /// Get a value by key.
    /// todo: remote this method
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.get_with_ts(KeySlice::new(key, 0))
    }

    pub async fn into_imm(self: Arc<Self>) -> anyhow::Result<Arc<ImmutableMemTable>> {
        // todo: into_imm 需不需要 sync?
        // self.sync_wal().await?;
        Ok(TransparentWrapperAlloc::wrap_arc(self))
    }

    /// Get an iterator over a range of keys.
    /// todo: remote this method
    pub async fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'a>> {
        self.scan_with_ts(
            lower.map(|k| KeyBytes::new(Bytes::copy_from_slice(k), 0)),
            upper.map(|k| KeyBytes::new(Bytes::copy_from_slice(k), 0)),
        )
        .await
    }
}

// with transaction
impl MemTable {
    pub fn get_with_ts(&self, key: KeySlice) -> Option<Bytes> {
        // todo: 因为 rust 的 Borrow trait 不够 general，这里需要复制，如何避免？
        let key = key.map(Bytes::copy_from_slice);
        self.map.get(&key).map(|x| x.value().clone())
    }

    pub async fn put_with_ts<W: WalHandle>(
        &self,
        wal: Option<&Wal<W>>,
        key: KeyBytes,
        value: Bytes,
    ) -> anyhow::Result<()> {
        let KeyBytes { key, timestamp } = key;
        let entry = Entry::new(key, value);
        self.put_batch(wal, slice::from_ref(&entry), timestamp)
            .await
    }

    pub async fn put_batch<W: WalHandle>(
        &self,
        wal: Option<&Wal<W>>,
        entries: &[Entry],
        timestamp: u64,
    ) -> anyhow::Result<()> {
        if let Some(wal) = wal {
            let entries = entries.iter().map(Entry::as_ref);
            wal.put_batch(entries, timestamp).await?;
        }

        let mut size = 0;
        for entry in entries {
            let Entry { key, value } = entry;
            let key = KeyBytes::new(key.clone(), timestamp);
            let value = value.clone();

            size += key.len() + value.len();
            self.map.insert(key, value);
        }
        self.approximate_size.fetch_add(size, Ordering::Release);

        Ok(())
    }

    pub async fn scan_with_ts(
        &self,
        lower: Bound<KeyBytes>,
        upper: Bound<KeyBytes>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'_>> {
        // todo: 由于 rust 的 Borrow trait 的限制，这里只能 copy

        let iter = self.map.range(BytesBound {
            start: lower,
            end: upper,
        });
        let iter = new_memtable_iter(iter);
        NonEmptyStream::try_new(iter).await
    }
}

#[cfg(test)]
impl MemTable {
    pub async fn for_testing_put_slice<W: WalHandle>(
        &self,
        wal: Option<&Wal<W>>,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        self.put(
            wal,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
        )
        .await
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub async fn for_testing_scan_slice<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'a>> {
        self.scan_with_ts(
            lower.map(|k| KeyBytes::new(Bytes::copy_from_slice(k), 0)),
            upper.map(|k| KeyBytes::new(Bytes::copy_from_slice(k), 0)),
        )
        .await
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// todo: remote this method
    pub async fn put<W: WalHandle>(
        &self,
        wal: Option<&Wal<W>>,
        key: Bytes,
        value: Bytes,
    ) -> anyhow::Result<()> {
        self.put_with_ts(wal, KeyBytes::new(key, 0), value).await
    }
}

impl MemTable {
    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod test {
    use crate::key::Key;
    use crate::manifest::Manifest;
    use crate::memtable::mutable::MemTable;
    use crate::mvcc::iterator::transform_bound;
    use crate::persistent::LocalFs;
    use crate::time::{TimeIncrement, TimeProvider};
    use crate::wal::Wal;
    use bytes::Bytes;
    use std::ops::Bound::Included;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_task1_memtable_get_wal() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let manifest = Manifest::create(&persistent).await.unwrap();
        let id = 123;

        let wal = Wal::create(id, &persistent).await.unwrap();
        {
            let memtable = MemTable::create_with_wal(id, &manifest).await.unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key1", b"value1")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key2", b"value2")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key3", b"value3")
                .await
                .unwrap();
        }

        {
            let memtable = MemTable::recover_from_wal(id, &persistent).await.unwrap();
            assert_eq!(
                &memtable.for_testing_get_slice(b"key1").unwrap()[..],
                b"value1"
            );
            assert_eq!(
                &memtable.for_testing_get_slice(b"key2").unwrap()[..],
                b"value2"
            );
            assert_eq!(
                &memtable.for_testing_get_slice(b"key3").unwrap()[..],
                b"value3"
            );
        }
    }

    #[tokio::test]
    async fn test_task1_memtable_overwrite() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let manifest = Manifest::create(&persistent).await.unwrap();
        let id = 123;
        let wal = Wal::create(id, &persistent).await.unwrap();

        {
            let memtable = MemTable::create_with_wal(id, &manifest).await.unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key1", b"value1")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key2", b"value2")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key3", b"value3")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key1", b"value11")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key2", b"value22")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(Some(&wal), b"key3", b"value33")
                .await
                .unwrap();
        }

        {
            let memtable = MemTable::recover_from_wal(id, &persistent).await.unwrap();
            assert_eq!(
                &memtable.for_testing_get_slice(b"key1").unwrap()[..],
                b"value11"
            );
            assert_eq!(
                &memtable.for_testing_get_slice(b"key2").unwrap()[..],
                b"value22"
            );
            assert_eq!(
                &memtable.for_testing_get_slice(b"key3").unwrap()[..],
                b"value33"
            );
        }
    }

    #[tokio::test]
    async fn test_memtable_mvcc() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let manifest = Manifest::create(&persistent).await.unwrap();
        let id = 123;
        let wal = Wal::create(id, &persistent).await.unwrap();

        let memtable = MemTable::create_with_wal(id, &manifest).await.unwrap();
        let time_provider = Box::<TimeIncrement>::default();

        memtable
            .put_with_ts(
                Some(&wal),
                Key::new(Bytes::copy_from_slice(b"key1"), time_provider.now()),
                Bytes::copy_from_slice(b"value1"),
            )
            .await
            .unwrap();

        {
            let now = time_provider.now();
            let (lower, upper) = transform_bound(Included(b"key1"), Included(b"key1"), now);
            let lower = lower.map(Key::from);
            let upper = upper.map(Key::from);
            let lower = lower.map(|ks| ks.map(|b| Bytes::copy_from_slice(b)));
            let upper = upper.map(|ks| ks.map(|b| Bytes::copy_from_slice(b)));
            let iter = memtable.scan_with_ts(lower, upper).await.unwrap();

            let (new_iter, _) = iter.unwrap().next().await;
            let new_iter = new_iter.unwrap();
            assert!(new_iter.is_none());
        }
    }
}
