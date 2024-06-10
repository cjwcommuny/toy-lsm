use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytemuck::TransparentWrapperAlloc;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use derive_getters::Getters;
use nom::AsBytes;
use ref_cast::RefCast;
use tracing_futures::Instrument;

use crate::bound::BytesBound;
use crate::iterators::NonEmptyStream;
use crate::key::{KeyBytes, KeySlice};
use crate::manifest::{Manifest, ManifestRecord, NewMemtable};
use crate::memtable::immutable::ImmutableMemTable;
use crate::memtable::iterator::{new_memtable_iter, MaybeEmptyMemTableIterRef};
use crate::persistent::interface::{ManifestHandle, WalHandle};
use crate::persistent::Persistent;
use crate::state::Map;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
/// todo: MemTable 本质是 Map，可以抽象为 trait
#[derive(Getters)]
pub struct MemTable<W> {
    pub(self) map: SkipMap<KeyBytes, Bytes>,
    wal: Option<Wal<W>>,
    id: usize,

    #[getter(skip)]
    approximate_size: Arc<AtomicUsize>,
}

impl<W> Debug for MemTable<W> {
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

impl<W> MemTable<W> {
    /// Create a new mem-table.
    #[cfg(test)]
    pub fn create(id: usize) -> Self {
        Self::new(id, SkipMap::new(), None)
    }

    fn new(id: usize, map: SkipMap<KeyBytes, Bytes>, wal: impl Into<Option<Wal<W>>>) -> Self {
        Self {
            map,
            wal: wal.into(),
            id,
            approximate_size: Arc::default(),
        }
    }

    pub fn as_immutable_ref(&self) -> &ImmutableMemTable<W> {
        ImmutableMemTable::ref_cast(self)
    }

    pub fn reset_wal(&mut self) {
        self.wal = None;
    }
}

impl<W: WalHandle> MemTable<W> {
    pub async fn create_with_wal<P: Persistent<WalHandle = W>>(
        id: usize,
        persistent: &P,
        manifest: &Manifest<P::ManifestHandle>,
    ) -> anyhow::Result<Self> {
        let wal = Wal::create(id, persistent).await?;
        let this = Self::new(id, SkipMap::new(), wal);

        {
            let manifest_record = ManifestRecord::NewMemtable(NewMemtable(id));
            manifest.add_record(manifest_record).await?;
        }

        Ok(this)
    }

    /// Create a memtable from WAL
    pub async fn recover_from_wal<P: Persistent<WalHandle = W>>(
        id: usize,
        persistent: &P,
    ) -> anyhow::Result<Self> {
        let (wal, map) = Wal::recover(id, persistent).await?;
        let this = Self::new(id, map, wal);
        Ok(this)
    }

    /// Get a value by key.
    /// todo: remote this method
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.get_with_ts(KeySlice::new(key, 0))
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// todo: remote this method
    pub async fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        self.put_with_ts(KeyBytes::new(key, 0), value).await
    }

    pub async fn sync_wal(&self) -> anyhow::Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync().await?;
        }
        Ok(())
    }

    pub async fn into_imm(self: Arc<Self>) -> anyhow::Result<Arc<ImmutableMemTable<W>>> {
        self.sync_wal().await?;
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
            lower.map(|k| KeySlice::new(k, 0)),
            upper.map(|k| KeySlice::new(k, 0)),
        )
        .await
    }
}

// with transaction
impl<W: WalHandle> MemTable<W> {
    pub fn get_with_ts(&self, key: KeySlice) -> Option<Bytes> {
        // todo: 因为 rust 的 Borrow trait 不够 general，这里需要复制，如何避免？
        let key = key.map(Bytes::copy_from_slice);
        self.map.get(&key).map(|x| x.value().clone())
    }

    pub async fn put_with_ts(&self, key: KeyBytes, value: Bytes) -> anyhow::Result<()> {
        let size = key.len() + value.len();
        if let Some(wal) = self.wal.as_ref() {
            wal.put(key.as_key_slice(), value.as_bytes())
                .instrument(tracing::info_span!("wal_put"))
                .await?
        }
        self.map.insert(key, value);
        self.approximate_size.fetch_add(size, Ordering::Release);

        Ok(())
    }

    pub async fn scan_with_ts<'a>(
        &'a self,
        lower: Bound<KeySlice<'a>>,
        upper: Bound<KeySlice<'a>>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'a>> {
        // todo: 由于 rust 的 Borrow trait 的限制，这里只能 copy
        let lower = lower.map(|ks| ks.map(Bytes::copy_from_slice));
        let upper = upper.map(|ks| ks.map(Bytes::copy_from_slice));

        let iter = self.map.range(BytesBound {
            start: lower.as_ref(),
            end: upper.as_ref(),
        });
        let iter = new_memtable_iter(iter);
        NonEmptyStream::try_new(iter).await
    }
}

fn build_path(dir: impl AsRef<Path>, id: usize) -> PathBuf {
    dir.as_ref().join(format!("{}.wal", id))
}

#[cfg(test)]
impl<W: WalHandle> MemTable<W> {
    pub async fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
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
        self.scan(lower, upper).await
    }
}

impl<W> MemTable<W> {
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
    use tempfile::tempdir;

    use crate::manifest::Manifest;
    use crate::memtable::mutable::MemTable;
    use crate::persistent::LocalFs;

    #[tokio::test]
    async fn test_task1_memtable_get_wal() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let manifest = Manifest::create(&persistent).await.unwrap();
        let id = 123;

        {
            let memtable = MemTable::create_with_wal(id, &persistent, &manifest)
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key1", b"value1")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key2", b"value2")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key3", b"value3")
                .await
                .unwrap();

            memtable.sync_wal().await.unwrap();
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

        {
            let memtable = MemTable::create_with_wal(id, &persistent, &manifest)
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key1", b"value1")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key2", b"value2")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key3", b"value3")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key1", b"value11")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key2", b"value22")
                .await
                .unwrap();
            memtable
                .for_testing_put_slice(b"key3", b"value33")
                .await
                .unwrap();

            memtable.sync_wal().await.unwrap();
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
}
