use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytemuck::TransparentWrapperAlloc;
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::{map, SkipMap};
use derive_getters::Getters;
use nom::AsBytes;

use crate::bound::{map_bound_own, BytesBound};
use crate::entry::Entry;
use crate::iterators::NonEmptyStream;
use ref_cast::RefCast;

use crate::memtable::immutable::ImmutableMemTable;
use crate::memtable::iterator::{new_memtable_iter, MaybeEmptyMemTableIterRef};
use crate::memtable::mutable;
use crate::state::Map;

use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
/// todo: MemTable 本质是 Map，可以抽象为 trait
#[derive(Getters)]
pub struct MemTable {
    pub(self) map: SkipMap<Bytes, Bytes>,
    wal: Option<Wal>,
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
    pub fn create(id: usize) -> Self {
        Self {
            map: SkipMap::new(),
            wal: None,
            id,
            approximate_size: Arc::default(),
        }
    }

    pub fn into_imm(self: Arc<Self>) -> Arc<ImmutableMemTable> {
        TransparentWrapperAlloc::wrap_arc(self)
    }

    pub fn as_immutable_ref(&self) -> &ImmutableMemTable {
        ImmutableMemTable::ref_cast(self)
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|x| x.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: Bytes, value: Bytes) -> impl Future<Output = Result<()>> + Send + '_ {
        async {
            let size = key.len() + value.len();
            if let Some(wal) = self.wal.as_ref() {
                wal.put(key.as_bytes(), value.as_bytes()).await?
            }
            self.map.insert(key, value);
            self.approximate_size.fetch_add(size, Ordering::Release);

            Ok(())
        }
    }

    pub async fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync().await?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub async fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> Result<MaybeEmptyMemTableIterRef<'a>> {
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
    pub async fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)).await
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub async fn for_testing_scan_slice<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> Result<MaybeEmptyMemTableIterRef<'a>> {
        self.scan(lower, upper).await
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
    use crate::memtable::mutable::MemTable;

    #[tokio::test]
    async fn test_task1_memtable_get() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").await.unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").await.unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").await.unwrap();
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

    #[tokio::test]
    async fn test_task1_memtable_overwrite() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").await.unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").await.unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").await.unwrap();
        memtable.for_testing_put_slice(b"key1", b"value11").await.unwrap();
        memtable.for_testing_put_slice(b"key2", b"value22").await.unwrap();
        memtable.for_testing_put_slice(b"key3", b"value33").await.unwrap();
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
