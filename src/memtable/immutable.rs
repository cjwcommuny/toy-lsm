use std::collections::Bound;
use std::fmt::{Debug, Formatter};

use crate::key::{KeyBytes, KeySlice};
use bytemuck::TransparentWrapper;
use bytes::Bytes;
use crossbeam_skiplist::map;
use derive_new::new;

use ref_cast::RefCast;

use crate::memtable::iterator::MaybeEmptyMemTableIterRef;
use crate::memtable::mutable::MemTable;

#[derive(RefCast, TransparentWrapper, new)]
#[repr(transparent)]
pub struct ImmutableMemTable(MemTable);

impl Debug for ImmutableMemTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl ImmutableMemTable {
    pub fn approximate_size(&self) -> usize {
        self.0.approximate_size()
    }

    pub fn size(&self) -> usize {
        self.0.size()
    }
    pub fn id(&self) -> usize {
        *self.0.id()
    }
}

// todo: remove it
impl ImmutableMemTable {
    pub async fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'a>> {
        self.0.scan(lower, upper).await
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = map::Entry<KeyBytes, Bytes>> {
        self.0.map().iter()
    }
}

impl ImmutableMemTable {
    pub fn get_with_ts(&self, key: KeySlice) -> Option<Bytes> {
        self.0.get_with_ts(key)
    }

    pub async fn scan_with_ts(
        &self,
        lower: Bound<KeyBytes>,
        upper: Bound<KeyBytes>,
    ) -> anyhow::Result<MaybeEmptyMemTableIterRef<'_>> {
        self.0.scan_with_ts(lower, upper).await
    }
}

impl From<MemTable> for ImmutableMemTable {
    fn from(table: MemTable) -> Self {
        Self::new(table)
    }
}
