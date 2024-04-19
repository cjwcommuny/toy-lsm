use bytemuck::TransparentWrapper;
use std::collections::Bound;

use crate::entry::Entry;
use crate::key::KeySlice;
use crate::memtable::iterator::MaybeEmptyMemTableIterRef;
use crate::memtable::mutable::MemTable;
use crate::sst::SsTableBuilder;
use bytes::Bytes;
use crossbeam_skiplist::map;
use deref_ext::DerefExt;
use derive_new::new;
use nom::AsBytes;
use ref_cast::RefCast;

#[derive(RefCast, TransparentWrapper, new, Debug)]
#[repr(transparent)]
pub struct ImmutableMemTable(MemTable);

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

    pub fn iter(&self) -> impl Iterator<Item = map::Entry<Bytes, Bytes>> {
        self.0.map().iter()
    }
}

impl From<MemTable> for ImmutableMemTable {
    fn from(table: MemTable) -> Self {
        Self::new(table)
    }
}
