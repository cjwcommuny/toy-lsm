use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};

use crate::entry::Entry;
use crate::key::KeySlice;
use crate::persistent::PersistentHandle;
use crate::sst::iterator::iter::SsTableIterator;
use crate::sst::SsTable;

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.

// todo: 这里应该用 type alias impl trait 去除 Box
pub type SstConcatIterator<'a> = Box<dyn Stream<Item = Result<Entry>> + Send + Unpin + 'a>;

pub fn create_sst_concat_and_seek_to_first<File>(
    sstables: Vec<&SsTable<File>>,
) -> Result<SstConcatIterator>
where
    File: PersistentHandle,
{
    let iter = stream::iter(sstables).flat_map(SsTableIterator::create_and_seek_to_first);
    Ok(Box::new(iter) as _)
}

pub fn create_sst_concat_and_seek_to_key<'a, File>(
    _sstables: Vec<&'a SsTable<File>>,
    _key: KeySlice,
) -> Result<SstConcatIterator<'a>>
where
    File: PersistentHandle,
{
    // let key = key.to_key_vec();
    // // todo: 理论上只有第一个 iter 需要 seek，会不会有点慢
    // let iter = stream::iter(sstables).flat_map(get_fn(key));
    // Ok(Box::new(iter) as _)
    todo!()
}

pub fn scan_sst_concat<'a, File, I>(
    sstables: I,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
) -> Result<SstConcatIterator<'a>>
where
    File: PersistentHandle + 'a,
    I: IntoIterator<Item = &'a SsTable<File>> + 'a,
    I::IntoIter: Send,
{
    let iter =
        stream::iter(sstables).flat_map(move |table| SsTableIterator::scan(table, lower, upper));

    Ok(Box::new(iter) as _)
}
