use std::collections::Bound;
use std::iter;

use bytes::Bytes;
use crossbeam_skiplist::map;
use futures::stream;

use crate::entry::Entry;
use crate::iterators::{MaybeEmptyStream, NonEmptyStream, OkIter};

pub type MemTableIterator<'a> = stream::Iter<OkIter<ClonedSkipMapRangeIter<'a>>>;
type ClonedSkipMapRangeIter<'a> =
    iter::Map<SkipMapRangeIter<'a>, fn(SkipMapRangeEntry<'a>) -> Entry>;

pub fn new_memtable_iter(iter: SkipMapRangeIter<'_>) -> MemTableIterator {
    let iter = iter.map(convert_entry as fn(_) -> _);
    stream::iter(OkIter::new(iter))
}

fn convert_entry(x: map::Entry<'_, Bytes, Bytes>) -> Entry {
    Entry {
        key: x.key().clone(),
        value: x.value().clone(),
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

type SkipMapRangeEntry<'a> = crossbeam_skiplist::map::Entry<'a, Bytes, Bytes>;

pub type NonEmptyMemTableIterRef<'a> = NonEmptyStream<Entry, Box<MemTableIterator<'a>>>;
pub type MaybeEmptyMemTableIterRef<'a> = MaybeEmptyStream<Entry, Box<MemTableIterator<'a>>>;
