// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use crate::entry::Entry;
use crate::iterators::{MergeIterator, TwoMergeIterator};
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::iterator::iter::SsTableIterator;

// todo: 用 MergeIterator vs MergeIteratorInner
pub type MergedSstIterator<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, SsTableIterator<'a, File>>,
    MergeIterator<Entry, SstConcatIterator<'a>>,
>;
