use crate::entry::{Entry, InnerEntry};
use crate::iterators::{MergeIterator, TwoMergeIterator};
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::iterator::iter::SsTableIterator;

// todo: 用 MergeIterator vs MergeIteratorInner
pub type MergedSstIterator<'a, File> = TwoMergeIterator<
    InnerEntry,
    MergeIterator<InnerEntry, SsTableIterator<'a, File>>,
    MergeIterator<InnerEntry, SstConcatIterator<'a>>,
>;
