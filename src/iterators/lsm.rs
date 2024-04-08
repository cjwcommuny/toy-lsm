use crate::entry::Entry;
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, iter_fut_to_stream,
    MergeIterator, NoDeletedIterator, TwoMergeIterator,
};
use crate::memtable::{ImmutableMemTable, MemTableIterator};
use crate::persistent::Persistent;
use crate::sst::iterator::MergedSstIterator;

use crate::bound::map_bound_own;
use derive_new::new;
use futures::{stream, StreamExt};
use std::collections::Bound;
use std::future::{ready, Future};
use std::iter;
use std::ops::Deref;
use std::sync::Arc;

use crate::state::LsmStorageStateInner;

pub type LsmIterator<'a, File> = NoDeletedIterator<LsmIteratorInner<'a, File>, anyhow::Error>;

type LsmIteratorInner<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, MemTableIterator<'a>>,
    MergedSstIterator<'a, File>,
>;

#[derive(new)]
pub struct LockedLsmIter<'a, P: Persistent> {
    state: &'a LsmStorageStateInner<P>,
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
}

impl<'a, P> LockedLsmIter<'a, P>
where
    P: Persistent,
{
    pub async fn iter(&self) -> anyhow::Result<LsmIterator<P::Handle>> {
        let a = {
            let lower = map_bound_own(self.lower);
            let upper = map_bound_own(self.upper);
            let memtable = self.state.memtable().deref().as_immutable_ref();
            let imm_memtables = self.state.imm_memtables().as_slice();
            let imm_memtables = imm_memtables.iter().map(Arc::as_ref);
            let iters =
                stream::iter(iter::once(memtable).chain(imm_memtables)).filter_map(move |table| {
                    let lower = lower.clone();
                    let upper = upper.clone();
                    async { table.scan(lower, upper).await.ok().flatten() }
                });
            create_merge_iter_from_non_empty_iters(iters).await
        };

        let b = self
            .state
            .sstables_state()
            .scan_sst(self.lower, self.upper)
            .await?;
        let merge = create_two_merge_iter(a, b).await?;
        let iter = new_no_deleted_iter(merge);
        Ok(iter)
    }
}
