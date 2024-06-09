use std::collections::Bound;
use std::iter;
use std::ops::Deref;
use std::sync::Arc;

use derive_new::new;
use futures::{stream, Stream, StreamExt};
use tracing::error;

use crate::entry::{Entry, InnerEntry};
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, MergeIterator,
    NoDeletedIterator, TwoMergeIterator,
};
use crate::key::{Key, KeySlice};
use crate::memtable::MemTableIterator;
use crate::mvcc::iterator::{build_time_dedup_iter, transform_bound};
use crate::persistent::Persistent;
use crate::sst::iterator::MergedSstIterator;
use crate::state::LsmStorageStateInner;

// pub type LsmIterator<'a, File> = NoDeletedIterator<LsmIteratorInner<'a, File>, anyhow::Error>;
pub type LsmIterator<'a> = Box<dyn Stream<Item = anyhow::Result<Entry>> + Unpin + 'a>;

type LsmIteratorInner<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, MemTableIterator<'a>>,
    MergedSstIterator<'a, File>,
>;

#[derive(new)]
pub struct LockedLsmIter<'a, P: Persistent> {
    state: arc_swap::Guard<Arc<LsmStorageStateInner<P>>>,
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
    timestamp: u64,
}

impl<'a, P> LockedLsmIter<'a, P>
where
    P: Persistent,
{
    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
        let a = self.build_memtable_iter().await;
        let b = self.build_sst_iter().await?;
        let merge = create_two_merge_iter(a, b).await?;
        let time_dedup = build_time_dedup_iter(merge, self.timestamp);
        // todo: dedup timestamps
        let iter = new_no_deleted_iter(time_dedup);
        let iter = Box::new(iter) as _;
        Ok(iter)
    }

    pub async fn build_memtable_iter(&self) -> MergeIterator<InnerEntry, MemTableIterator> {
        let (lower, upper) = transform_bound(self.lower, self.upper, self.timestamp);
        let lower = lower.map(Key::from);
        let upper = upper.map(Key::from);

        let memtable = self.state.memtable().deref().as_immutable_ref();
        let imm_memtables = self.state.imm_memtables().as_slice();
        let imm_memtables = imm_memtables.iter().map(Arc::as_ref);
        let tables = iter::once(memtable).chain(imm_memtables);
        let iters = stream::iter(tables).filter_map(move |table| async {
            table
                .scan_with_ts(lower, upper)
                .await
                .inspect_err(|e| error!(error = ?e))
                .ok()
                .flatten()
        });
        create_merge_iter_from_non_empty_iters(iters).await
    }

    pub async fn build_sst_iter(&self) -> anyhow::Result<MergedSstIterator<P::SstHandle>> {
        let (lower, upper) = transform_bound(self.lower, self.upper, self.timestamp);
        let lower = lower.map(Key::from);
        let upper = upper.map(Key::from);

        self.state.sstables_state().scan_sst(lower, upper).await
    }
}
