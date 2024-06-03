use std::collections::Bound;
use std::iter;
use std::ops::Deref;
use std::sync::Arc;

use derive_new::new;
use futures::{stream, StreamExt};
use tracing::error;

use crate::entry::{Entry, InnerEntry};
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, MergeIterator,
    NoDeletedIterator, TwoMergeIterator,
};
use crate::key::KeySlice;
use crate::memtable::MemTableIterator;
use crate::persistent::Persistent;
use crate::sst::iterator::MergedSstIterator;
use crate::state::LsmStorageStateInner;

pub type LsmIterator<'a, File> = NoDeletedIterator<LsmIteratorInner<'a, File>, anyhow::Error>;

type LsmIteratorInner<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, MemTableIterator<'a>>,
    MergedSstIterator<'a, File>,
>;

pub struct LockedLsmIter<'a, P: Persistent> {
    state: arc_swap::Guard<Arc<LsmStorageStateInner<P>>>,
    lower: Bound<KeySlice<'a>>,
    upper: Bound<KeySlice<'a>>,
}

impl<'a, P: Persistent> LockedLsmIter<'a, P> {
    pub fn new(
        state: arc_swap::Guard<Arc<LsmStorageStateInner<P>>>,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
        timestamp: u64,
    ) -> Self {
        let lower = lower.map(|key| KeySlice::new(key, timestamp));
        let upper = upper.map(|key| KeySlice::new(key, timestamp));
        Self { state, lower, upper }
    }
}

impl<'a, P> LockedLsmIter<'a, P>
where
    P: Persistent,
{
    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a, P::SstHandle>> {
        let a = self.build_memtable_iter().await;
        let b = self.build_sst_iter().await?;
        let merge = create_two_merge_iter(a, b).await?;
        // todo: dedup timestamps
        let iter = new_no_deleted_iter(merge);
        Ok(iter)
    }

    pub async fn build_memtable_iter(&self) -> MergeIterator<InnerEntry, MemTableIterator> {
        let memtable = self.state.memtable().deref().as_immutable_ref();
        let imm_memtables = self.state.imm_memtables().as_slice();
        let imm_memtables = imm_memtables.iter().map(Arc::as_ref);
        let tables = iter::once(memtable).chain(imm_memtables);
        let iters = stream::iter(tables).filter_map(move |table| async {
            table
                .scan_with_ts(self.lower, self.upper)
                .await
                .inspect_err(|e| error!(error = ?e))
                .ok()
                .flatten()
        });
        create_merge_iter_from_non_empty_iters(iters).await
    }

    pub async fn build_sst_iter(&self) -> anyhow::Result<MergedSstIterator<P::SstHandle>> {
        self.state
            .sstables_state()
            .scan_sst(self.lower, self.upper)
            .await
    }
}
