use bytes::Bytes;
use std::collections::Bound;

use derive_new::new;
use futures::{stream, Stream, StreamExt};
use ouroboros::self_referencing;
use std::iter;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::error;

use crate::entry::{Entry, InnerEntry, Keyed};
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, MergeIterator, TwoMergeIterator,
};
use crate::key::Key;
use crate::memtable::MemTableIterator;
use crate::mvcc::iterator::{build_time_dedup_iter, transform_bound};
use crate::persistent::Persistent;
use crate::sst::iterator::MergedSstIterator;
use crate::state::LsmStorageStateInner;

// todo: change it to use<'a>
pub type LsmIterator<'a> = Box<dyn Stream<Item = anyhow::Result<Entry>> + Unpin + Send + 'a>;

#[allow(dead_code)]
type LsmIteratorInner<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, MemTableIterator<'a>>,
    MergedSstIterator<'a, File>,
>;

#[self_referencing]
pub struct LsmIter<'a, S: 'a> {
    state: LsmWithRange<'a, S>,

    #[borrows(state)]
    #[covariant]
    iter: LsmIterator<'this>,
}

impl<'a, S, P> LsmIter<'a, S>
where
    S: Deref<Target = LsmStorageStateInner<P>> + Send + 'a,
    P: Persistent,
{
    pub async fn try_build(
        state: S,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
        timestamp: u64,
    ) -> anyhow::Result<LsmIterator<'a>> {
        let guard = LsmWithRange {
            state,
            lower,
            upper,
            timestamp,
        };

        let iter = LsmIter::try_new_async(guard, |guard| Box::pin(guard.iter())).await?;
        Ok(Box::new(iter))
    }
}

impl<'a, S, P> Stream for LsmIter<'a, S>
where
    S: Deref<Target = LsmStorageStateInner<P>>,
    P: Persistent,
{
    type Item = anyhow::Result<Entry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);

        this.with_iter_mut(|iter| {
            let pinned = Pin::new(iter);
            pinned.poll_next(cx)
        })
    }
}

#[derive(new)]
pub struct LsmWithRange<'a, S> {
    state: S,
    pub(crate) lower: Bound<&'a [u8]>,
    pub(crate) upper: Bound<&'a [u8]>,
    timestamp: u64,
}

fn assert_raw_stream(_s: &impl Stream<Item = anyhow::Result<InnerEntry>>) {}

fn assert_tuple_stream(_s: &impl Stream<Item = anyhow::Result<(Keyed<Bytes, Bytes>, u64)>>) {}

fn assert_result_stream(_s: &impl Stream<Item = anyhow::Result<Keyed<Bytes, Bytes>>>) {}

impl<'a, S, P> LsmWithRange<'a, S>
where
    S: Deref<Target = LsmStorageStateInner<P>>,
    P: Persistent,
{
    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
        let time_dedup = self.iter_with_delete().await?;
        let iter = new_no_deleted_iter(time_dedup);
        let iter = Box::new(iter) as _;
        Ok(iter)
    }

    pub async fn iter_with_delete(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Entry>> + Unpin + Send + '_> {
        let a = self.build_memtable_iter().await;
        assert_raw_stream(&a);
        let b = self.build_sst_iter().await?;
        assert_raw_stream(&b);
        let merge = create_two_merge_iter(a, b).await?;
        assert_raw_stream(&merge);
        let merge = merge.map(|entry| entry.map(Keyed::into_timed_tuple));
        assert_tuple_stream(&merge);
        let time_dedup = build_time_dedup_iter(merge, self.timestamp);
        assert_result_stream(&time_dedup);
        Ok(time_dedup)
    }

    pub async fn build_memtable_iter(&self) -> MergeIterator<InnerEntry, MemTableIterator> {
        let (lower, upper) = transform_bound(self.lower, self.upper, self.timestamp);
        let lower = lower.map(Key::from);
        let upper = upper.map(Key::from);

        let memtable = self.state.memtable().deref().as_immutable_ref();
        let imm_memtables = self.state.imm_memtables().as_slice();
        let imm_memtables = imm_memtables.iter().map(Arc::as_ref);
        let tables = iter::once(memtable).chain(imm_memtables);
        let iters = stream::iter(tables).filter_map(move |table| {
            // todo: 这里不用每个 loop 都 copy，可以放在外面？
            let lower = lower.map(|ks| ks.map(Bytes::copy_from_slice));
            let upper = upper.map(|ks| ks.map(Bytes::copy_from_slice));

            async {
                table
                    .scan_with_ts(lower, upper)
                    .await
                    .inspect_err(|e| error!(error = ?e))
                    .ok()
                    .flatten()
            }
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
