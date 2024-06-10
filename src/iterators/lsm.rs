use bytes::Bytes;
use std::collections::Bound;
use std::future::Future;
use std::iter;
use std::ops::Deref;
use std::sync::Arc;

use derive_new::new;
use futures::{stream, Stream, StreamExt};
use tracing::error;

use crate::entry::{Entry, InnerEntry, Keyed};
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

pub type LsmIterator<'a> = Box<dyn Stream<Item = anyhow::Result<Entry>> + Unpin + Send + 'a>;

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

fn assert_raw_stream(s: &impl Stream<Item = anyhow::Result<InnerEntry>>) {}

fn assert_tuple_stream(s: &impl Stream<Item = anyhow::Result<(Keyed<Bytes, Bytes>, u64)>>) {}

fn assert_result_stream(s: &impl Stream<Item = anyhow::Result<Keyed<Bytes, Bytes>>>) {}

impl<'a, P> LockedLsmIter<'a, P>
where
    P: Persistent,
{
    pub fn iter(&'a self) -> impl Future<Output = anyhow::Result<LsmIterator<'a>>> + Send {
        async {
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
            // todo: dedup timestamps
            let iter = new_no_deleted_iter(time_dedup);
            let iter = Box::new(iter) as _;
            Ok(iter)
        }
    }

    pub fn build_memtable_iter(
        &self,
    ) -> impl Future<Output = MergeIterator<InnerEntry, MemTableIterator>> + Send {
        async {
            let (lower, upper) = transform_bound(self.lower, self.upper, self.timestamp);
            let lower = lower.map(Key::from);
            let upper = upper.map(Key::from);

            let memtable = self.state.memtable().deref().as_immutable_ref();
            let imm_memtables = self.state.imm_memtables().as_slice();
            let imm_memtables = imm_memtables.iter().map(Arc::as_ref);
            let tables = iter::once(memtable).chain(imm_memtables);
            let iters = stream::iter(tables).filter_map(move |table| {
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
    }

    pub fn build_sst_iter(
        &self,
    ) -> impl Future<Output = anyhow::Result<MergedSstIterator<P::SstHandle>>> + Send {
        async {
            let (lower, upper) = transform_bound(self.lower, self.upper, self.timestamp);
            let lower = lower.map(Key::from);
            let upper = upper.map(Key::from);

            self.state.sstables_state().scan_sst(lower, upper).await
        }
    }
}
