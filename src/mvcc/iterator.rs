use crate::bound::BoundRange;
use crate::entry::{Entry, InnerEntry, Keyed};
use crate::iterators::inspect::{InspectIter, InspectIterImpl};
use crate::iterators::lsm::LsmIterator;
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{create_two_merge_iter, LockedLsmIter};
use crate::key::KeyBytes;
use crate::mvcc::transaction::{RWSet, Transaction};
use crate::persistent::Persistent;
use crate::utils::scoped::ScopedMutex;
use async_iter_ext::StreamTools;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use derive_new::new;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use num_traits::Bounded;
use ouroboros::self_referencing;
use std::future::ready;
use std::ops::Bound;

struct TxnWithBound<'a, P: Persistent> {
    txn: Transaction<'a, P>,
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
}

#[self_referencing]
pub struct LockedTxnIterWithTxn<'a, P: Persistent> {
    txn: TxnWithBound<'a, P>,

    #[borrows(txn)]
    #[covariant]
    iter: LockedTxnIter<'this, P>,
}

impl<'a, P: Persistent> LockedTxnIterWithTxn<'a, P> {
    pub fn new_(txn: Transaction<'a, P>, lower: Bound<&'a [u8]>, upper: Bound<&'a [u8]>) -> Self {
        Self::new(TxnWithBound { txn, lower, upper }, |txn| {
            txn.txn.scan(txn.lower, txn.upper)
        })
    }

    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
        self.with_iter(|iter| iter.iter()).await
    }
}

#[derive(new)]
pub struct LockedTxnIter<'a, P: Persistent> {
    local_storage: &'a SkipMap<Bytes, Bytes>,
    lsm_iter: LockedLsmIter<'a, P>,
    key_hashes: Option<&'a ScopedMutex<RWSet>>,
}

impl<'a, P: Persistent> LockedTxnIter<'a, P> {
    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
        let lsm_iter = self.lsm_iter.iter_with_delete().await?;
        let local_iter = txn_local_iterator(
            self.local_storage,
            self.lsm_iter.lower.map(Bytes::copy_from_slice),
            self.lsm_iter.upper.map(Bytes::copy_from_slice),
        );
        let merged = create_two_merge_iter(local_iter, lsm_iter).await?;
        let iter = new_no_deleted_iter(merged);
        let iter = InspectIterImpl::inspect_stream(iter, |entry| {
            let Ok(entry) = entry else { return };
            let Some(key_hashes) = self.key_hashes else {
                return;
            };
            let key = entry.key.as_ref();
            key_hashes.lock_with(|mut set| set.add_read_key(key));
        });
        let iter = Box::new(iter) as _;
        Ok(iter)
    }
}

pub fn txn_local_iterator(
    map: &SkipMap<Bytes, Bytes>,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
) -> impl Stream<Item = anyhow::Result<Entry>> + Unpin + Send + '_ {
    let it = map.range((lower, upper)).map(|entry| {
        let key = entry.key().clone();
        let value = entry.value().clone();
        let pair = Keyed::new(key, value);
        Ok::<_, anyhow::Error>(pair)
    });
    stream::iter(it)
}

pub fn build_time_dedup_iter<S, A, T, E>(
    s: S,
    timestamp_upper: T,
) -> impl Stream<Item = Result<A, E>> + Unpin + Send
where
    S: Stream<Item = Result<(A, T), E>> + Unpin + Send,
    A: PartialEq + Send,
    E: Send,
    T: PartialOrd + Copy + Send + Sync,
{
    s.try_filter(move |(_, timestamp)| {
        // todo: use binary search?
        let condition = timestamp.le(&timestamp_upper);
        ready(condition)
    })
    .dedup_by(|left, right| match (left, right) {
        (Ok((left, _)), Ok((right, _))) => left.eq(right),
        _ => false,
    })
    .map(|entry| entry.map(|pair| pair.0))
}

pub trait WatermarkGcIter {
    type Stream<S: Stream<Item = anyhow::Result<InnerEntry>>>: Stream<
        Item = anyhow::Result<InnerEntry>,
    >;

    fn build_watermark_gc_iter<S>(s: S, watermark: u64) -> Self::Stream<S>
    where
        S: Stream<Item = anyhow::Result<InnerEntry>>;
}

struct WatermarkGcIterImpl;

impl WatermarkGcIter for WatermarkGcIterImpl {
    type Stream<S: Stream<Item = anyhow::Result<InnerEntry>>> =
        impl Stream<Item = anyhow::Result<InnerEntry>>;

    fn build_watermark_gc_iter<S>(s: S, watermark: u64) -> Self::Stream<S>
    where
        S: Stream<Item = anyhow::Result<InnerEntry>>,
    {
        s.scan(None, move |state: &mut Option<KeyBytes>, entry| {
            let item = match entry {
                Err(e) => Some(Some(Err(e))),
                Ok(entry) => {
                    if let Some(prev_key) = state {
                        if prev_key.key == entry.key.key {
                            if entry.key.timestamp <= watermark {
                                Some(None)
                            } else {
                                Some(Some(Ok(entry)))
                            }
                        } else {
                            *state = Some(entry.key.clone());
                            Some(Some(Ok(entry)))
                        }
                    } else {
                        *state = Some(entry.key.clone());
                        Some(Some(Ok(entry)))
                    }
                }
            };
            ready(item)
        })
        .filter_map(|entry| async { entry })
    }
}

pub fn transform_bound<A, T>(lower: Bound<A>, upper: Bound<A>, timestamp: T) -> BoundRange<(A, T)>
where
    T: Bounded,
{
    (
        transform_lower_bound(lower, timestamp),
        transform_upper_bound(upper),
    )
}

fn transform_lower_bound<A, T>(lower: Bound<A>, timestamp: T) -> Bound<(A, T)>
where
    T: Bounded,
{
    use Bound::{Excluded, Included, Unbounded};
    match lower {
        Included(a) => Included((a, timestamp)),
        Excluded(a) => Excluded((a, T::min_value())),
        Unbounded => Unbounded,
    }
}

fn transform_upper_bound<A, T>(upper: Bound<A>) -> Bound<(A, T)>
where
    T: Bounded,
{
    use Bound::{Excluded, Included, Unbounded};
    match upper {
        Included(a) => Included((a, T::min_value())),
        Excluded(a) => Excluded((a, T::max_value())),
        Unbounded => Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::{InnerEntry, Keyed};
    use crate::iterators::utils::test_utils::assert_stream_eq;
    use crate::key::KeyBytes;
    use crate::mvcc::iterator::{build_time_dedup_iter, WatermarkGcIter, WatermarkGcIterImpl};
    use bytes::Bytes;
    use futures::{stream, Stream, StreamExt, TryStreamExt};

    #[tokio::test]
    async fn test_build_time_dedup_iter() {
        test_time_dedup_iter_helper([(Keyed::new("a", "a1"), 1)], 3, [Keyed::new("a", "a1")]).await;
        test_time_dedup_iter_helper(
            [
                (Keyed::new("a", "a1"), 1),
                (Keyed::new("a", "a2"), 2),
                (Keyed::new("b", "b3"), 3),
            ],
            2,
            [Keyed::new("a", "a2")],
        )
        .await;
        test_time_dedup_iter_helper(
            [
                (Keyed::new("a", "a1"), 1),
                (Keyed::new("a", "a2"), 2),
                (Keyed::new("a", "a3"), 3),
                (Keyed::new("b", "b2"), 2),
                (Keyed::new("c", "c1"), 1),
                (Keyed::new("c", "c3"), 3),
            ],
            2,
            [
                Keyed::new("a", "a2"),
                Keyed::new("b", "b2"),
                Keyed::new("c", "c1"),
            ],
        )
        .await;
    }

    async fn test_time_dedup_iter_helper<I, S>(s: I, timestamp_upper: u64, expected: S)
    where
        I: IntoIterator<Item = (Keyed<&'static str, &'static str>, u64)>,
        I::IntoIter: Send,
        S: IntoIterator<Item = Keyed<&'static str, &'static str>>,
    {
        let s = stream::iter(s).map(Ok::<_, ()>);
        let result: Vec<_> = build_time_dedup_iter(s, timestamp_upper)
            .try_collect()
            .await
            .unwrap();
        let expected: Vec<_> = expected.into_iter().collect();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_watermark_gc() {
        test_watermark_gc_helper([("a", 0), ("b", 0)], 5, [("a", 0), ("b", 0)]).await;
        test_watermark_gc_helper([("a", 3), ("a", 2), ("b", 0)], 2, [("a", 3), ("b", 0)]).await;
        test_watermark_gc_helper([("a", 3), ("a", 2), ("b", 5)], 2, [("a", 3), ("b", 5)]).await;
    }

    async fn test_watermark_gc_helper(
        input: impl IntoIterator<Item = (&'static str, u64)>,
        watermark: u64,
        expected_output: impl IntoIterator<Item = (&'static str, u64)>,
    ) {
        fn transform(
            iter: impl IntoIterator<Item = (&'static str, u64)>,
        ) -> impl Stream<Item = anyhow::Result<InnerEntry>> {
            let s = iter.into_iter().map(|(key, timestamp)| {
                Ok(InnerEntry::new(
                    KeyBytes::new(Bytes::from(key), timestamp),
                    Bytes::new(),
                ))
            });
            stream::iter(s)
        }

        assert_stream_eq(
            WatermarkGcIterImpl::build_watermark_gc_iter(transform(input), watermark)
                .map(Result::unwrap),
            transform(expected_output).map(Result::unwrap),
        )
        .await;
    }
}
