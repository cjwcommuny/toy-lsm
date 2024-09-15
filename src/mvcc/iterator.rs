use crate::bound::BoundRange;
use crate::entry::{Entry, InnerEntry, Keyed};
use crate::iterators::inspect::{InspectIter, InspectIterImpl};
use crate::iterators::lsm::{LsmIterImpl, LsmIterator};
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{create_two_merge_iter, LsmWithRange};
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
pub struct TxnLsmIter<'a, P: Persistent> {
    state: TxnWithRange2<'a, P>,

    #[borrows(state)]
    #[covariant]
    iter: LsmIterator<'this>,
}

// impl<'a, P: Persistent> TxnLsmIter<'a, P> {
//     pub async fn try_build(txn: Transaction<'a, P>, lower: Bound<&'a [u8]>, upper: Bound<&'a [u8]>) -> anyhow::Result<Self> {
//         let state = TxnWithRange2{txn, lower, upper};
//         Self::try_new_async(state, |state| state.txn.scan(state.lower, state.upper))
//     }
// }

#[self_referencing]
pub struct LockedTxnIterWithTxn<'a, P: Persistent> {
    txn: TxnWithBound<'a, P>,

    #[borrows(txn)]
    #[covariant]
    iter: TxnWithRange<'this, P>,
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

pub struct TxnWithRange2<'a, P: Persistent> {
    txn: Transaction<'a, P>,
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
}

// impl<'a, P: Persistent> TxnWithRange2<'a, P> {
//     pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
//         let lsm_iter = LsmIterImpl::try_build()
//     }
// }


#[derive(new)]
pub struct TxnWithRange<'a, P: Persistent> {
    local_storage: &'a SkipMap<Bytes, Bytes>,
    lsm_iter: LsmWithRange<'a, P>,
    key_hashes: Option<&'a ScopedMutex<RWSet>>,
}

impl<'a, P: Persistent> TxnWithRange<'a, P> {
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
    type Stream<S: Stream<Item = anyhow::Result<InnerEntry>> + Send + Unpin>: Stream<Item = anyhow::Result<InnerEntry>>
        + Send
        + Unpin;

    fn build_watermark_gc_iter<S>(s: S, watermark: u64) -> Self::Stream<S>
    where
        S: Stream<Item = anyhow::Result<InnerEntry>> + Send + Unpin;
}

pub struct WatermarkGcIterImpl;

impl WatermarkGcIter for WatermarkGcIterImpl {
    type Stream<S: Stream<Item = anyhow::Result<InnerEntry>> + Send + Unpin> =
        impl Stream<Item = anyhow::Result<InnerEntry>> + Send + Unpin;

    fn build_watermark_gc_iter<S>(s: S, watermark: u64) -> Self::Stream<S>
    where
        S: Stream<Item = anyhow::Result<InnerEntry>> + Send + Unpin,
    {
        let result = s
            .scan(None, move |state: &mut Option<KeyBytes>, entry| {
                let item = Some(build_item(state, entry, watermark));
                ready(item)
            })
            .filter_map(|entry| async { entry });

        // todo: remove Box::pin?
        Box::pin(result)
    }
}

fn build_item(
    state: &mut Option<KeyBytes>,
    entry: anyhow::Result<InnerEntry>,
    watermark: u64,
) -> Option<anyhow::Result<InnerEntry>> {
    match entry {
        Err(e) => Some(Err(e)),
        Ok(entry) => {
            if let Some(prev_key) = state {
                if prev_key.key == entry.key.key {
                    return None;
                }
            }
            if entry.key.timestamp > watermark {
                Some(Ok(entry))
            } else {
                *state = Some(entry.key.clone());
                if entry.value.is_empty() {
                    // is delete
                    None
                } else {
                    Some(Ok(entry))
                }
            }
        }
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
        test_watermark_gc_helper(
            [("a", "a", 0), ("b", "b", 0)],
            5,
            [("a", "a", 0), ("b", "b", 0)],
        )
        .await;
        test_watermark_gc_helper(
            [("a", "a", 3), ("a", "a", 2), ("b", "b", 0)],
            2,
            [("a", "a", 3), ("a", "a", 2), ("b", "b", 0)],
        )
        .await;
        test_watermark_gc_helper(
            [("a", "a", 3), ("a", "a", 2), ("b", "b", 5)],
            2,
            [("a", "a", 3), ("a", "a", 2), ("b", "b", 5)],
        )
        .await;
        test_watermark_gc_helper(
            [("a", "a", 2), ("a", "a", 1), ("b", "b", 5)],
            3,
            [("a", "a", 2), ("b", "b", 5)],
        )
        .await;
        test_watermark_gc_helper([("a", "", 2), ("a", "a", 1)], 2, []).await;
    }

    async fn test_watermark_gc_helper<S1, S2>(input: S1, watermark: u64, expected_output: S2)
    where
        S1: IntoIterator<Item = (&'static str, &'static str, u64)>,
        S1::IntoIter: Send,
        S2: IntoIterator<Item = (&'static str, &'static str, u64)>,
        S2::IntoIter: Send,
    {
        fn transform(
            iter: impl IntoIterator<Item = (&'static str, &'static str, u64)>,
        ) -> impl Stream<Item = anyhow::Result<InnerEntry>> {
            let s = iter.into_iter().map(|(key, value, timestamp)| {
                Ok(InnerEntry::new(
                    KeyBytes::new(Bytes::from(key), timestamp),
                    Bytes::from(value),
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
