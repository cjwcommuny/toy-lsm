use async_iter_ext::StreamTools;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use num_traits::Bounded;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::future::ready;
use std::ops::Bound;
use std::sync::Arc;

use crate::bound::BoundRange;
use crate::entry::{Entry, Keyed};
use crate::iterators::lsm::LsmIterator;
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{create_two_merge_iter, LockedLsmIter};
use crate::persistent::Persistent;

pub struct LockedTxnIter<'a, P: Persistent> {
    local_storage: arc_swap::Guard<Arc<SkipMap<Bytes, Bytes>>>,
    lsm_iter: LockedLsmIter<'a, P>,
}

impl<'a, P: Persistent> LockedTxnIter<'a, P> {
    pub async fn iter(&'a self) -> anyhow::Result<LsmIterator<'a>> {
        let lsm_iter = self.lsm_iter.iter_with_delete().await?;
        let local_iter = txn_local_iterator(
            self.local_storage.as_ref(),
            self.lsm_iter.lower.map(Bytes::copy_from_slice),
            self.lsm_iter.upper.map(Bytes::copy_from_slice),
        );
        let merged = create_two_merge_iter(local_iter, lsm_iter).await?;
        let iter = new_no_deleted_iter(merged);
        let iter = Box::new(iter) as _;
        Ok(iter)
    }
}

pub fn txn_local_iterator<'a>(
    map: &'a SkipMap<Bytes, Bytes>,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
) -> impl Stream<Item = anyhow::Result<Entry>> + Unpin + Send + 'a {
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

pub fn transform_bound<A, T>(lower: Bound<A>, upper: Bound<A>, timestamp: T) -> BoundRange<(A, T)>
where
    T: Bounded,
{
    (
        transform_lower_bound(lower),
        transform_upper_bound(upper, timestamp),
    )
}

fn transform_lower_bound<A, T>(lower: Bound<A>) -> Bound<(A, T)>
where
    T: Bounded,
{
    use Bound::{Excluded, Included, Unbounded};
    match lower {
        Included(a) => Included((a, T::min_value())),
        Excluded(a) => Excluded((a, T::max_value())),
        Unbounded => Unbounded,
    }
}

fn transform_upper_bound<A, T>(upper: Bound<A>, timestamp: T) -> Bound<(A, T)>
where
    T: Bounded,
{
    use Bound::{Excluded, Included, Unbounded};
    match upper {
        Included(a) => Included((a, timestamp)),
        Excluded(a) => Excluded((a, T::min_value())),
        Unbounded => Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::Keyed;
    use crate::mvcc::iterator::build_time_dedup_iter;
    use futures::{stream, StreamExt, TryStreamExt};

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
        let s = stream::iter(s).map(|pair| Ok::<_, ()>(pair));
        let result: Vec<_> = build_time_dedup_iter(s, timestamp_upper)
            .try_collect()
            .await
            .unwrap();
        let expected: Vec<_> = expected.into_iter().collect();
        assert_eq!(result, expected);
    }
}
