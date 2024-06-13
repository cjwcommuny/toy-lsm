use std::future::ready;
use std::ops::Bound;

use crate::bound::BoundRange;
use async_iter_ext::StreamTools;
use futures::{Stream, StreamExt, TryStreamExt};
use num_traits::Bounded;

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
    T: Bounded + Clone,
{
    use Bound::{Excluded, Included, Unbounded};

    let lower = match lower {
        Included(a) => Included((a, timestamp.clone())),
        Excluded(a) => Excluded((a, T::min_value())),
        Unbounded => Unbounded,
    };

    let upper = match upper {
        Included(a) => Included((a, T::min_value())),
        Excluded(a) => Excluded((a, T::max_value())),
        Unbounded => Unbounded,
    };

    (lower, upper)
}
