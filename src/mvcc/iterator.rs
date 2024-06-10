use std::ops::Bound;

use async_iter_ext::StreamTools;
use futures::{Stream, StreamExt, TryStreamExt};
use num_traits::Bounded;

pub fn build_time_dedup_iter<S, A, T, E>(
    s: S,
    timestamp_upper: T,
) -> impl Stream<Item = Result<A, E>> + Unpin + Send
where
    S: Stream<Item = Result<(A, T), E>> + Unpin + Send,
    A: PartialEq,
    T: PartialOrd + Copy,
{
    let s = s
        .try_filter(|(_, timestamp)| async { timestamp.le(&timestamp_upper) })
        .dedup_by(|left, right| match (left, right) {
            (Ok((left, _)), Ok((right, _))) => left.eq(right),
            _ => false,
        })
        .map(|entry| entry.map(|pair| pair.0));
    s
}

pub fn transform_bound<A, T>(
    lower: Bound<A>,
    upper: Bound<A>,
    timestamp: T,
) -> (Bound<(A, T)>, Bound<(A, T)>)
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
