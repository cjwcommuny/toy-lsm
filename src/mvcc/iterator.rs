use std::collections::Bound::{Excluded, Included, Unbounded};
use std::future::ready;
use std::ops::Bound;

use async_iter_ext::StreamTools;
use futures::{Stream, StreamExt, TryStreamExt};
use num_traits::Bounded;

use crate::bound::BoundRange;

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
    #[tokio::test]
    async fn test_build_time_dedup_iter() {}
}
