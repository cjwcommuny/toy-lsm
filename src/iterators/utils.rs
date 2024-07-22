use std::future::Future;
use std::iter::Map;
use std::iter::Once;

use std::iter;

use either::Either;
use futures::future::IntoStream;
use futures::stream::{FlatMap, Flatten, Iter};
use futures::{stream, FutureExt, Stream, StreamExt};

pub fn transpose_try_iter<I, T, E>(iterator: Result<I, E>) -> Either<I, Once<Result<T, E>>>
where
    I: Iterator<Item = Result<T, E>>,
{
    match iterator {
        Ok(iterator) => Either::Left(iterator),
        Err(e) => Either::Right(iter::once(Err(e))),
    }
}

pub fn split_first<I>(mut iterator: I) -> Option<(I::Item, I)>
where
    I: Iterator,
{
    let head = iterator.next()?;
    Some((head, iterator))
}

type IterFutIterToStreamReturn<OuterIter, Fut, InnerIter> = FlatMap<
    IterFutToStreamReturn<OuterIter, Fut>,
    Iter<InnerIter>,
    fn(InnerIter) -> Iter<InnerIter>,
>;
pub fn iter_fut_iter_to_stream<OuterIter, Fut, InnerIter>(
    iterator: OuterIter,
) -> IterFutIterToStreamReturn<OuterIter, Fut, InnerIter>
where
    OuterIter: Iterator<Item = Fut>,
    Fut: Future<Output = InnerIter>,
    InnerIter: Iterator,
{
    iter_fut_to_stream(iterator).flat_map(stream::iter)
}

type IterFutToStreamReturn<OuterIter, Fut> =
    Flatten<Iter<Map<OuterIter, fn(Fut) -> IntoStream<Fut>>>>;
pub fn iter_fut_to_stream<OuterIter, Fut, T>(
    iterator: OuterIter,
) -> IterFutToStreamReturn<OuterIter, Fut>
where
    OuterIter: Iterator<Item = Fut>,
    Fut: Future<Output = T>,
{
    stream::iter(iterator.map(FutureExt::into_stream as fn(_) -> _)).flatten()
}

#[cfg(test)]
pub mod test_utils {
    use crate::entry::Entry;
    use bytes::Bytes;
    use futures::stream::Iter;
    use futures::{stream, Stream, StreamExt};
    use std::fmt::Debug;
    use std::pin::pin;
    use std::vec;

    pub type EntryStream = Iter<vec::IntoIter<Entry>>;

    pub fn build_stream<'a>(source: impl IntoIterator<Item = (&'a str, &'a str)>) -> EntryStream {
        let s: Vec<_> = source
            .into_iter()
            .map(|(key, value)| Entry::from_slice(key.as_bytes(), value.as_bytes()))
            .collect();
        stream::iter(s)
    }

    pub fn build_tuple_stream<'a>(
        source: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> impl Stream<Item = (Bytes, Bytes)> {
        let s: Vec<_> = source
            .into_iter()
            .map(|(key, value)| {
                (
                    Bytes::copy_from_slice(key.as_bytes()),
                    Bytes::copy_from_slice(value.as_bytes()),
                )
            })
            .collect();
        stream::iter(s)
    }

    pub async fn assert_stream_eq<S1, S2>(s1: S1, s2: S2)
    where
        S1: Stream,
        S2: Stream,
        S1::Item: PartialEq<S2::Item> + Debug,
        S2::Item: Debug,
    {
        let s1: Vec<_> = s1.collect().await;
        let s2: Vec<_> = s2.collect().await;
        assert_eq!(s1, s2);
    }

    pub async fn eq<S1, S2>(s1: S1, s2: S2) -> bool
    where
        S1: Stream,
        S2: Stream,
        S1::Item: PartialEq<S2::Item> + Debug,
        S2::Item: Debug,
    {
        let mut s1 = pin!(s1);
        let mut s2 = pin!(s2);
        loop {
            match (s1.next().await, s2.next().await) {
                (Some(x1), Some(x2)) => {
                    if x1 != x2 {
                        dbg!((x1, x2));
                        return false;
                    }
                }
                (Some(_), None) | (None, Some(_)) => return false,
                (None, None) => return true,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::iterators::utils::test_utils::eq;
    use futures::stream;
    use std::fmt::Debug;

    #[tokio::test]
    async fn test_eq() {
        assert!(iter_eq([1], [1],).await);
        assert!(!iter_eq([1], [2],).await);
        assert!(iter_eq([] as [i32; 0], [],).await);
        assert!(!iter_eq([1, 2, 3], [1, 2],).await);
    }

    async fn iter_eq<T: PartialEq + Debug>(
        s1: impl IntoIterator<Item = T>,
        s2: impl IntoIterator<Item = T>,
    ) -> bool {
        eq(stream::iter(s1), stream::iter(s2)).await
    }
}
