use std::future::Future;
use std::iter;
use std::iter::Map;
use std::iter::Once;

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
