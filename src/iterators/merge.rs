use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::future::ready;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{pin_mut, FutureExt, Stream, StreamExt};
use pin_project::pin_project;
use tracing::{error, info, Instrument};

use crate::iterators::maybe_empty::NonEmptyStream;
use crate::iterators::merge::heap::HeapWrapper;
use crate::iterators::no_duplication::{new_no_duplication, NoDuplication};

mod heap;

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub type MergeIterator<Item, I> = NoDuplication<MergeIteratorInner<Item, I>>;

pub async fn create_merge_iter<Item, I>(iters: impl Stream<Item = I>) -> MergeIterator<Item, I>
where
    Item: Ord + Debug,
    I: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    new_no_duplication(MergeIteratorInner::create(iters).await)
}

pub async fn create_merge_iter_from_non_empty_iters<Item, I>(
    iters: impl Stream<Item = NonEmptyStream<Item, I>>,
) -> MergeIterator<Item, I>
where
    Item: Ord + Debug,
    I: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    new_no_duplication(MergeIteratorInner::from_non_empty_iters(iters).await)
}

#[pin_project]
pub struct MergeIteratorInner<Item, I>
where
    Item: Ord,
{
    iters: BinaryHeap<HeapWrapper<Item, I>>,
}

impl<Item, I> MergeIteratorInner<Item, I>
where
    I: Stream<Item = anyhow::Result<Item>> + Unpin,
    Item: Ord + Debug,
{
    pub async fn create(iters: impl Stream<Item = I>) -> Self {
        let iters = iters
            .map(NonEmptyStream::try_new)
            .flat_map(FutureExt::into_stream)
            .filter_map(|x| ready(x.inspect_err(|err| error!(error = ?err)).ok().flatten()));
        Self::from_non_empty_iters(iters).await
    }

    pub async fn from_non_empty_iters(iters: impl Stream<Item = NonEmptyStream<Item, I>>) -> Self {
        let iters: BinaryHeap<_> = iters
            .enumerate()
            .map(|(index, iter)| HeapWrapper { index, iter })
            .collect()
            .await;
        Self { iters }
    }
}

impl<I, Item> Stream for MergeIteratorInner<Item, I>
where
    I: Stream<Item = anyhow::Result<Item>> + Unpin,
    Item: Ord + Debug,
{
    type Item = anyhow::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _span = tracing::info_span!("MergeIteratorInner poll next").entered();
        use Poll::{Pending, Ready};
        let this = self.project();

        let Some(current) = this.iters.pop() else {
            info!("return none");
            return Ready(None);
        };

        let fut = current
            .iter
            .next()
            .instrument(tracing::info_span!("current_iter_next"));
        pin_mut!(fut);
        let Ready((next_iter, item)) = fut.poll(cx) else {
            info!("MergeIteratorInner pending");
            return Pending;
        };

        let next_iter = match next_iter {
            Ok(next_iter) => next_iter,
            Err(e) => {
                error!(error = ?e);
                return Ready(Some(Err(e)))
            },
        };

        if let Some(next_iter) = next_iter {
            let next_wrapper = HeapWrapper {
                index: current.index,
                iter: next_iter,
            };
            this.iters.push(next_wrapper);
        }
        info!(elem = ?item);
        Ready(Some(Ok(item)))
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::future::Future;
    use std::time::Duration;

    use futures::{FutureExt, stream, StreamExt};
    use rand::Rng;
    use tokio::time::sleep;

    use crate::entry::Entry;
    use crate::iterators::merge::MergeIteratorInner;
    use crate::iterators::utils::build_stream;
    use crate::iterators::{create_merge_iter, eq};

    #[tokio::test]
    async fn test_empty() {
        let empty: [i32; 0] = [];
        helper([empty, empty, empty], []).await
    }

    #[tokio::test]
    async fn test_single() {
        helper([[1, 2, 3]], [1, 2, 3]).await
    }

    #[tokio::test]
    async fn test_basic() {
        helper(
            [[1, 3, 5], [6, 8, 10], [2, 7, 11]],
            [1, 2, 3, 5, 6, 7, 8, 10, 11],
        )
        .await
    }

    #[tokio::test]
    async fn test_same() {
        helper(
            [
                [Entry::from_slice(b"1", b"10")],
                [Entry::from_slice(b"1", b"20")],
            ],
            [
                Entry::from_slice(b"1", b"10"),
                Entry::from_slice(b"1", b"20"),
            ],
        )
        .await
    }

    async fn helper<T: Ord + Debug>(
        iters: impl IntoIterator<Item = impl IntoIterator<Item = T>>,
        expect: impl IntoIterator<Item = T>,
    ) {
        let iters = stream::iter(iters)
            .map(stream::iter)
            .map(|inner| inner.flat_map(|x| build(x).into_stream()));
        let merged = MergeIteratorInner::create(iters).await;
        let merged: Vec<_> = merged.map(Result::unwrap).collect().await;
        let expect: Vec<_> = expect.into_iter().collect();
        assert_eq!(expect, merged);
    }

    fn build<T>(x: T) -> impl Future<Output = anyhow::Result<T>> + Unpin {
        Box::pin(async move {
            sleep(Duration::from_millis(100)).await;
            Ok::<T, anyhow::Error>(x)
        })
    }

    #[tokio::test]
    async fn test_task2_merge_1() {
        let build_sub_stream = || {
            let i1 = build_stream([("a", "1.1"), ("b", "2.1"), ("c", "3.1"), ("e", "")]).map(Ok);
            let i2 = build_stream([("a", "1.2"), ("b", "2.2"), ("c", "3.2"), ("d", "4.2")]).map(Ok);
            let i3 = build_stream([("b", "2.3"), ("c", "3.3"), ("d", "4.3")]).map(Ok);
            (i1, i2, i3)
        };

        let (i1, i2, i3) = build_sub_stream();

        assert!(
            eq(
                create_merge_iter(stream::iter([i1, i2, i3]))
                    .await
                    .map(Result::unwrap),
                build_stream([
                    ("a", "1.1"),
                    ("b", "2.1"),
                    ("c", "3.1"),
                    ("d", "4.2"),
                    ("e", ""),
                ]),
            )
            .await
        );

        let (i1, i2, i3) = build_sub_stream();

        assert!(
            eq(
                create_merge_iter(stream::iter([i3, i1, i2]))
                    .await
                    .map(Result::unwrap),
                build_stream([
                    ("a", "1.1"),
                    ("b", "2.3"),
                    ("c", "3.3"),
                    ("d", "4.3"),
                    ("e", ""),
                ]),
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_task2_merge_2() {}
}
