// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use std::fmt::Debug;
use std::future::Future;

use futures::stream::unfold;
use futures::{Stream, StreamExt};

use crate::iterators::no_duplication::{new_no_duplication, NoDuplication};
use crate::iterators::{MaybeEmptyStream, NonEmptyStream};

// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub type TwoMergeIterator<Item, A, B> = NoDuplication<TwoMergeIterInner<Item, A, B>>;

pub async fn create_two_merge_iter<Item, A, B>(
    a: A,
    b: B,
) -> anyhow::Result<NoDuplication<TwoMergeIterInner<Item, A, B>>>
where
    Item: Ord + Debug + Unpin,
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    let inner = create_inner(a, b).await?;
    Ok(new_no_duplication(inner))
}

pub type TwoMergeIterInner<
    Item: Ord + Debug + Unpin,
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
> = impl Stream<Item = anyhow::Result<Item>> + Unpin;
pub async fn create_inner<A, B, Item>(a: A, b: B) -> anyhow::Result<TwoMergeIterInner<Item, A, B>>
where
    Item: Ord + Debug + Unpin,
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    let a = NonEmptyStream::try_new(a).await?;
    let b = NonEmptyStream::try_new(b).await?;
    let x = unfold((a, b), unfold_fn);
    Ok(Box::pin(x))
}

async fn unfold_fn<A, B, Item>(
    state: (MaybeEmptyStream<Item, A>, MaybeEmptyStream<Item, B>),
) -> Option<(
    anyhow::Result<Item>,
    (MaybeEmptyStream<Item, A>, MaybeEmptyStream<Item, B>),
)>
where
    Item: Ord + Debug + Unpin,
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    match state {
        (None, None) => None,
        (Some(a), None) => {
            let (item, next_a) = handle_next(a).await;
            Some((item, (next_a, None)))
        }
        (None, Some(b)) => {
            let (item, next_b) = handle_next(b).await;
            Some((item, (None, next_b)))
        }
        (Some(a), Some(b)) => {
            if a.item() <= b.item() {
                let (item, next_a) = handle_next(a).await;
                Some((item, (next_a, Some(b))))
            } else {
                let (item, next_b) = handle_next(b).await;
                Some((item, (Some(a), next_b)))
            }
        }
    }
}

async fn handle_next<Item, S>(
    s: NonEmptyStream<Item, S>,
) -> (anyhow::Result<Item>, MaybeEmptyStream<Item, S>)
where
    S: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    let (next_s, item) = s.next().await;
    match next_s {
        Ok(next_s) => (Ok(item), next_s),
        Err(e) => (Err(e), None),
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use futures::{stream, StreamExt};

    use crate::iterators::create_two_merge_iter;

    #[tokio::test]
    async fn test_simple() {
        helper([1], [2], vec![1, 2]).await
    }
    #[tokio::test]
    async fn test_simple2() {
        helper([2], [1], vec![1, 2]).await
    }

    #[tokio::test]
    async fn test_single() {
        helper([], [1], vec![1]).await;
        helper([1], [], vec![1]).await;
    }

    #[tokio::test]
    async fn test_basic() {
        helper([1, 3, 6], [2, 4, 5], vec![1, 2, 3, 4, 5, 6]).await
    }

    async fn helper<T: Debug + Ord + Unpin>(
        a: impl IntoIterator<Item = T> + Unpin,
        b: impl IntoIterator<Item = T> + Unpin,
        expect: impl IntoIterator<Item = T> + Unpin,
    ) {
        let a = stream::iter(a.into_iter().map(Ok::<T, anyhow::Error>));
        let b = stream::iter(b.into_iter().map(Ok::<T, anyhow::Error>));
        let expect: Vec<_> = expect.into_iter().collect();
        let iter = create_two_merge_iter(a, b)
            .await
            .unwrap()
            .map(Result::unwrap);
        assert_eq!(expect, iter.collect::<Vec<_>>().await);
    }
}
