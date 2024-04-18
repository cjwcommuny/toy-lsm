use std::fmt::Debug;
use std::future::Future;
use std::mem;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use futures::Stream;
use pin_project::pin_project;
use tracing::info;

use crate::iterators::no_duplication::{new_no_duplication, NoDuplication};
use crate::iterators::{MaybeEmptyStream, NonEmptyStream};

// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub type TwoMergeIterator<Item, A, B> = NoDuplication<TwoMergeIteratorInner<Item, A, B>>;

pub async fn create_two_merge_iter<Item, A, B>(
    a: A,
    b: B,
) -> anyhow::Result<TwoMergeIterator<Item, A, B>>
where
    Item: Ord + Debug + Unpin,
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    let inner = TwoMergeIteratorInner::create(a, b).await?;
    Ok(new_no_duplication(inner))
}

#[pin_project]
pub struct TwoMergeIteratorInner<Item, A, B> {
    #[pin]
    a: MaybeEmptyStream<Item, A>,
    #[pin]
    b: MaybeEmptyStream<Item, B>,
}

impl<Item, A, B> TwoMergeIteratorInner<Item, A, B>
where
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    pub async fn create(a: A, b: B) -> anyhow::Result<Self> {
        let a = NonEmptyStream::try_new(a).await?;
        let b = NonEmptyStream::try_new(b).await?;
        let s = Self { a, b };
        Ok(s)
    }
}

impl<Item, A, B> Stream for TwoMergeIteratorInner<Item, A, B>
where
    A: Stream<Item = anyhow::Result<Item>> + Unpin,
    B: Stream<Item = anyhow::Result<Item>> + Unpin,
    Item: Ord + Debug + Unpin,
{
    type Item = anyhow::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::Ready;

        let this = self.project();
        let a_opt = Pin::into_inner(this.a);
        let b_opt = Pin::into_inner(this.b);
        let a = mem::take(a_opt);
        let b = mem::take(b_opt);

        match (a, b) {
            (None, None) => Ready(None),
            (Some(a), None) => handle_next(cx, a, a_opt, None, b_opt),
            (None, Some(b)) => handle_next(cx, b, b_opt, None, a_opt),
            (Some(a), Some(b)) => {
                if a.item() < b.item() {
                    handle_next(cx, a, a_opt, Some(b), b_opt)
                } else {
                    handle_next(cx, b, b_opt, Some(a), a_opt)
                }
            }
        }
    }
}

fn handle_next<Item, I, A>(
    cx: &mut Context<'_>,
    iter1: NonEmptyStream<Item, I>,
    position1: &mut MaybeEmptyStream<Item, I>,
    iter2: A,
    position2: &mut A,
) -> Poll<Option<anyhow::Result<Item>>>
where
    I: Stream<Item = anyhow::Result<Item>> + Unpin,
    Item: Debug,
{
    use Poll::{Pending, Ready};

    let _ = mem::replace(position2, iter2);
    let fut = pin!(iter1.next());
    match fut.poll(cx) {
        Pending => Pending,
        Ready((new_a, item)) => match new_a {
            Ok(new_a) => {
                let _ = mem::replace(position1, new_a);
                info!(elem = ?item, "two_merge");
                Ready(Some(Ok(item)))
            }
            Err(e) => Ready(Some(Err(e))),
        },
    }
}

#[cfg(test)]
mod test {
    use crate::iterators::create_two_merge_iter;
    use futures::{stream, StreamExt};

    use std::fmt::Debug;

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
