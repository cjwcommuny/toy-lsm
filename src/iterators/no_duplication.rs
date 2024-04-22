use std::fmt::Debug;

use async_iter_ext::{Dedup, StreamTools};
use futures::Stream;

pub type NoDuplication<I> =
    Dedup<I, <I as Stream>::Item, fn(&<I as Stream>::Item, &<I as Stream>::Item) -> bool>;

pub fn new_no_duplication<I, Item>(iter: I) -> NoDuplication<I>
where
    I: Stream<Item = anyhow::Result<Item>>,
    Item: PartialEq + Debug,
{
    iter.dedup_by(cmp)
}

fn cmp<T, E>(left: &Result<T, E>, right: &Result<T, E>) -> bool
where
    T: PartialEq,
{
    match (left.as_ref(), right.as_ref()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use futures::stream;
    use futures::StreamExt;

    use crate::entry::Entry;
    use crate::iterators::no_duplication::new_no_duplication;

    #[derive(Debug)]
    struct Pair(Entry);

    impl PartialEq for Pair {
        fn eq(&self, other: &Self) -> bool {
            self.0.key == other.0.key && self.0.value == other.0.value
        }
    }

    impl From<Entry> for Pair {
        fn from(value: Entry) -> Self {
            Self(value)
        }
    }

    #[tokio::test]
    async fn test_empty() {
        let arr: [i32; 0] = [];
        helper(arr, arr).await;
    }

    #[tokio::test]
    async fn test_single() {
        helper([1], [1]).await;
    }

    #[tokio::test]
    async fn test_basic() {
        helper([1, 3, 2], [1, 3, 2]).await;
    }

    #[tokio::test]
    async fn test_dup() {
        helper([1, 1, 3, 4, 4, 7, 2, 2], [1, 3, 4, 7, 2]).await;
    }

    #[tokio::test]
    async fn test_dup_get_first() {
        helper(
            [
                Entry::from_slice(b"1", b"11"),
                Entry::from_slice(b"1", b"10"),
            ],
            [Pair(Entry::from_slice(b"1", b"11"))],
        )
        .await;
    }

    async fn helper<T: PartialEq + Debug, U: PartialEq + Debug + From<T>>(
        input: impl IntoIterator<Item = T> + Unpin,
        expect: impl IntoIterator<Item = U> + Unpin,
    ) {
        let a = stream::iter(input.into_iter().map(Ok::<T, anyhow::Error>));
        let expect: Vec<_> = expect.into_iter().collect();
        let iter = new_no_duplication(a).map(Result::unwrap).map(Into::into);
        assert_eq!(expect, iter.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn test_dup_2() {
        helper(
            [
                Entry::from_slice(b"0", b"2333333"),
                Entry::from_slice(b"00", b"2333"),
                Entry::from_slice(b"00", b"2333333"),
                Entry::from_slice(b"1", b""),
                Entry::from_slice(b"2", b"2333"),
                Entry::from_slice(b"3", b"23333"),
                Entry::from_slice(b"4", b""),
            ],
            [
                Pair(Entry::from_slice(b"0", b"2333333")),
                Pair(Entry::from_slice(b"00", b"2333")),
                Pair(Entry::from_slice(b"1", b"")),
                Pair(Entry::from_slice(b"2", b"2333")),
                Pair(Entry::from_slice(b"3", b"23333")),
                Pair(Entry::from_slice(b"4", b"")),
            ],
        )
        .await;
    }
}
