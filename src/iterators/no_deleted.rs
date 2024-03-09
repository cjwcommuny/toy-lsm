use std::future::{ready, Ready};

use futures::stream::FilterMap;
use futures::{Stream, StreamExt};

use crate::entry::Entry;

pub type NoDeletedIterator<I, E> = FilterMap<
    I,
    Ready<Option<Result<Entry, E>>>,
    fn(Result<Entry, E>) -> Ready<Option<Result<Entry, E>>>,
>;

pub fn new_no_deleted_iter<I, E>(stream: I) -> NoDeletedIterator<I, E>
where
    I: Stream<Item = Result<Entry, E>>,
{
    let f = |item: Result<Entry, E>| {
        let item = item
            .map(|entry: Entry| (!entry.value.is_empty()).then_some(entry))
            .transpose();
        ready(item)
    };
    stream.filter_map(f as fn(_) -> _)
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use futures::stream;
    use futures::stream::StreamExt;

    use crate::entry::Entry;
    use crate::iterators::no_deleted::new_no_deleted_iter;

    #[tokio::test]
    async fn test_empty() {
        helper([], []).await;
    }

    #[tokio::test]
    async fn test_single() {
        helper(
            [Entry::from_slice(b"1", b"10")],
            [Entry::from_slice(b"1", b"10")],
        )
        .await;
    }

    #[tokio::test]
    async fn test_single_deleted() {
        helper([Entry::from_slice(b"1", b"")], []).await;
    }

    #[tokio::test]
    async fn test_deleted() {
        helper(
            [Entry::from_slice(b"1", b""), Entry::from_slice(b"2", b"20")],
            [Entry::from_slice(b"2", b"20")],
        )
        .await;
    }

    async fn helper(
        input: impl IntoIterator<Item = Entry> + Unpin,
        expect: impl IntoIterator<Item = Entry> + Unpin,
    ) {
        let a = stream::iter(input.into_iter().map(Ok::<Entry, anyhow::Error>));
        let expect: Vec<_> = expect.into_iter().collect();
        let iter = new_no_deleted_iter(a).map(Result::unwrap);
        assert_eq!(expect, iter.collect::<Vec<_>>().await);
    }
}
