use std::future::Future;
use futures::{Stream, StreamExt};

pub type MaybeEmptyStream<Item, S> = Option<NonEmptyStream<Item, S>>;

pub struct NonEmptyStream<Item, S> {
    item: Item,
    stream: S,
}

impl<Item, S> NonEmptyStream<Item, S> {
    pub fn item(&self) -> &Item {
        &self.item
    }
}

impl<Item, S> NonEmptyStream<Item, S>
where
    S: Stream<Item = anyhow::Result<Item>> + Unpin,
{
    pub async fn next(self) -> (anyhow::Result<MaybeEmptyStream<Item, S>>, Item) {
        let NonEmptyStream { item, stream } = self;
        let next_stream = Self::try_new(stream).await;
        (next_stream, item)
    }

    pub async fn try_new(mut stream: S) -> anyhow::Result<Option<NonEmptyStream<Item, S>>> {
        let iter = stream
            .next()
            .await
            .transpose()?
            .map(|item| NonEmptyStream { item, stream });
        Ok(iter)
    }
}
