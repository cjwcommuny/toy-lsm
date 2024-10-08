use crate::entry::{Entry, InnerEntry};
use futures::{Stream, StreamExt};

pub fn unwrap_ts_stream<S>(s: S) -> impl Stream<Item = anyhow::Result<Entry>>
where
    S: Stream<Item = anyhow::Result<InnerEntry>>,
{
    s.map(|item| {
        item.map(|entry| Entry {
            key: entry.key.into_inner(),
            value: entry.value,
        })
    })
}
