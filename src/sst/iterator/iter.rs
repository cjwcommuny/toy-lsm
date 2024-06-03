use std::future::ready;
use std::iter::Once;
use std::ops::Bound;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{future, FutureExt};
use futures::{stream, Stream, StreamExt};
use pin_project::pin_project;
use tracing::info;

use crate::block::BlockIterator;
use crate::entry::{Entry, InnerEntry};
use crate::iterators::{iter_fut_iter_to_stream, split_first, MergeIterator, TwoMergeIterator};
use crate::key::{KeyBytes, KeySlice};
use crate::persistent::SstHandle;
use crate::sst::bloom::Bloom;
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::{bloom, BlockMeta, SsTable};

// 暂时用 box，目前 rust 不能够方便地在 struct 中存 closure
type InnerIter<'a> = Pin<Box<dyn Stream<Item = anyhow::Result<InnerEntry>> + Send + 'a>>;

fn build_iter<'a, File>(
    table: &'a SsTable<File>,
    lower: Bound<KeySlice<'a>>,
    upper: Bound<KeySlice<'a>>,
) -> impl Stream<Item = anyhow::Result<InnerEntry>> + Send + 'a
where
    File: SstHandle,
{
    let iter = match lower {
        Bound::Included(key) => future::Either::Left(future::Either::Left(build_bounded_iter(
            table,
            KeySlice::from_slice(key),
            upper,
            |meta: &BlockMeta, key| meta.last_key.raw_ref() < key,
        ))),
        Bound::Excluded(key) => future::Either::Left(future::Either::Right(build_bounded_iter(
            table,
            KeySlice::from_slice(key),
            upper,
            |meta, key| meta.last_key.raw_ref() <= key,
        ))),
        Bound::Unbounded => future::Either::Right(build_unbounded_iter(table)),
    };
    match upper {
        Bound::Included(upper) => future::Either::Left(future::Either::Left(transform_stop_iter(
            iter,
            upper,
            |a, b| a <= b,
        ))),
        Bound::Excluded(upper) => future::Either::Left(future::Either::Right(transform_stop_iter(
            iter,
            upper,
            |a, b| a < b,
        ))),
        Bound::Unbounded => future::Either::Right(iter),
    }
}

fn transform_stop_iter<'a>(
    iter: impl Stream<Item = anyhow::Result<Entry>> + 'a,
    upper: &'a [u8],
    f: for<'b> fn(&'b [u8], &'b [u8]) -> bool,
) -> impl Stream<Item = anyhow::Result<Entry>> + 'a {
    iter.take_while(move |entry| {
        let condition = entry
            .as_ref()
            .map(|entry| f(&entry.key, upper))
            .unwrap_or(true);
        ready(condition)
    })
}

fn build_bounded_iter<'a, File>(
    table: &'a SsTable<File>,
    low: KeySlice<'a>,
    upper: Bound<&'a [u8]>,
    partition: impl for<'c> Fn(&'c BlockMeta, &'c [u8]) -> bool,
) -> impl Stream<Item = anyhow::Result<Entry>> + 'a
where
    File: SstHandle,
{
    let index = table
        .block_meta
        .as_slice()
        .partition_point(|meta| partition(meta, low.raw_ref()));

    let metas = table.block_meta[index..]
        .iter()
        .map(BlockMeta::first_key)
        .map(KeyBytes::raw_ref);
    let metas = (index..).zip(metas);

    let Some(((head_index, _), tail)) = split_first(metas) else {
        return future::Either::Left(stream::empty());
    };

    let head = table
        .get_block_iter_with_key(head_index, low)
        .into_stream()
        .flat_map(stream::iter);
    let tail = tail
        .take_while(move |(_, first_key)| match &upper {
            Bound::Included(upper) => first_key <= upper,
            Bound::Excluded(upper) => first_key < upper,
            Bound::Unbounded => true,
        })
        .map(|(index, _)| table.get_block_iter(index));
    let tail = iter_fut_iter_to_stream(tail);
    let iter = head.chain(tail);

    future::Either::Right(iter)
}

fn build_unbounded_iter<File>(
    table: &SsTable<File>,
) -> impl Stream<Item = anyhow::Result<Entry>> + '_
where
    File: SstHandle,
{
    let iter = (0..table.block_meta.len()).map(|block_index| table.get_block_iter(block_index));
    iter_fut_iter_to_stream(iter)
}

#[pin_project]
pub struct SsTableIterator<'a, File> {
    table: &'a SsTable<File>,
    #[pin]
    inner: InnerIter<'a>,
    bloom: Option<&'a Bloom>,
}

impl<'a, File> SsTableIterator<'a, File> {
    pub fn may_contain(&self, key: &[u8]) -> bool {
        true
        // todo
        // bloom::may_contain(self.bloom, key)
    }
}

impl<'a, File> SsTableIterator<'a, File>
where
    File: SstHandle,
{
    pub fn create_and_seek_to_first(table: &'a SsTable<File>) -> Self {
        Self::scan(table, Bound::Unbounded, Bound::Unbounded)
    }
}

impl<'a, File> SsTableIterator<'a, File>
where
    File: SstHandle,
{
    pub fn scan(table: &'a SsTable<File>, lower: Bound<KeySlice<'a>>, upper: Bound<KeySlice<'a>>) -> Self {
        let iter = build_iter(table, lower, upper);
        let this = Self {
            table,
            inner: Box::pin(iter) as _,
            bloom: table.bloom.as_ref(),
        };
        this
    }
}

// todo: 感觉没必要 impl Stream，使用 (Bloom, InnerIter) 比较好？
impl<'a, File> Stream for SsTableIterator<'a, File> {
    type Item = anyhow::Result<InnerEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let inner = this.inner;

        inner.poll_next(cx)
    }
}

pub type BlockFallibleIter = either::Either<BlockIterator, Once<anyhow::Result<InnerEntry>>>;

pub type MergedSstIterator<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, SsTableIterator<'a, File>>,
    MergeIterator<Entry, SstConcatIterator<'a>>,
>;

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;
    use futures::StreamExt;
    use nom::AsBytes;
    use tempfile::tempdir;

    use crate::sst::builder::{generate_sst, key_of, num_of_keys, value_of};
    use crate::sst::iterator::SsTableIterator;

    #[tokio::test]
    async fn test_sst_seek_key() {
        let dir = tempdir().unwrap();
        let sst = generate_sst(&dir).await;
        let mut iter = SsTableIterator::scan(&sst, Bound::Unbounded, Bound::Unbounded);
        for i in 0..num_of_keys() {
            let entry = iter
                .next()
                .await
                .unwrap()
                .unwrap_or_else(|_| panic!("panic on {}", i));
            let key = entry.key.as_bytes();
            let value = entry.value.as_bytes();
            assert_eq!(
                key,
                key_of(i).for_testing_key_ref(),
                "expected key: {:?}, actual key: {:?}",
                Bytes::copy_from_slice(key_of(i).for_testing_key_ref()),
                Bytes::copy_from_slice(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                Bytes::copy_from_slice(&value_of(i)),
                Bytes::copy_from_slice(value)
            );
        }
    }
}
