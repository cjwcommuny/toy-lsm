use std::iter;

use bytes::Bytes;
use crossbeam_skiplist::map;
use futures::stream;

use crate::bound::BytesBound;
use crate::entry::InnerEntry;
use crate::iterators::{MaybeEmptyStream, NonEmptyStream, OkIter};
use crate::key::KeyBytes;

pub type MemTableIterator<'a> = stream::Iter<OkIter<ClonedSkipMapRangeIter<'a>>>;
type ClonedSkipMapRangeIter<'a> =
    iter::Map<SkipMapRangeIter<'a>, for<'b> fn(map::Entry<'b, KeyBytes, Bytes>) -> InnerEntry>;

pub fn new_memtable_iter(iter: SkipMapRangeIter) -> MemTableIterator {
    let iter = iter.map(convert_entry as for<'a> fn(map::Entry<'a, KeyBytes, Bytes>) -> _);
    stream::iter(OkIter::new(iter))
}

fn convert_entry(x: map::Entry<'_, KeyBytes, Bytes>) -> InnerEntry {
    InnerEntry {
        key: x.key().clone(),
        value: x.value().clone(),
    }
}

type SkipMapRangeIter<'a> = map::Range<'a, KeyBytes, BytesBound, KeyBytes, Bytes>;

pub type NonEmptyMemTableIterRef<'a> = NonEmptyStream<InnerEntry, MemTableIterator<'a>>;
pub type MaybeEmptyMemTableIterRef<'a> = MaybeEmptyStream<InnerEntry, MemTableIterator<'a>>;

#[cfg(test)]
mod test {
    use std::collections::Bound;

    use futures::{stream, Stream, StreamExt};
    use nom::AsBytes;

    use crate::entry::Entry;
    use crate::iterators::create_merge_iter_from_non_empty_iters;
    use crate::memtable::MemTable;
    use crate::persistent::interface::WalHandle;
    use crate::persistent::wal_handle::WalFile;

    #[tokio::test]
    async fn test_task1_memtable_iter() {
        use std::ops::Bound;
        let memtable: MemTable<WalFile> = MemTable::create(0);
        memtable
            .for_testing_put_slice(b"key1", b"value1")
            .await
            .unwrap();
        memtable
            .for_testing_put_slice(b"key2", b"value2")
            .await
            .unwrap();
        memtable
            .for_testing_put_slice(b"key3", b"value3")
            .await
            .unwrap();

        {
            let mut iter = get_memtable_iter(&memtable, Bound::Unbounded, Bound::Unbounded).await;

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key1");
            assert_eq!(entry.value.as_bytes(), b"value1");

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key2");
            assert_eq!(entry.value.as_bytes(), b"value2");

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key3");
            assert_eq!(entry.value.as_bytes(), b"value3");

            assert!(iter.next().await.is_none());
        }

        {
            let mut iter = get_memtable_iter(
                &memtable,
                Bound::Included(b"key1"),
                Bound::Included(b"key2"),
            )
            .await;

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key1");
            assert_eq!(entry.value.as_bytes(), b"value1");

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key2");
            assert_eq!(entry.value.as_bytes(), b"value2");

            assert!(iter.next().await.is_none());
        }

        {
            let mut iter = get_memtable_iter(
                &memtable,
                Bound::Excluded(b"key1"),
                Bound::Excluded(b"key3"),
            )
            .await;

            let entry = iter.next().await.unwrap().unwrap();
            assert_eq!(entry.key.as_bytes(), b"key2");
            assert_eq!(entry.value.as_bytes(), b"value2");

            assert!(iter.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_task1_empty_memtable_iter() {
        use std::ops::Bound;
        let memtable: MemTable<WalFile> = MemTable::create(0);

        let _ = memtable
            .for_testing_scan_slice(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"))
            .await
            .unwrap();
        let _ = memtable
            .for_testing_scan_slice(Bound::Included(b"key1"), Bound::Included(b"key2"))
            .await
            .unwrap();
        let _ = memtable
            .for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded)
            .await
            .unwrap();
    }

    async fn get_memtable_iter<'a, W: WalHandle>(
        memtable: &'a MemTable<W>,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> impl Stream<Item = anyhow::Result<Entry>> + Send + 'a {
        let iter = memtable.for_testing_scan_slice(lower, upper).await.unwrap();

        create_merge_iter_from_non_empty_iters(stream::iter(iter.into_iter())).await
    }
}
