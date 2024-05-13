use crate::entry::Entry;
use crate::iterators::merge::MergeIteratorInner;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, iter_fut_to_stream,
    MergeIterator, NonEmptyStream,
};
use crate::key::KeySlice;
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::future::{ready, Future};
use std::ops::{Range, RangeBounds};
use std::sync::Arc;
use tracing::error;

use crate::persistent::{SstHandle, SstPersistent};
use crate::sst::iterator::{create_sst_concat_and_seek_to_first, SsTableIterator};
use crate::sst::{SsTable, SsTableBuilder, SstOptions, Sstables};

#[derive(Serialize, Deserialize)]
pub struct CompactionTask {}

pub fn apply_compaction<File: SstHandle>(
    sstables: &mut Sstables<File>,
    source: Range<usize>,
    source_level: usize,
    destination_level: usize,
    new_sst: Vec<Arc<SsTable<File>>>,
) {
    // handle source
    {
        let source_ids = sstables.table_ids(source_level).clone();
        let source_ids = &source_ids[source.clone()];

        for id in source_ids {
            sstables.sstables.remove(id);
        }

        sstables.table_ids_mut(source_level).splice(source, []);
    }

    // handle destination
    {
        let destination_ids = sstables.table_ids(destination_level).clone();
        for id in &destination_ids {
            sstables.sstables.remove(id);
        }

        sstables
            .table_ids_mut(destination_level)
            .splice(.., new_sst.iter().map(|table| *table.id()));

        for table in new_sst {
            sstables.sstables.insert(*table.id(), table);
        }
    }
}

// todo: return Stream<Item = Arc<SsTable<File>>>
pub async fn compact_generate_new_sst<'a, P: SstPersistent, U, L>(
    upper_sstables: U,
    lower_sstables: L,
    next_sst_id: impl Fn() -> usize + Send + 'a + Sync,
    options: &'a SstOptions,
    persistent: &'a P,
) -> anyhow::Result<Vec<Arc<SsTable<P::Handle>>>>
where
    U: IntoIterator<Item = &'a SsTable<P::Handle>> + Send + 'a,
    U::IntoIter: Send,
    L: IntoIterator<Item = &'a SsTable<P::Handle>> + Send + 'a,
    L::IntoIter: Send,
{
    // todo: non-zero level should use concat iterator
    let l0 = {
        let iters = stream::iter(upper_sstables.into_iter())
            .map(SsTableIterator::create_and_seek_to_first)
            .map(Box::new);

        MergeIteratorInner::create(iters).await
    };

    let l1 = {
        let tables = lower_sstables.into_iter().collect();
        create_sst_concat_and_seek_to_first(tables)
    }?;
    let iter = assert_send(create_two_merge_iter(l0, l1)).await?;
    let s: Vec<_> = assert_send(
        stream::unfold(iter, |mut iter| async {
            let id = next_sst_id();
            let b = batch(
                &mut iter,
                id,
                *options.block_size(),
                *options.target_sst_size(),
                persistent,
            )
            .await?;
            Some((b, iter))
        })
        .collect(),
    )
    .await;
    Ok(s)
}

fn assert_send<T: Send>(x: T) -> T {
    x
}

async fn batch<I, P>(
    iter: &mut I,
    sst_id: usize,
    block_size: usize,
    target_sst_size: usize,
    persistent: &P,
) -> Option<Arc<SsTable<P::Handle>>>
where
    P: SstPersistent,
    I: Stream<Item = anyhow::Result<Entry>> + Unpin,
{
    let mut builder = SsTableBuilder::new(block_size);

    while builder.estimated_size() <= target_sst_size {
        // todo: handle error
        let Some(Ok(entry)) = iter.next().await else {
            break;
        };

        // 被删除的 entry 不再添加
        if entry.value.is_empty() {
            continue;
        }

        let key = KeySlice::from_slice(entry.key.as_ref());
        let value = entry.value.as_ref();
        builder.add(key, value);
    }
    // todo: add error log
    // todo: 是否需要 block cache
    // todo: 这个检查应该放到 builder.build
    if builder.is_empty() {
        None
    } else {
        let table = builder
            .build(sst_id, None, persistent)
            .await
            .inspect_err(|err| error!(error = ?err))
            .ok()?;
        Some(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_apply_compaction() {}
}
