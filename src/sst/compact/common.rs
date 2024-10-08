use crate::entry::InnerEntry;
use crate::iterators::create_two_merge_iter;
use crate::iterators::merge::MergeIteratorInner;

use derive_new::new;
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::manifest::{Compaction, Manifest, ManifestRecord};
use crate::mvcc::iterator::{WatermarkGcIter, WatermarkGcIterImpl};
use crate::persistent::{Persistent, SstHandle};
use crate::sst::compact::full::generate_full_compaction_task;
use crate::sst::compact::leveled;
use crate::sst::compact::leveled::compact_task;
use crate::sst::compact::CompactionOptions::{Full, Leveled, NoCompaction};
use crate::sst::iterator::{create_sst_concat_and_seek_to_first, SsTableIterator};
use crate::sst::{SsTable, SsTableBuilder, SstOptions, Sstables};
use crate::state::sst_id::{SstIdGenerator, SstIdGeneratorImpl};
use crate::utils::send::assert_send;
use futures::future::Either;
use itertools::Itertools;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::error;

#[derive(Serialize, Deserialize, new, PartialEq, Debug)]
pub struct NewCompactionRecord {
    pub task: NewCompactionTask,
    pub result_sst_ids: Vec<usize>,
}

#[derive(Serialize, Deserialize, new, PartialEq, Debug, Clone)]
pub struct NewCompactionTask {
    pub source_level: usize,
    pub source_ids: Vec<usize>,
    pub destination_level: usize,
    pub destination_ids: Vec<usize>,
}

// todo: return Stream<Item = Arc<SsTable<File>>>
// todo: replace it with compact_task
pub async fn compact_generate_new_sst<'a, P: Persistent, U, L>(
    upper_sstables: U,
    lower_sstables: L,
    next_sst_id: SstIdGeneratorImpl,
    options: &SstOptions,
    persistent: P,
    watermark: Option<u64>,
) -> anyhow::Result<Vec<Arc<SsTable<P::SstHandle>>>>
where
    U: Iterator<Item = &'a SsTable<P::SstHandle>> + Send + 'a,
    L: Iterator<Item = &'a SsTable<P::SstHandle>> + Send + 'a,
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
    let merged_iter = assert_send(create_two_merge_iter(l0, l1)).await?;
    let iter = match watermark {
        Some(watermark) => Either::Left(WatermarkGcIterImpl::build_watermark_gc_iter(
            merged_iter,
            watermark,
        )),
        None => Either::Right(merged_iter),
    };
    let s: Vec<_> = assert_send(
        stream::unfold(iter, |mut iter| async {
            let b = batch(
                &mut iter,
                &next_sst_id,
                *options.block_size(),
                *options.target_sst_size(),
                &persistent,
            )
            .await?;
            Some((b, iter))
        })
        .collect(),
    )
    .await;
    Ok(s)
}

async fn batch<I, P>(
    iter: &mut I,
    next_sst_id: &SstIdGeneratorImpl,
    block_size: usize,
    target_sst_size: usize,
    persistent: &P,
) -> Option<Arc<SsTable<P::SstHandle>>>
where
    P: Persistent,
    I: Stream<Item = anyhow::Result<InnerEntry>> + Unpin,
{
    let mut builder = SsTableBuilder::new(block_size);

    while builder.estimated_size() <= target_sst_size {
        // todo: handle error
        let Some(Ok(entry)) = iter.next().await else {
            break;
        };

        // 被删除的 entry 不再添加
        // if entry.value.is_empty() {
        //     continue;
        // }

        let key = entry.key.as_key_slice();
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
            .build(next_sst_id.next_sst_id(), None, persistent)
            .await
            .inspect_err(|err| error!(error = ?err))
            .ok()?;
        Some(Arc::new(table))
    }
}

pub async fn force_compact<P: Persistent + Clone>(
    old_sstables: Arc<Sstables<P::SstHandle>>,
    sstables: &mut Sstables<P::SstHandle>,
    next_sst_id: SstIdGeneratorImpl,
    options: Arc<SstOptions>,
    persistent: P,
    manifest: Option<Manifest<P::ManifestHandle>>,
    watermark: Option<u64>,
) -> anyhow::Result<()> {
    // todo: 这个可以提到外面，就不用 clone state 了
    use either::Either;

    // todo: support other compaction
    let tasks = match options.compaction_option() {
        Leveled(options) => Either::Left(leveled::generate_tasks(options, sstables).into_iter()),
        Full => Either::Right(generate_full_compaction_task(sstables).into_iter()),
        NoCompaction => Either::Right(None.into_iter()),
    };

    let (records, new_ssts) = {
        let concurrency = options.compaction_option().concurrency();
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut records = Vec::new();
        let mut new_ssts = Vec::new();
        for task in tasks {
            let permit = semaphore.clone().acquire_owned().await?;
            let old_sstables = old_sstables.clone();
            let next_sst_id = next_sst_id.clone();
            let options = options.clone();
            let persistent = persistent.clone();

            let (record, sst) = tokio::spawn(assert_send(async move {
                let _permit = permit;

                let new_ssts = assert_send(compact_task(
                    old_sstables.as_ref(),
                    task.clone(),
                    next_sst_id,
                    options.as_ref(),
                    persistent,
                    watermark,
                ))
                .await?;
                let result_sst_ids = new_ssts.iter().map(|table| *table.id()).collect();
                let record = NewCompactionRecord {
                    task,
                    result_sst_ids,
                };
                Ok::<_, anyhow::Error>((record, new_ssts))
            }))
            .await??;
            records.push(record);
            new_ssts.extend(sst);
        }
        (records, new_ssts)
    };

    // remove old sst
    for record in &records {
        for old_id in record
            .task
            .source_ids
            .iter()
            .chain(record.task.destination_ids.iter())
        {
            if let Some(sst) = sstables.sstables.get(old_id) {
                sst.set_to_delete();
            }
            sstables.sstables.remove(old_id);
        }
    }

    // add new sst
    for new_sst in new_ssts {
        sstables.sstables.insert(*new_sst.id(), new_sst);
    }

    // modify ids
    apply_compaction_v2(sstables, &records);

    // todo: add manifest
    // todo: guarantee atomicity?
    if let Some(manifest) = manifest {
        let record = ManifestRecord::Compaction(Compaction(records));
        manifest.add_record(record).await?;
    }

    Ok(())
}

pub fn apply_compaction_v2<File: SstHandle>(
    sstables: &mut Sstables<File>,
    records: &[NewCompactionRecord],
) {
    for record in records {
        apply_compaction_v2_single(sstables, record);
    }
}

pub fn apply_compaction_v2_single<File: SstHandle>(
    sstables: &mut Sstables<File>,
    record: &NewCompactionRecord,
) {
    let source_level = sstables.table_ids_mut(record.task.source_level);
    if let Some((source_begin_index, _)) = source_level
        .iter()
        .find_position(|id| **id == record.task.source_ids[0])
    {
        source_level.drain(source_begin_index..source_begin_index + record.task.source_ids.len());
    }

    let destination_level = sstables.table_ids_mut(record.task.destination_level);
    let destination_begin_index = destination_level
        .iter()
        .find_position(|id| **id == record.task.destination_ids[0])
        .map(|(index, _)| index)
        .unwrap_or(0);
    destination_level.splice(
        destination_begin_index..destination_begin_index + record.task.destination_ids.len(),
        record.result_sst_ids.iter().copied(),
    );
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_apply_compaction() {}
}
