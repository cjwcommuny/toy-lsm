// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use std::collections::{Bound, HashMap};
use std::fmt::{Debug, Formatter};
use std::future::ready;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

use futures::{stream, FutureExt, Stream, StreamExt};
use tokio::sync::RwLock;
use tracing::error;

use crate::entry::Entry;
use crate::iterators::merge::MergeIteratorInner;
use crate::iterators::{
    create_merge_iter, create_merge_iter_from_non_empty_iters, create_two_merge_iter,
    iter_fut_to_stream, MergeIterator, NonEmptyStream,
};
use crate::key::KeySlice;
use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
};
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::iterator::{
    create_sst_concat_and_seek_to_first, scan_sst_concat, MergedSstIterator, SsTableIterator,
};
use crate::sst::option::SstOptions;
use crate::sst::{bloom, SsTable, SsTableBuilder};

#[derive(Default)]
pub struct Sstables<File> {
    /// L0 SSTs, from latest to earliest.
    l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    /// todo: 这里的 key 不存储 index，只存储 reference
    /// todo: 这个接口的设计需要调整，把 usize 封装起来
    sstables: HashMap<usize, Arc<SsTable<File>>>,
}

impl<File> Clone for Sstables<File> {
    fn clone(&self) -> Self {
        Self {
            l0_sstables: self.l0_sstables.clone(),
            levels: self.levels.clone(),
            sstables: self.sstables.clone(),
        }
    }
}

impl<File: PersistentHandle> Debug for Sstables<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstables")
            .field("l0_sstables", &self.l0_sstables)
            .field("levels", &self.levels)
            .field("sstables", &self.sstables)
            .finish()
    }
}

#[cfg(test)]
impl<File> Sstables<File> {
    pub fn l0_sstables(&self) -> &[usize] {
        &self.l0_sstables
    }

    pub fn levels(&self) -> &[(usize, Vec<usize>)] {
        &self.levels
    }

    pub fn sstables(&self) -> &HashMap<usize, Arc<SsTable<File>>> {
        &self.sstables
    }
}

// only for test
impl<File> Sstables<File> {
    // todo: delete it
    pub fn sstables_mut(&mut self) -> &mut HashMap<usize, Arc<SsTable<File>>> {
        &mut self.sstables
    }

    pub fn new(options: &SstOptions) -> Self {
        let levels = match options.compaction_option() {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            l0_sstables: Vec::new(),
            levels,
            sstables: HashMap::new(),
        }
    }
}

impl<File> Sstables<File>
where
    File: PersistentHandle,
{
    pub fn insert_sst(&mut self, table: Arc<SsTable<File>>) {
        self.l0_sstables.insert(0, *table.id());
        self.sstables.insert(*table.id(), table);
    }

    pub async fn scan_sst<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<MergedSstIterator<'a, File>> {
        let l0 = self.scan_l0(lower, upper).await;
        let levels = self.scan_levels(lower, upper).await;

        create_two_merge_iter(l0, levels).await
    }

    pub async fn scan_l0<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> MergeIterator<Entry, SsTableIterator<'a, File>> {
        let iters = self.build_l0_iter(lower, upper);
        let iters = stream::iter(iters);
        create_merge_iter(iters).await
    }

    async fn scan_l02<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> MergeIteratorInner<Entry, SsTableIterator<'a, File>> {
        let iters = self.build_l0_iter(lower, upper);
        let iters = stream::iter(iters);
        MergeIteratorInner::create(iters).await
    }

    fn build_l0_iter<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> impl Iterator<Item = SsTableIterator<'a, File>> + 'a {
        let iters = self
            .l0_sstables
            .iter()
            .map(|id| self.sstables.get(id).unwrap())
            .filter_map(move |table| {
                if !filter_sst_by_bloom(table, lower, upper) {
                    None
                } else {
                    Some(SsTableIterator::scan(table, lower, upper))
                }
            });
        iters
    }

    async fn scan_levels<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> MergeIterator<Entry, SstConcatIterator<'a>> {
        let iters = self.levels.iter().filter_map(move |(_, ids)| {
            let tables = ids.iter().map(|id| self.sstables.get(id).unwrap().as_ref());
            scan_sst_concat(tables, lower, upper)
                .inspect_err(|err| error!(error = ?err))
                .ok()
        });
        let iters = stream::iter(iters);
        create_merge_iter(iters).await
    }

    fn level_size(&self, level: usize) -> usize {
        if level == 0 {
            self.l0_sstables.len()
        } else {
            let (_, ids) = self.levels.get(level - 1).unwrap();
            ids.len()
        }
    }

    fn table_ids_mut(&mut self, level: usize) -> &mut Vec<usize> {
        if level == 0 {
            &mut self.l0_sstables
        } else {
            let (_, ids) = self.levels.get_mut(level - 1).unwrap();
            ids
        }
    }

    pub fn apply_compaction(
        &mut self,
        upper_level: usize,
        upper: &[usize],
        lower_level: usize,
        lower: &[usize],
        new_sst_ids: Vec<usize>,
    ) {
        let current_upper_size = self.level_size(upper_level);

        // remove upper level ids
        println!("remove upper level {} ids", upper_level);
        self.table_ids_mut(upper_level)
            .truncate(dbg!(current_upper_size) - dbg!(upper.len()));

        // replace lower level ids
        println!("remove lower level {} ids", lower_level);
        let _ = mem::replace(self.table_ids_mut(lower_level), new_sst_ids);

        // remove old sstables
        for index in upper.iter().chain(lower.iter()) {
            self.sstables_mut().remove(index);
        }
    }

    pub fn clean_up_files(_ids: impl IntoIterator<Item = usize>) {
        todo!()
    }

    async fn compact_full(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
        sstables: &HashMap<usize, Arc<SsTable<File>>>,
        _next_sst_id: impl Fn() -> usize,
    ) -> anyhow::Result<Vec<Arc<SsTable<File>>>> {
        let l0 = {
            let iters = l0_sstables
                .iter()
                .map(|index| sstables.get(index).unwrap().as_ref())
                .map(SsTableIterator::create_and_seek_to_first)
                .map(Box::new)
                .map(NonEmptyStream::try_new);
            let iters = iter_fut_to_stream(iters)
                .filter_map(|s| ready(s.inspect_err(|err| error!(error = ?err)).ok().flatten()));
            create_merge_iter_from_non_empty_iters(iters).await
        };
        let l1 = {
            let tables = l1_sstables
                .iter()
                .map(|index| sstables.get(index).unwrap().as_ref())
                .collect();
            create_sst_concat_and_seek_to_first(tables)
        }?;
        let _iter = create_two_merge_iter(l0, l1).await?;
        // let tables: Vec<_> = iter
        //     .batching(|iter| {
        //         let id = next_sst_id();
        //         let path = self.path_of_sst(id);
        //         let iter = pin!(iter);
        //         batch(
        //             iter,
        //             id,
        //             *self.options.block_size(),
        //             *self.options.target_sst_size(),
        //             path,
        //         )
        //     })
        //     .collect();
        // Ok(tables)
        todo!()
    }
}

fn filter_sst_by_bloom<File>(
    table: &SsTable<File>,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> bool {
    use Bound::Included;
    if let (Included(lower), Included(upper)) = (lower, upper) {
        if lower == upper {
            return bloom::may_contain(table.bloom.as_ref(), lower);
        }
    }
    true
}

async fn batch<I, P>(
    iter: &mut Pin<&mut I>,
    sst_id: usize,
    block_size: usize,
    target_sst_size: usize,
    persistent: &P,
) -> Option<SsTable<P::Handle>>
where
    P: Persistent,
    I: Stream<Item = anyhow::Result<Entry>>,
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
        builder
            .build(sst_id, None, persistent)
            .await
            .inspect_err(|err| error!(error = ?err))
            .ok()
    }
}

pub async fn force_full_compaction<File>(
    this: &RwLock<Sstables<File>>,
    upper_level: usize,
    upper: Vec<usize>,
    lower_level: usize,
    lower: Vec<usize>,
    next_sst_id: impl Fn() -> usize,
) -> anyhow::Result<()>
where
    File: PersistentHandle,
{
    let new_sst = {
        let guard = this.read().await;
        guard
            .compact_full(&upper, &lower, &guard.sstables, next_sst_id)
            .await
    }?;
    let new_sst_ids: Vec<_> = new_sst.iter().map(|table| *table.id()).collect();

    {
        let mut guard = this.write().await;
        guard.apply_compaction(upper_level, &upper, lower_level, &lower, new_sst_ids);

        // add new sstables
        for table in new_sst {
            guard.sstables.insert(*table.id(), table);
        }

        let _sst_id_to_remove = upper.iter().chain(lower.iter());
        // for sst in sst_id_to_remove {
        //     std::fs::remove_file(guard.path_of_sst(*sst))?;
        // }
        todo!()
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::Unbounded;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::iterators::{create_two_merge_iter, NonEmptyStream};
    use crate::key::KeySlice;
    use futures::StreamExt;
    use tempfile::{tempdir, TempDir};
    use tokio::time::timeout;
    use tracing::{info, Instrument};
    use tracing_subscriber::fmt::format::FmtSpan;

    use crate::persistent::file_object::FileObject;
    use crate::persistent::LocalFs;
    use crate::sst::iterator::SsTableIterator;
    use crate::sst::{SsTable, SsTableBuilder, SstOptions, Sstables};

    #[tokio::test]
    async fn test() {
        // tracing_subscriber::fmt::fmt()
        //     .with_span_events(FmtSpan::EXIT | FmtSpan::ENTER | FmtSpan::CLOSE)
        //     .with_target(false)
        //     .with_level(false)
        //     .init();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(Default::default())
            .build();
        let dir = TempDir::new().unwrap();
        let path = dir.as_ref();
        let persistent = LocalFs::new(path.to_path_buf());
        let mut sst = Sstables::<FileObject>::new(&options);
        let table = {
            let mut builder = SsTableBuilder::new(16);
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11");
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22");
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"33"), b"11");
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"44"), b"22");
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"55"), b"11");
            builder.add(KeySlice::for_testing_from_slice_no_ts(b"66"), b"22");
            builder.build(0, None, &persistent).await.unwrap()
        };
        sst.insert_sst(Arc::new(table));

        let iter = sst.scan_sst(Unbounded, Unbounded).await.unwrap();
        // assert_eq!()
    }
}
