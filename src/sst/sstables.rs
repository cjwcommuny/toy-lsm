use deref_ext::DerefExt;
use std::cmp::max;
use std::collections::{Bound, HashMap};
use std::fmt::{Debug, Formatter};
use std::future::ready;
use std::iter::repeat;
use std::pin::Pin;
use std::sync::Arc;
use std::{iter, mem};

use futures::{pin_mut, stream, FutureExt, Stream, StreamExt};
use itertools::Itertools;
use ordered_float::NotNan;
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
    pub(super) l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub(super) levels: Vec<Vec<usize>>,
    /// SST objects.
    /// todo: 这里的 key 不存储 index，只存储 reference
    /// todo: 这个接口的设计需要调整，把 usize 封装起来
    pub(super) sstables: HashMap<usize, Arc<SsTable<File>>>,
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
            CompactionOptions::Leveled(opt) => {
                repeat(Vec::new()).take(opt.max_levels() - 1).collect()
            }
            CompactionOptions::NoCompaction => Vec::new(),
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
        let iters = self.levels.iter().filter_map(move |ids| {
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
            let ids = self.levels.get(level - 1).unwrap();
            ids.len()
        }
    }

    pub(super) fn table_ids_mut(&mut self, level: usize) -> &mut Vec<usize> {
        if level == 0 {
            &mut self.l0_sstables
        } else {
            let ids = self.levels.get_mut(level - 1).unwrap();
            ids
        }
    }

    pub(super) fn table_ids(&self, level: usize) -> &Vec<usize> {
        if level == 0 {
            &self.l0_sstables
        } else {
            let ids = self.levels.get(level - 1).unwrap();
            ids
        }
    }

    pub(super) fn tables(&self, level: usize) -> impl DoubleEndedIterator<Item = &SsTable<File>> {
        self.table_ids(level)
            .iter()
            .map(|id| self.sstables.get(id).unwrap().as_ref())
    }

    pub fn clean_up_files(_ids: impl IntoIterator<Item = usize>) {
        todo!()
    }

    pub async fn force_compaction<P: Persistent<Handle = File>>(&mut self) -> anyhow::Result<()> {
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
