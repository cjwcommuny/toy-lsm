use anyhow::anyhow;

use std::collections::{Bound, HashMap};
use std::fmt::{Debug, Formatter};

use std::iter::repeat;

use futures::stream;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tracing::error;

use crate::entry::InnerEntry;

use crate::iterators::{create_merge_iter, create_two_merge_iter, MergeIterator};
use crate::key::{KeyBytes, KeySlice};
use crate::manifest::Flush;
use crate::memtable::ImmutableMemTable;
use crate::persistent::SstHandle;
use crate::sst::compact::common::{apply_compaction_v2, NewCompactionRecord, NewCompactionTask};
use crate::sst::compact::CompactionOptions;
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::iterator::{scan_sst_concat, MergedSstIterator, SsTableIterator};
use crate::sst::option::SstOptions;
use crate::sst::SsTable;
use crate::utils::range::MinMax;

#[derive(Default)]
pub struct Sstables<File: SstHandle> {
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

impl<File: SstHandle> Clone for Sstables<File> {
    fn clone(&self) -> Self {
        Self {
            l0_sstables: self.l0_sstables.clone(),
            levels: self.levels.clone(),
            sstables: self.sstables.clone(),
        }
    }
}

impl<File: SstHandle> Debug for Sstables<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstables")
            .field("l0_sstables", &self.l0_sstables)
            .field("levels", &self.levels)
            .field("sstables", &self.sstables)
            .finish()
    }
}

impl<File: SstHandle> Sstables<File> {
    pub fn sst_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.l0_sstables
            .iter()
            .chain(self.levels.iter().flatten())
            .copied()
    }
}

#[cfg(test)]
impl<File: SstHandle> Sstables<File> {
    pub fn l0_sstables(&self) -> &[usize] {
        &self.l0_sstables
    }

    pub fn levels(&self) -> &[Vec<usize>] {
        &self.levels
    }
}

// only for test
impl<File: SstHandle> Sstables<File> {
    pub fn sstables(&self) -> &HashMap<usize, Arc<SsTable<File>>> {
        &self.sstables
    }

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
            CompactionOptions::Full => repeat(Vec::new()).take(1).collect(),
        };
        Self {
            l0_sstables: Vec::new(),
            levels,
            sstables: HashMap::new(),
        }
    }
}

impl<File: SstHandle> Sstables<File> {
    pub fn get_l0_key_minmax(&self) -> Option<MinMax<KeyBytes>> {
        self.tables(0).fold(None, |range, table| {
            let table_range = table.get_key_range();
            let new_range = match range {
                None => table_range,
                Some(range) => range.union(table_range),
            };
            Some(new_range)
        })
    }

    pub fn select_table_by_range<'a>(
        &'a self,
        level: usize,
        range: &'a MinMax<KeyBytes>,
    ) -> impl Iterator<Item = usize> + 'a {
        self.tables(level)
            .filter(|table| table.get_key_range().overlap(range))
            .map(|table| *table.id())
    }
    pub(super) fn tables(&self, level: usize) -> impl DoubleEndedIterator<Item = &SsTable<File>> {
        self.table_ids(level)
            .iter()
            .map(|id| self.sstables.get(id).unwrap().as_ref())
    }

    pub(crate) fn table_ids(&self, level: usize) -> &Vec<usize> {
        if level == 0 {
            &self.l0_sstables
        } else {
            let ids = self.levels.get(level - 1).unwrap();
            ids
        }
    }
}

impl<File> Sstables<File>
where
    File: SstHandle,
{
    pub fn insert_sst(&mut self, table: Arc<SsTable<File>>) {
        self.l0_sstables.insert(0, *table.id());
        self.sstables.insert(*table.id(), table);
    }

    pub async fn scan_sst<'a>(
        &'a self,
        lower: Bound<KeySlice<'a>>,
        upper: Bound<KeySlice<'a>>,
    ) -> anyhow::Result<MergedSstIterator<'a, File>> {
        let l0 = self.scan_l0(lower, upper).await;
        let levels = self.scan_levels(lower, upper).await;

        create_two_merge_iter(l0, levels).await
    }

    pub async fn scan_l0<'a>(
        &'a self,
        lower: Bound<KeySlice<'a>>,
        upper: Bound<KeySlice<'a>>,
    ) -> MergeIterator<InnerEntry, SsTableIterator<'a, File>> {
        let iters = self.build_l0_iter(lower, upper);
        let iters = stream::iter(iters);
        create_merge_iter(iters).await
    }

    fn build_l0_iter<'a>(
        &'a self,
        lower: Bound<KeySlice<'a>>,
        upper: Bound<KeySlice<'a>>,
    ) -> impl Iterator<Item = SsTableIterator<'a, File>> + 'a {
        let iters = self
            .l0_sstables
            .iter()
            .map(|id| self.sstables.get(id).unwrap())
            .filter_map(move |table| {
                // todo: scan not use bloom?
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
        lower: Bound<KeySlice<'a>>,
        upper: Bound<KeySlice<'a>>,
    ) -> MergeIterator<InnerEntry, SstConcatIterator<'a>> {
        let iters = self.levels.iter().filter_map(move |ids| {
            let tables = ids.iter().map(|id| self.sstables.get(id).unwrap().as_ref());
            scan_sst_concat(tables, lower, upper)
                .inspect_err(|err| error!(error = ?err))
                .ok()
        });
        let iters = stream::iter(iters);
        create_merge_iter(iters).await
    }

    pub(super) fn table_ids_mut(&mut self, level: usize) -> &mut Vec<usize> {
        if level == 0 {
            &mut self.l0_sstables
        } else {
            let ids = self.levels.get_mut(level - 1).unwrap();
            ids
        }
    }

    pub fn clean_up_files(_ids: impl IntoIterator<Item = usize>) {
        todo!()
    }

    // todo: 合并函数
    pub fn apply_compaction_sst_ids(&mut self, records: &[NewCompactionRecord]) {
        apply_compaction_v2(self, records);
    }

    pub fn apply_compaction_sst_v2(
        &mut self,
        new_sst: Vec<Arc<SsTable<File>>>,
        task: &NewCompactionTask,
    ) {
        for id in task.source_ids.iter().chain(&task.destination_ids) {
            self.sstables.remove(id);
        }

        for table in new_sst {
            self.sstables.insert(*table.id(), table);
        }
    }
}

fn filter_sst_by_bloom<File: SstHandle>(
    _table: &SsTable<File>,
    lower: Bound<KeySlice>,
    upper: Bound<KeySlice>,
) -> bool {
    use Bound::Included;
    if let (Included(lower), Included(upper)) = (lower, upper) {
        if lower == upper {
            return true;
            // return bloom::may_contain(table.bloom.as_ref(), lower);
        }
    }
    true
}

pub fn build_next_sst_id(a: &AtomicUsize) -> impl Fn() -> usize + Sized + '_ {
    || a.fetch_add(1, Relaxed)
}

pub fn fold_flush_manifest<File: SstHandle>(
    imm_memtables: &mut Vec<Arc<ImmutableMemTable>>,
    sstables: &mut Sstables<File>,
    Flush(id): Flush,
) -> anyhow::Result<()> {
    let table = imm_memtables
        .pop()
        .ok_or(anyhow!("expect memtable with id {}", id))?;
    if table.id() != id {
        return Err(anyhow!("expect memtable with id {}", id));
    }
    sstables.l0_sstables.insert(0, id);
    Ok(())
}

#[cfg(test)]
mod tests {}
