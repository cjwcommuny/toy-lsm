use anyhow::anyhow;

use std::collections::{Bound, HashMap};
use std::fmt::{Debug, Formatter};

use std::iter::repeat;

use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use futures::stream;

use tracing::error;

use crate::entry::InnerEntry;

use crate::iterators::{create_merge_iter, create_two_merge_iter, MergeIterator};
use crate::key::KeySlice;
use crate::manifest::{Compaction, Flush};
use crate::memtable::ImmutableMemTable;
use crate::persistent::SstHandle;
use crate::sst::compact::CompactionOptions;
use crate::sst::iterator::concat::SstConcatIterator;
use crate::sst::iterator::{scan_sst_concat, MergedSstIterator, SsTableIterator};
use crate::sst::option::SstOptions;
use crate::sst::SsTable;

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

impl<File: SstHandle> Debug for Sstables<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstables")
            .field("l0_sstables", &self.l0_sstables)
            .field("levels", &self.levels)
            .field("sstables", &self.sstables)
            .finish()
    }
}

impl<File> Sstables<File> {
    pub fn sst_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.l0_sstables
            .iter()
            .chain(self.levels.iter().flatten())
            .copied()
    }
}

#[cfg(test)]
impl<File> Sstables<File> {
    pub fn l0_sstables(&self) -> &[usize] {
        &self.l0_sstables
    }

    pub fn levels(&self) -> &[Vec<usize>] {
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

    pub(crate) fn table_ids(&self, level: usize) -> &Vec<usize> {
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

    pub fn fold_compaction_manifest(&mut self, Compaction(task, result_ids): Compaction) {
        let source = self.table_ids_mut(task.source());
        source.remove(task.source_index());
        let destination = self.table_ids_mut(task.destination());
        let _ = mem::replace(destination, result_ids);
    }
}

fn filter_sst_by_bloom<File>(
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

pub fn fold_flush_manifest<W, File>(
    imm_memtables: &mut Vec<Arc<ImmutableMemTable<W>>>,
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
