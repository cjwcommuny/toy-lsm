use futures::{stream, FutureExt, Stream, StreamExt};

use std::collections::{Bound, HashMap};
use std::fmt::{Debug, Formatter};

use std::future::{ready, Future};
use std::mem;

use bytes::Bytes;
use deref_ext::DerefExt;
use std::pin::Pin;
use std::process::Output;
use either::Either;

use crate::bound::map_bound_own;
use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::iterators::{
    create_merge_iter, create_merge_iter_from_non_empty_iters, create_two_merge_iter,
    iter_fut_to_stream, NonEmptyStream,
};
use crate::key::KeySlice;
use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
};
use crate::sst::iterator::{
    create_sst_concat_and_seek_to_first, scan_sst_concat, MergedSstIterator, SsTableIterator,
};
use crate::sst::option::SstOptions;
use crate::sst::{SsTable, SsTableBuilder};

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
    sstables: HashMap<usize, SsTable<File>>,
}

impl<File> Debug for Sstables<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstables")
            .field("l0_sstables", &self.l0_sstables)
            .field("levels", &self.levels)
            .field("sstables", &self.sstables)
            .finish()
    }
}

// only for test
impl<File> Sstables<File> {
    // todo: delete it
    pub fn sstables_mut(&mut self) -> &mut HashMap<usize, SsTable<File>> {
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

// fn type_hint<'a, F, File: 'a>(f: F) -> F
// where
//     F: for<'b> FnMut(&'b usize) -> &'a SsTable<File> + 'a,
// {
//     f
// }
//
// fn type_hint2<F, File>(f: F) -> F
// where
//     for<'a> F: FnMut(&'a SsTable<File>) -> SsTableIterator<'a, File>,
// {
//     f
// }
//
// fn type_hint3<F, File>(f: F) -> F
// where
//     F: for<'b> FnMut(&'b SsTableIterator<File>) -> bool,
// {
//     f
// }
//
// fn type_hint4<F, File, Fut>(f: F) -> F
// where
//     F: for<'b> FnMut(SsTableIterator<File>) -> Fut,
//     Fut: Future<Output = NonEmptyStream<anyhow::Result<Entry>, Box<SsTableIterator<File>>>>,
// {
//     f
// }

impl<File> Sstables<File>
where
    File: PersistentHandle,
{
    pub fn insert_sst(&mut self, table: SsTable<File>) {
        self.l0_sstables.insert(0, *table.id());
        self.sstables.insert(*table.id(), table);
    }

    pub async fn scan_sst<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<MergedSstIterator<'a, File>> {
        // todo: Because SsTableIterator::create involves I/O operations and might be slow,
        // todo: we do not want to do this in the state critical section.
        // todo: Therefore, you should firstly take read the state and clone the Arc of the LSM state snapshot.
        let sstables = &self.sstables;

        let lower = map_bound_own(lower);
        let upper = map_bound_own(upper);

        let lower2 = lower.clone();
        let upper2 = upper.clone();

        let lower3 = lower.clone();
        let upper3 = upper.clone();

        let l0 = {
            let iters = stream::iter(self.l0_sstables.iter())
                .filter_map(|id| {
                    let table = sstables.get(id).unwrap();
                    let iter = SsTableIterator::scan(table, lower2.clone(), upper2.clone());
                    // if !filter_sst_by_bloom(
                    //     table,
                    //     lower3.as_ref().map(Bytes::as_ref),
                    //     upper3.as_ref().map(Bytes::as_ref),
                    // ) {
                    //     return Either::Left(stream::empty());
                    // }
                    let s = async {
                        let s = NonEmptyStream::try_new(Box::new(iter))
                            .await
                            .ok()
                            .flatten();
                        s
                    };
                    s
                });


            // let iters = self.l0_sstables.iter();
            // let iters = iters.map(type_hint(move |id| sstables.get(id).unwrap()));
            // let iters = iters.map(type_hint2(move |table| {
            //     SsTableIterator::scan(table, lower2.clone(), upper2.clone())
            // }));
            // let iters = iters.filter(type_hint3(move |table| {
            //     filter_sst_by_bloom(
            //         table,
            //         lower3.as_ref().map(Bytes::as_ref),
            //         upper3.as_ref().map(Bytes::as_ref),
            //     )
            // }));
            // let iters = iters
            //     // todo: 这样用bloom filter 效率如何？
            //     // todo: 这里避免 Box？
            //     .map(|iter| {
            //         let x = NonEmptyStream::try_new(Box::new(iter)).map(|s| s.ok().flatten());
            //         x
            //     });
            // let iters = iter_fut_to_stream(iters).filter_map(ready);
            create_merge_iter_from_non_empty_iters(iters).await
        };

        let levels = {
            let lower = lower.clone();
            let upper = upper.clone();
            let iters = self
                .levels
                .iter()
                .filter_map(move |(_, ids)| {
                    let tables = ids.iter().map(|id| self.sstables.get(id).unwrap());
                    scan_sst_concat(tables, lower.clone(), upper.clone()).ok()
                })
                .map(Box::new);
            let iters = stream::iter(iters);
            create_merge_iter(iters).await
        };

        create_two_merge_iter(l0, levels).await
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
        sstables: &HashMap<usize, SsTable<File>>,
        _next_sst_id: impl Fn() -> usize,
    ) -> anyhow::Result<Vec<SsTable<File>>> {
        let l0 = {
            let iters = l0_sstables
                .iter()
                .map(|index| sstables.get(index).unwrap())
                .map(SsTableIterator::create_and_seek_to_first)
                .map(Box::new)
                .map(NonEmptyStream::try_new);
            let iters = iter_fut_to_stream(iters).filter_map(|s| ready(s.ok().flatten()));
            create_merge_iter_from_non_empty_iters(iters).await
        };
        let l1 = {
            let tables = l1_sstables
                .iter()
                .map(|index| sstables.get(index).unwrap())
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
    iter: &SsTableIterator<File>,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> bool {
    use Bound::Included;
    if let (Included(lower), Included(upper)) = (lower, upper) {
        if lower == upper {
            return iter.may_contain(lower.as_ref());
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
        builder.build(sst_id, None, persistent).await.ok()
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
