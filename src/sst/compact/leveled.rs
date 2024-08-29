use crate::key::KeyBytes;
use crate::persistent::{Persistent, SstHandle};
use crate::sst::compact::common::{
    compact_generate_new_sst, NewCompactionTask,
};
use crate::sst::{SsTable, SstOptions, Sstables};
use crate::state::sst_id::SstIdGeneratorImpl;
use crate::utils::num::power_of_2;
use crate::utils::range::MinMax;
use crate::utils::send::assert_send;
use bytes::Bytes;
use derive_new::new;
use either::Either;
use getset::CopyGetters;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::cmp::max;
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, new, TypedBuilder, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct LeveledCompactionOptions {
    #[getset(skip)]
    level_size_multiplier_2_exponent: usize,

    max_levels: usize,

    max_bytes_for_level_base: u64,
    // level0_file_num_compaction_trigger: usize,
    #[builder(default = 1)]
    concurrency: usize,
}

impl LeveledCompactionOptions {
    pub fn level_size_multiplier(&self) -> usize {
        power_of_2(self.level_size_multiplier_2_exponent)
    }

    pub fn compute_target_size(
        &self,
        current_level: usize,
        last_level: usize,
        last_level_size: usize,
    ) -> usize {
        last_level_size
            / (power_of_2(self.level_size_multiplier_2_exponent * (last_level - current_level)))
    }
}

// todo: 怎么处理 last level？
fn filter_and_sort_source_levels(
    level_sizes: &[u64],
    target_sizes: &[u64],
    max_bytes_for_level_base: u64,
) -> Vec<usize> {
    let mut level_and_scores: Vec<_> = level_sizes
        .iter()
        .zip(target_sizes)
        .enumerate()
        .map(|(index, (level_size, target_size))| {
            let denominator = if index == 0 {
                max_bytes_for_level_base
            } else {
                *target_size
            };
            *level_size as f64 / denominator as f64
        })
        .enumerate()
        .filter(|(_, priority)| *priority > 1.0)
        .collect();

    level_and_scores.sort_by_key(|(level, priority)| (*level != 0, OrderedFloat(-priority)));
    level_and_scores
        .into_iter()
        .map(|(level, _)| level)
        .collect()
}

fn select_level_destination(
    options: &LeveledCompactionOptions,
    source: usize,
    target_sizes: &[u64],
) -> usize {
    assert!(source < target_sizes.len() - 1);
    target_sizes
        .iter()
        .enumerate()
        .skip(source + 2)
        .find(|(_, &target_size)| target_size > options.max_bytes_for_level_base)
        .map(|(level, _)| level - 1)
        .unwrap_or(target_sizes.len() - 1)
}

fn compute_level_sizes<File: SstHandle>(sstables: &Sstables<File>) -> Vec<u64> {
    iter::once(&sstables.l0_sstables)
        .chain(&sstables.levels)
        .map(|level| {
            level
                .iter()
                .map(|id| {
                    let table = sstables.sstables.get(id).unwrap();
                    table.table_size()
                })
                .sum()
        })
        .collect()
}

fn compute_target_sizes(last_level_size: u64, options: &LeveledCompactionOptions) -> Vec<u64> {
    let last_level_target_size = max(last_level_size, options.max_bytes_for_level_base);
    let mut target_sizes: Vec<_> = iter::successors(Some(last_level_target_size), |size| {
        Some(size / options.level_size_multiplier() as u64)
    })
    .take(options.max_levels())
    .collect();
    target_sizes.reverse();
    target_sizes
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum KeyRange {
    Range { left: Bytes, right: Bytes },
    Inf,
    Empty,
}

pub fn generate_tasks<File: SstHandle>(
    option: &LeveledCompactionOptions,
    sstables: &Sstables<File>,
) -> Vec<NewCompactionTask> {
    let level_sizes = compute_level_sizes(sstables);
    let target_sizes = compute_target_sizes(*level_sizes.last().unwrap(), option);
    let source_levels_sorted = filter_and_sort_source_levels(
        &level_sizes,
        &target_sizes,
        option.max_bytes_for_level_base(),
    );

    

    {
        let mut result = Vec::new();
        let mut tables_in_compaction = HashSet::new();
        for source_level in source_levels_sorted {
            let tasks = generate_tasks_for_level(
                source_level,
                sstables,
                &target_sizes,
                &mut tables_in_compaction,
                option,
            );
            result.extend(tasks)
        }
        result
    }
}

fn generate_tasks_for_level<'a, File>(
    source_level: usize,
    sstables: &'a Sstables<File>,
    target_sizes: &[u64],
    tables_in_compaction: &'a mut HashSet<usize>,
    option: &LeveledCompactionOptions,
) -> impl Iterator<Item = NewCompactionTask> + 'a {
    if source_level == 0 {
        let l0_minmax = sstables.get_l0_key_minmax().unwrap();
        let destination_level = select_level_destination(option, 0, target_sizes);
        let source_ids = sstables.levels[0].clone();
        let destination_ids = generate_next_level_table_ids(
            tables_in_compaction,
            sstables,
            &l0_minmax,
            destination_level,
        );
        let task = NewCompactionTask {
            source_level,
            source_ids,
            destination_level,
            destination_ids,
        };
        let iter = iter::once(task);
        Either::Left(iter)
    } else {
        // todo: by priority?
        let iter = sstables
            .tables(source_level)
            .scan(tables_in_compaction, move |tables_in_compaction, table| {
                let source_id = *table.id();
                let task = if tables_in_compaction.contains(&source_id) {
                    None
                } else {
                    let minmax = table.get_key_range();
                    let destination_level = source_level + 1;
                    let source_ids = vec![source_id];
                    let destination_ids = generate_next_level_table_ids(
                        tables_in_compaction,
                        sstables,
                        &minmax,
                        destination_level,
                    );
                    if destination_ids.is_empty() {
                        None
                    } else {
                        Some(NewCompactionTask {
                            source_level,
                            source_ids,
                            destination_level,
                            destination_ids,
                        })
                    }
                };
                Some(task)
            })
            .flatten();
        Either::Right(iter)
    }
}

// todo: use reference instead of Arc
pub async fn compact_task<'a, P: Persistent>(
    sstables: Arc<Sstables<P::SstHandle>>,
    task: NewCompactionTask,
    next_sst_id: SstIdGeneratorImpl,
    options: Arc<SstOptions>,
    persistent: P,
    watermark: Option<u64>,
) -> anyhow::Result<Vec<Arc<SsTable<P::SstHandle>>>> {
    let upper_sstables = task
        .destination_ids
        .iter()
        .map(|id| sstables.sstables.get(id).unwrap().as_ref());
    let lower_sstables = task
        .source_ids
        .iter()
        .map(|id| sstables.sstables.get(id).unwrap().as_ref());
    assert_send(compact_generate_new_sst(
        upper_sstables,
        lower_sstables,
        next_sst_id,
        options,
        persistent,
        watermark,
    ))
    .await
}

fn generate_next_level_table_ids<File>(
    tables_in_compaction: &mut HashSet<usize>,
    sstables: &Sstables<File>,
    source_key_range: &MinMax<KeyBytes>,
    destination_level: usize,
) -> Vec<usize> {
    sstables
        .select_table_by_range(destination_level, source_key_range)
        .scan(tables_in_compaction, |tables_in_compaction, id| {
            let id = if tables_in_compaction.contains(&id) {
                None
            } else {
                tables_in_compaction.insert(id);
                Some(id)
            };
            Some(id)
        })
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    
    
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use crate::persistent::file_object::FileObject;
    use crate::persistent::LocalFs;
    use crate::sst::compact::common::NewCompactionTask;
    use crate::sst::compact::leveled::{
        filter_and_sort_source_levels, generate_tasks_for_level, select_level_destination,
    };
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    
    use crate::sst::{SsTable, SstOptions, Sstables};
    use crate::state::LsmStorageState;
    use crate::test_utils::insert_sst;

    #[test]
    fn test_filter_and_sort_source_levels() {
        assert_eq!(
            filter_and_sort_source_levels(&[120, 301, 599, 5000], &[100, 300, 600, 1000], 300),
            vec![3, 1],
        );
        assert_eq!(
            filter_and_sort_source_levels(&[120, 299, 1000], &[150, 300, 600], 100),
            vec![2, 0]
        );
    }

    #[test]
    fn test_select_level_destination() {
        let options = LeveledCompactionOptions::new(1, 4, 300, 1);
        assert_eq!(
            select_level_destination(&options, 0, &[120, 240, 480, 960]),
            1
        );
        assert_eq!(
            select_level_destination(&options, 0, &[75, 150, 300, 600]),
            2
        );
        assert_eq!(
            select_level_destination(&options, 2, &[75, 150, 300, 600]),
            3
        );
    }

    #[test]
    fn test_generate_tasks() {
        let option = LeveledCompactionOptions::builder()
            .max_levels(4)
            .max_bytes_for_level_base(2048)
            .level_size_multiplier_2_exponent(1)
            .concurrency(1)
            .build();

        {
            let tables = [
                SsTable::mock(0, "0011", "0020"),
                SsTable::mock(1, "0018", "0022"),
                SsTable::mock(2, "0030", "0040"),
                SsTable::mock(3, "0000", "0008"),
                SsTable::mock(4, "0012", "0018"),
                SsTable::mock(5, "0020", "0037"),
                SsTable::mock(6, "0041", "0048"),
            ];
            let sstables = Sstables {
                l0_sstables: vec![0, 1, 2],
                levels: vec![
                    vec![3, 4, 5, 6],
                ],
                sstables: tables.into_iter().map(Arc::new).enumerate().collect(),
            };
            let target_size = vec![];
            let mut tables_in_compaction = HashSet::new();
            let tasks: Vec<_> = generate_tasks_for_level(
                0,
                &sstables,
                &target_size,
                &mut tables_in_compaction,
                &option,
            )
            .collect();
            let expected: Vec<_> = [NewCompactionTask::new(0, vec![0, 1, 2], 1, vec![4, 5])]
                .into_iter().collect();
            assert_eq!(tasks, expected)
        }
    }

    // #[tokio::test]
    // async fn test_force_compact_level() {
    //     let dir = tempdir().unwrap();
    //     let (state, mut sstables) = prepare_sstables(&dir).await;
    //
    //     {
    //         assert_eq!(sstables.l0_sstables, [4, 3, 2, 1, 0]);
    //         assert_eq!(
    //             sstables.levels,
    //             vec![Vec::<usize>::new(), Vec::new(), Vec::new()]
    //         );
    //         assert_eq!(sstables.sstables.len(), 5);
    //     }
    //
    //     compact_with_task(
    //         &mut sstables,
    //         build_next_sst_id(&state.sst_id),
    //         &state.options,
    //         &state.persistent,
    //         &CompactionTask::new(0, SourceIndex::Index { index: 4 }, 1),
    //         None,
    //     )
    //     .await
    //     .unwrap();
    //
    //     {
    //         assert_eq!(sstables.l0_sstables, [4, 3, 2, 1]);
    //         assert_eq!(sstables.levels, vec![vec![9, 10], vec![], vec![]]);
    //         assert_eq!(sstables.sstables.len(), 6);
    //     }
    //
    //     compact_with_task(
    //         &mut sstables,
    //         build_next_sst_id(&state.sst_id),
    //         &state.options,
    //         &state.persistent,
    //         &CompactionTask::new(0, SourceIndex::Index { index: 3 }, 1),
    //         None,
    //     )
    //     .await
    //     .unwrap();
    //
    //     {
    //         assert_eq!(sstables.l0_sstables, [4, 3, 2]);
    //         assert_eq!(sstables.levels, vec![vec![12, 13, 14, 15], vec![], vec![]]);
    //         assert_eq!(sstables.sstables.len(), 7);
    //     }
    //
    //     compact_with_task(
    //         &mut sstables,
    //         build_next_sst_id(&state.sst_id),
    //         &state.options,
    //         &state.persistent,
    //         &CompactionTask::new(1, SourceIndex::Index { index: 0 }, 2),
    //         None,
    //     )
    //     .await
    //     .unwrap();
    //
    //     {
    //         assert_eq!(sstables.l0_sstables, [4, 3, 2]);
    //         assert_eq!(sstables.levels, vec![vec![13, 14, 15], vec![17], vec![]]);
    //         assert_eq!(sstables.sstables.len(), 7);
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_force_compaction() {
    //     let dir = tempdir().unwrap();
    //     let (state, mut sstables) = prepare_sstables(&dir).await;
    //     force_compact(
    //         &mut sstables,
    //         state.sst_id_generator(),
    //         &state.options,
    //         &state.persistent,
    //         None,
    //         None,
    //     )
    //     .await
    //     .unwrap();
    //     {
    //         assert_eq!(sstables.l0_sstables, [4, 3, 2, 1]);
    //         assert_eq!(sstables.levels, vec![vec![], vec![], vec![9, 10]]);
    //         assert_eq!(sstables.sstables.len(), 6);
    //     }
    //
    //     for i in 0..5 {
    //         let begin = i * 100;
    //         let range = begin..begin + 100;
    //         for i in range {
    //             let key = format!("key-{:04}", i);
    //             let expected_value = format!("value-{:04}", i);
    //             let value = state.get(key.as_bytes()).await.unwrap().unwrap();
    //             assert_eq!(expected_value.as_bytes(), value.as_bytes());
    //         }
    //     }
    // }

    async fn prepare_sstables(dir: &TempDir) -> (LsmStorageState<LocalFs>, Sstables<FileObject>) {
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(4)
            .max_bytes_for_level_base(2048)
            .level_size_multiplier_2_exponent(1)
            .build();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(CompactionOptions::Leveled(compaction_options))
            .enable_wal(false)
            .enable_mvcc(true)
            .build();
        let state = LsmStorageState::new(options, persistent).await.unwrap();
        let _next_sst_id = AtomicUsize::default();
        let state_lock = Mutex::default();

        for i in 0..5 {
            let guard = state_lock.lock().await;
            let begin = i * 100;
            insert_sst(&state, begin..begin + 100).await.unwrap();
            state.force_flush_imm_memtable(&guard).await.unwrap();
        }

        let sstables = Clone::clone(state.inner.load().sstables_state().as_ref());
        (state, sstables)
    }
}
