use bytes::Bytes;
use derive_new::new;
use getset::CopyGetters;
use itertools::Itertools;
use ordered_float::{NotNan, OrderedFloat};
use std::cmp::{max, Ordering};
use std::collections::HashSet;
use std::iter;
use typed_builder::TypedBuilder;

use crate::persistent::SstHandle;
use crate::sst::compact::common::{CompactionTask, NewCompactionTask, SourceIndex};
use crate::sst::Sstables;
use crate::utils::num::power_of_2;
#[derive(Debug, Clone, new, TypedBuilder, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct LeveledCompactionOptions {
    #[getset(skip)]
    level_size_multiplier_2_exponent: usize,

    max_levels: usize,

    max_bytes_for_level_base: u64,
    level0_file_num_compaction_trigger: usize,

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

fn select_level_source(
    level_sizes: &[u64],
    target_sizes: &[u64],
    max_bytes_for_level_base: u64,
) -> Option<usize> {
    let scores: Vec<_> = level_sizes
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
        .collect();
    let source = scores
        .iter()
        .enumerate()
        .filter(|(_, &priority)| priority > 1.0) // 只有大于 1 才会 trigger compaction
        .max_by(|(left_index, left), (right_index, right)| {
            left.total_cmp(right)
                .then_with(|| (-(*left_index as i64)).cmp(&-(*right_index as i64)))
            // todo: make it looking better...
        })?
        .0;
    if source == level_sizes.len() - 1 {
        None
    } else {
        Some(source)
    }
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
    level0_file_num_compaction_trigger: usize,
) -> Vec<NewCompactionTask> {
    let level_sizes = compute_level_sizes(sstables);
    let target_sizes = compute_target_sizes(*level_sizes.last().unwrap(), option);
    let source_levels_sorted = filter_and_sort_source_levels(
        &level_sizes,
        &target_sizes,
        option.max_bytes_for_level_base(),
    );

    let levels_range = Vec::<KeyRange>::new();
    let tables_in_compaction = HashSet::<usize>::new();

    let result = Vec::new();
    for source_level in source_levels_sorted {
        let destination_level = select_level_destination(option, source_level, &target_sizes);
    }

    result
}

fn generate_task_for_level<File: SstHandle>(
    source_level: usize,
    sstables: &Sstables<File>,
    target_sizes: &[u64],
    tables_in_compaction: &mut HashSet<usize>,
    option: &LeveledCompactionOptions,
) -> Vec<(Vec<usize>, Vec<usize>)> {
    if source_level == 0 {
        let l0_minmax = sstables.get_l0_key_minmax().unwrap();
        let destination_level = select_level_destination(option, 0, target_sizes);
        let this_level_table_ids = sstables.levels[0].clone();
        let next_level_table_ids: Vec<_> = sstables
            .select_table_by_range(destination_level, &l0_minmax)
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
            .collect();
        vec![(this_level_table_ids, next_level_table_ids)]
    } else {
        // todo: by priority?
        let x: Vec<_> = sstables
            .tables(source_level)
            .scan(tables_in_compaction, |tables_in_compaction, table| {
                let source_id = *table.id();
                let x = if tables_in_compaction.contains(&source_id) {
                    None
                } else {
                    let minmax = table.get_key_range();
                    let destination_level = source_level + 1;
                    let this_level_table_ids = vec![source_id];
                    let next_level_table_ids: Vec<_> = sstables
                        .select_table_by_range(destination_level, &minmax)
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
                        .collect();
                    if next_level_table_ids.is_empty() {
                        None
                    } else {
                        Some((this_level_table_ids, next_level_table_ids))
                    }
                };
                Some(x)
            })
            .flatten()
            .collect();

        x
    }
}

// fn get_key_range_of_tables()

pub fn generate_task<File: SstHandle>(
    compact_options: &LeveledCompactionOptions,
    sstables: &Sstables<File>,
) -> Option<CompactionTask> {
    let level_sizes = compute_level_sizes(sstables);
    let target_sizes = compute_target_sizes(*level_sizes.last().unwrap(), compact_options);

    // todo: only select one source sst
    let source = select_level_source(
        &level_sizes,
        &target_sizes,
        compact_options.max_bytes_for_level_base(),
    )?;
    let destination = select_level_destination(compact_options, source, &target_sizes);

    let (source_index, _) = sstables
        .table_ids(source)
        .iter()
        .copied()
        .enumerate()
        .min_by(|(_, left_id), (_, right_id)| left_id.cmp(right_id))?;

    let task = CompactionTask::new(
        source,
        SourceIndex::Index {
            index: source_index,
        },
        destination,
    );

    Some(task)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use nom::AsBytes;
    use tempfile::{tempdir, TempDir};
    use tokio::sync::Mutex;

    use crate::persistent::file_object::FileObject;
    use crate::persistent::LocalFs;
    use crate::sst::compact::common::{
        compact_with_task, force_compact, CompactionTask, SourceIndex,
    };
    use crate::sst::compact::leveled::{select_level_destination, select_level_source};
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    use crate::sst::sstables::build_next_sst_id;
    use crate::sst::{SstOptions, Sstables};
    use crate::state::{LsmStorageState, Map};
    use crate::test_utils::insert_sst;

    #[test]
    fn test_select_level_source() {
        assert_eq!(
            select_level_source(&[120, 301, 600], &[150, 300, 600], 300),
            Some(1)
        );
        assert_eq!(
            select_level_source(&[120, 300, 601], &[150, 300, 600], 300),
            None
        );
    }

    #[test]
    fn test_select_level_destination() {
        let options = LeveledCompactionOptions::new(1, 4, 300, 1, 1);
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

    #[tokio::test]
    async fn test_force_compact_level() {
        let dir = tempdir().unwrap();
        let (state, mut sstables) = prepare_sstables(&dir).await;

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1, 0]);
            assert_eq!(
                sstables.levels,
                vec![Vec::<usize>::new(), Vec::new(), Vec::new()]
            );
            assert_eq!(sstables.sstables.len(), 5);
        }

        compact_with_task(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            &CompactionTask::new(0, SourceIndex::Index { index: 4 }, 1),
            None,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1]);
            assert_eq!(sstables.levels, vec![vec![9, 10], vec![], vec![]]);
            assert_eq!(sstables.sstables.len(), 6);
        }

        compact_with_task(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            &CompactionTask::new(0, SourceIndex::Index { index: 3 }, 1),
            None,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2]);
            assert_eq!(sstables.levels, vec![vec![12, 13, 14, 15], vec![], vec![]]);
            assert_eq!(sstables.sstables.len(), 7);
        }

        compact_with_task(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            &CompactionTask::new(1, SourceIndex::Index { index: 0 }, 2),
            None,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2]);
            assert_eq!(sstables.levels, vec![vec![13, 14, 15], vec![17], vec![]]);
            assert_eq!(sstables.sstables.len(), 7);
        }
    }

    #[tokio::test]
    async fn test_force_compaction() {
        let dir = tempdir().unwrap();
        let (state, mut sstables) = prepare_sstables(&dir).await;
        force_compact(
            &mut sstables,
            || state.next_sst_id(),
            &state.options,
            &state.persistent,
            None,
            None,
        )
        .await
        .unwrap();
        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1]);
            assert_eq!(sstables.levels, vec![vec![], vec![], vec![9, 10]]);
            assert_eq!(sstables.sstables.len(), 6);
        }

        for i in 0..5 {
            let begin = i * 100;
            let range = begin..begin + 100;
            for i in range {
                let key = format!("key-{:04}", i);
                let expected_value = format!("value-{:04}", i);
                let value = state.get(key.as_bytes()).await.unwrap().unwrap();
                assert_eq!(expected_value.as_bytes(), value.as_bytes());
            }
        }
    }

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
