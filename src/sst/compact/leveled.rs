use std::cmp::max;
use std::future::{ready, Future};
use std::iter;
use std::sync::Arc;

use derive_new::new;
use getset::CopyGetters;
use ordered_float::NotNan;
use tracing::{info, trace};
use typed_builder::TypedBuilder;

use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::compact::common::{apply_compaction, compact_generate_new_sst};
use crate::sst::compact::CompactionOptions::Leveled;
use crate::sst::{SsTable, SstOptions, Sstables};
use crate::utils::num::power_of_2;

#[derive(Debug, Clone, new, TypedBuilder, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct LeveledCompactionOptions {
    #[getset(skip)]
    level_size_multiplier_2_exponent: usize,

    level0_file_num_compaction_trigger: usize,

    max_levels: usize,

    max_bytes_for_level_base: u64,
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

pub async fn force_compaction<P: Persistent>(
    sstables: &mut Sstables<P::Handle>,
    next_sst_id: impl Fn() -> usize + Send + Sync,
    options: &SstOptions,
    persistent: &P,
) -> anyhow::Result<()> {
    let Leveled(compact_options) = options.compaction_option() else {
        trace!("skip force compaction");
        return Ok(());
    };

    // todo: only select one source sst
    let Some(source) = select_level_source(sstables, compact_options) else {
        return Ok(());
    };
    let Some(destination) = select_level_destination(sstables, compact_options, source) else {
        return Ok(());
    };
    force_compact_level(
        sstables,
        next_sst_id,
        options,
        persistent,
        source,
        destination,
    )
    .await
}

async fn force_compact_level<P: Persistent>(
    sstables: &mut Sstables<<P as Persistent>::Handle>,
    next_sst_id: impl Fn() -> usize + Send + Sync,
    options: &SstOptions,
    persistent: &P,
    source: usize,
    destination: usize,
) -> anyhow::Result<()> {
    // select the oldest sst
    let source_index_and_id = sstables
        .table_ids(source)
        .iter()
        .copied()
        .enumerate()
        .min_by(|(_, left_id), (_, right_id)| left_id.cmp(right_id));
    let source_level =
        source_index_and_id.map(|(_, id)| sstables.sstables.get(&id).unwrap().as_ref());
    let new_sst = {
        let destination_level = sstables.tables(destination);
        compact_generate_new_sst(
            source_level,
            destination_level,
            next_sst_id,
            options,
            persistent,
        )
        .await?
    };

    let source_range = match source_index_and_id {
        Some((index, _)) => index..index + 1,
        None => 0..0, // empty range
    };
    apply_compaction(sstables, source_range, source, destination, new_sst);

    Ok(())
}

fn select_level_source<File>(
    sstables: &Sstables<File>,
    options: &LeveledCompactionOptions,
) -> Option<usize>
where
    File: PersistentHandle,
{
    let source = iter::once(&sstables.l0_sstables)
        .chain(&sstables.levels)
        .map(|level| {
            let level = level.iter().map(|id| {
                let table = sstables.sstables.get(id).unwrap();
                table.table_size()
            });
            compute_compact_priority(options, level)
        })
        .filter(|&priority| priority >= 1.0) // 只有大于 1 才会 trigger compaction
        .enumerate()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .expect("BUG: must have max score")
        .0;
    if source == options.max_levels() {
        None
    } else {
        Some(source)
    }
}

fn select_level_destination<File>(
    sstables: &Sstables<File>,
    options: &LeveledCompactionOptions,
    source: usize,
) -> Option<usize>
where
    File: PersistentHandle,
{
    let last_level_table_size: u64 = sstables
        .levels
        .last()?
        .iter()
        .map(|id| sstables.sstables.get(id).unwrap().table_size())
        .sum();
    select_level_destination_impl(options, source, last_level_table_size)
}

fn compute_compact_priority(
    options: &LeveledCompactionOptions,
    table_sizes: impl IntoIterator<Item = u64>,
) -> f64 {
    let (count, size) = table_sizes
        .into_iter()
        .fold((0, 0), |(prev_count, prev_size), table_size| {
            (prev_count + 1, prev_size + table_size)
        });

    let count_priority = count as f64 / options.level0_file_num_compaction_trigger() as f64;
    let size_priority = size as f64 / options.max_bytes_for_level_base() as f64;
    let count_priority = NotNan::new(count_priority).unwrap();
    let size_priority = NotNan::new(size_priority).unwrap();

    max(count_priority, size_priority).into_inner()
}

fn select_level_destination_impl(
    options: &LeveledCompactionOptions,
    source: usize,
    last_level_table_size: u64,
) -> Option<usize> {
    if source == options.max_levels() - 1 {
        return None;
    }

    let max_bytes_for_level_base = options.max_bytes_for_level_base();
    let last_level_target_size = max(last_level_table_size, max_bytes_for_level_base);
    let target_sizes = {
        let mut target_sizes: Vec<_> = iter::successors(
            Some((
                last_level_target_size / options.level_size_multiplier() as u64,
                last_level_target_size,
            )),
            |(size, next_size)| Some((size / options.level_size_multiplier() as u64, *size)),
        )
        .take(options.max_levels() - 1)
        .collect();
        target_sizes.reverse();
        target_sizes
    };

    let destination = target_sizes
        .into_iter()
        .enumerate()
        .skip(source + 1)
        .find(|(level, (_, target_size_next_level))| {
            *target_size_next_level > max_bytes_for_level_base
        })
        .map(|(level, _)| level)
        .unwrap_or(options.max_levels - 1);
    Some(destination)
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;
    use std::ops::Range;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use crate::persistent::memory::{Memory, MemoryObject};
    use crate::persistent::Persistent;
    use crate::sst::compact::leveled::{
        compute_compact_priority, force_compact_level, force_compaction,
        select_level_destination_impl,
    };
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    use crate::sst::sstables::build_next_sst_id;
    use crate::sst::{SstOptions, Sstables};
    use crate::state::{LsmStorageState, Map};

    #[test]
    fn test_compute_compact_priority() {
        let level = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(
            compute_compact_priority(&LeveledCompactionOptions::new(2, 10, 10, 100,), level),
            1.0
        );
        assert_eq!(
            compute_compact_priority(&LeveledCompactionOptions::new(2, 10, 10, 1,), level),
            55.0
        );
    }

    #[test]
    fn test_select_level_destination() {
        let options = LeveledCompactionOptions::new(1, 10, 4, 300);
        assert_eq!(select_level_destination_impl(&options, 0, 0), Some(3));
        assert_eq!(select_level_destination_impl(&options, 0, 600), Some(2));
        assert_eq!(select_level_destination_impl(&options, 2, 600), Some(3));
    }

    #[tokio::test]
    async fn test_force_compact_level() {
        let (state, mut sstables) = prepare_sstables().await;

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1, 0]);
            assert_eq!(sstables.levels, vec![vec![], vec![], vec![]]);
            assert_eq!(sstables.sstables.len(), 5);
        }

        force_compact_level(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            0,
            1,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1]);
            assert_eq!(sstables.levels, vec![vec![9, 10], vec![], vec![]]);
            assert_eq!(sstables.sstables.len(), 6);
        }

        force_compact_level(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            0,
            1,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2]);
            assert_eq!(sstables.levels, vec![vec![12, 13, 14], vec![], vec![]]);
            assert_eq!(sstables.sstables.len(), 6);
        }

        force_compact_level(
            &mut sstables,
            build_next_sst_id(&state.sst_id),
            &state.options,
            &state.persistent,
            1,
            2,
        )
        .await
        .unwrap();

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2]);
            assert_eq!(sstables.levels, vec![vec![13, 14], vec![16], vec![]]);
            assert_eq!(sstables.sstables.len(), 6);
        }
    }

    #[tokio::test]
    async fn test_force_compaction() {
        let (state, mut sstables) = prepare_sstables().await;
        force_compaction(
            &mut sstables,
            || state.next_sst_id(),
            &state.options,
            &state.persistent,
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

    async fn prepare_sstables() -> (LsmStorageState<Memory>, Sstables<Arc<MemoryObject>>) {
        let persistent = Memory::default();
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(4)
            .max_bytes_for_level_base(2048)
            .level0_file_num_compaction_trigger(1)
            .level_size_multiplier_2_exponent(1)
            .build();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(CompactionOptions::Leveled(compaction_options))
            .build();
        let mut state = LsmStorageState::new(options, persistent);
        let next_sst_id = AtomicUsize::default();
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

    async fn insert_sst<P: Persistent>(
        state: &LsmStorageState<P>,
        range: Range<u64>,
    ) -> anyhow::Result<()> {
        for i in range {
            let key = format!("key-{:04}", i);
            let value = format!("value-{:04}", i);
            state.put(key, value).await?;
        }
        Ok(())
    }
}
