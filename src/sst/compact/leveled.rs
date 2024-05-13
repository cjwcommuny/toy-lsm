use std::cmp::max;
use std::future::{ready, Future};
use std::iter;
use std::sync::Arc;

use derive_new::new;
use getset::CopyGetters;
use ordered_float::NotNan;
use tracing::{info, trace};
use typed_builder::TypedBuilder;

use crate::persistent::{SstHandle, SstPersistent};
use crate::sst::compact::common::{apply_compaction, compact_generate_new_sst};
use crate::sst::compact::CompactionOptions::Leveled;
use crate::sst::{SsTable, SstOptions, Sstables};
use crate::utils::num::power_of_2;

#[derive(Debug, Clone, new, TypedBuilder, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct LeveledCompactionOptions {
    #[getset(skip)]
    level_size_multiplier_2_exponent: usize,

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

pub async fn force_compaction<P: SstPersistent>(
    sstables: &mut Sstables<P::Handle>,
    next_sst_id: impl Fn() -> usize + Send + Sync,
    options: &SstOptions,
    persistent: &P,
) -> anyhow::Result<()> {
    let Leveled(compact_options) = options.compaction_option() else {
        trace!("skip force compaction");
        return Ok(());
    };

    let level_sizes = compute_level_sizes(sstables);
    let target_sizes = compute_target_sizes(*level_sizes.last().unwrap(), compact_options);

    // todo: only select one source sst
    let Some(source) = select_level_source(
        &level_sizes,
        &target_sizes,
        compact_options.max_bytes_for_level_base(),
    ) else {
        return Ok(());
    };
    let destination = select_level_destination(compact_options, source, &target_sizes);

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

async fn force_compact_level<P: SstPersistent>(
    sstables: &mut Sstables<<P as SstPersistent>::Handle>,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use nom::AsBytes;
    use tokio::sync::Mutex;

    use crate::persistent::memory::{Memory, MemoryObject};
    use crate::sst::compact::leveled::{
        force_compact_level, force_compaction, select_level_destination, select_level_source,
    };
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
        let options = LeveledCompactionOptions::new(1, 4, 300);
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
        let (state, mut sstables) = prepare_sstables().await;

        {
            assert_eq!(sstables.l0_sstables, [4, 3, 2, 1, 0]);
            assert_eq!(
                sstables.levels,
                vec![Vec::<usize>::new(), Vec::new(), Vec::new()]
            );
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
}
