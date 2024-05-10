use std::cmp::max;
use std::iter;

use derive_new::new;
use getset::CopyGetters;
use ordered_float::NotNan;
use typed_builder::TypedBuilder;

use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::compact::common::{apply_compaction, compact_generate_new_sst};
use crate::sst::compact::CompactionOptions::Leveled;
use crate::sst::{SstOptions, Sstables};
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
    next_sst_id: impl Fn() -> usize,
    options: &SstOptions,
    persistent: &P,
) -> anyhow::Result<()> {
    let Leveled(compact_options) = options.compaction_option() else {
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
    next_sst_id: impl Fn() -> usize + Sized,
    options: &SstOptions,
    persistent: &P,
    source: usize,
    destination: usize,
) -> anyhow::Result<()> {
    // select the oldest sst
    let source_level = sstables.tables(source).next_back();

    let destination_level = sstables.tables(destination);
    let mut new_sst = compact_generate_new_sst(
        source_level,
        destination_level,
        next_sst_id,
        options,
        persistent,
    )
    .await?;

    let source_len = sstables.table_ids(source).len();
    apply_compaction(
        sstables,
        source_len - 1..source_len,
        source,
        destination,
        new_sst,
    );

    Ok(())
}

// async fn force_compact_level()

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
    let table_sizes = iter::once(&sstables.l0_sstables)
        .chain(&sstables.levels)
        .map(|level| {
            level
                .iter()
                .map(|id| sstables.sstables.get(id).unwrap().table_size())
                .sum()
        });
    select_level_destination_impl(options, source, table_sizes)
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
    table_sizes: impl IntoIterator<Item = u64>,
) -> Option<usize> {
    let max_bytes_for_level_base = options.max_bytes_for_level_base();
    let max_size = table_sizes.into_iter().max().unwrap();
    let last_level_target_size = max(max_size, max_bytes_for_level_base);
    let target_sizes = {
        let mut target_sizes: Vec<_> = iter::successors(Some(last_level_target_size), |prev| {
            Some(prev / options.level_size_multiplier() as u64)
        })
        .take(options.max_levels())
        .collect();
        target_sizes.reverse();
        target_sizes
    };

    target_sizes
        .into_iter()
        .enumerate()
        .skip(source + 1)
        .peekable()
        .find(|(level, target_size_next_level)| *target_size_next_level >= max_bytes_for_level_base)
        .map(|(level, _)| level)
}

#[cfg(test)]
mod tests {
    use proptest::collection::vec;
    use std::ops::{Deref, Range, RangeBounds};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Mutex;
    use tracing_subscriber::fmt::format;

    use crate::persistent::memory::Memory;
    use crate::persistent::Persistent;
    use crate::sst::compact::leveled::{
        compute_compact_priority, force_compact_level, select_level_destination_impl,
    };
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    use crate::sst::sstables::build_next_sst_id;
    use crate::sst::SstOptions;
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
        assert_eq!(
            select_level_destination_impl(&options, 0, [300, 0, 0, 0]),
            Some(3)
        );
        assert_eq!(
            select_level_destination_impl(&options, 0, [300, 0, 300, 600]),
            Some(2)
        );
        assert_eq!(
            select_level_destination_impl(&options, 2, [300, 0, 300, 600]),
            Some(3)
        );
    }

    #[tokio::test]
    async fn test_force_compact_level() {
        let persistent = Memory::default();
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(4)
            .max_bytes_for_level_base(1)
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

        let mut sstables = Clone::clone(state.inner.load().sstables_state().as_ref());

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
