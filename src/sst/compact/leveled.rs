use anyhow::{anyhow, Error};
use derive_new::new;
use getset::CopyGetters;
use std::cmp::max;
use std::iter;

use ordered_float::NotNan;

use crate::persistent::{Persistent, PersistentHandle};
use crate::sst::compact::common::apply_compaction;
use crate::sst::compact::CompactionOptions::Leveled;
use crate::sst::{SsTable, SstOptions, Sstables};
use crate::utils::num::power_of_2;

#[derive(Debug, Clone, new, CopyGetters)]
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
    let new_sst = Sstables::compact_generate_new_sst(
        source_level,
        destination_level,
        next_sst_id,
        options,
        persistent,
    )
    .await?;

    let source_len = sstables.table_ids(source).len();
    apply_compaction(sstables, source_len - 1.., source, destination, new_sst);

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
    use crate::sst::compact::leveled::{compute_compact_priority, select_level_destination_impl};
    use crate::sst::compact::LeveledCompactionOptions;

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
}
