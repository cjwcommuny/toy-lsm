use derive_new::new;
use getset::CopyGetters;
use std::cmp::max;
use std::iter;

use ordered_float::NotNan;

use crate::persistent::PersistentHandle;
use crate::sst::SsTable;
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

pub fn compute_compact_priority(
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

pub fn select_level_destination(
    options: &LeveledCompactionOptions,
    source: usize,
    table_sizes: impl IntoIterator<Item = u64>,
) -> usize {
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
        .unwrap()
        .0
}

#[cfg(test)]
mod tests {
    use crate::sst::compact::leveled::{compute_compact_priority, select_level_destination};
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
        assert_eq!(select_level_destination(&options, 0, [300, 0, 0, 0]), 3);
        assert_eq!(select_level_destination(&options, 0, [300, 0, 300, 600]), 2);
        assert_eq!(select_level_destination(&options, 2, [300, 0, 300, 600]), 3);
    }
}
