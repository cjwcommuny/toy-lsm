use derive_new::new;
use getset::CopyGetters;
use std::cmp::max;

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

pub fn compute_compact_priority<'a, File: PersistentHandle>(
    options: &LeveledCompactionOptions,
    level: impl IntoIterator<Item = &'a SsTable<File>> + 'a,
) -> f64 {
    let (count, size) = level
        .into_iter()
        .fold((0, 0), |(prev_count, prev_size), table| {
            (prev_count + 1, prev_size + table.table_size())
        });

    let count_priority = count as f64 / options.level0_file_num_compaction_trigger() as f64;
    let size_priority = size as f64 / options.max_bytes_for_level_base() as f64;
    let count_priority = NotNan::new(count_priority).unwrap();
    let size_priority = NotNan::new(size_priority).unwrap();

    max(count_priority, size_priority).into_inner()
}
