use std::cmp::max;

use ordered_float::NotNan;

use crate::persistent::PersistentHandle;
use crate::sst::SsTable;

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub max_bytes_for_level_base: u64,
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

    let count_priority = count as f64 / options.level0_file_num_compaction_trigger as f64;
    let size_priority = size as f64 / options.max_bytes_for_level_base as f64;
    let count_priority = NotNan::new(count_priority).unwrap();
    let size_priority = NotNan::new(size_priority).unwrap();

    max(count_priority, size_priority).into_inner()
}
