use crate::persistent::SstHandle;
use crate::sst::compact::common::{CompactionTask, NewCompactionTask, SourceIndex};
use crate::sst::Sstables;

#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactionOptions;

pub fn generate_full_compaction_task<File: SstHandle>(
    sstables: &Sstables<File>,
) -> Option<NewCompactionTask> {
    let len = sstables.l0_sstables.len();
    let l0_minmax = sstables.get_l0_key_minmax().unwrap();
    let source_ids = sstables.levels[0].clone();
    let destination_level = 1;
    let destination_ids: Vec<_> = sstables
        .select_table_by_range(destination_level, &l0_minmax)
        .collect();
    let task = NewCompactionTask::new(0, source_ids, destination_level, destination_ids);
    Some(task)
}
