use crate::sst::compact::common::NewCompactionTask;
use crate::sst::Sstables;

#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactionOptions;

pub fn generate_full_compaction_task<File>(sstables: &Sstables<File>) -> Option<NewCompactionTask> {
    let l0_minmax = sstables.get_l0_key_minmax()?;
    let source_ids = sstables.table_ids(0).clone();
    let destination_level = 1;
    let destination_ids: Vec<_> = sstables
        .select_table_by_range(destination_level, &l0_minmax)
        .collect();
    let task = NewCompactionTask::new(0, source_ids, destination_level, destination_ids);
    Some(task)
}
