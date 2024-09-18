use crate::persistent::SstHandle;
use crate::sst::compact::common::NewCompactionTask;
use crate::sst::Sstables;

#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactionOptions;

pub fn generate_full_compaction_task<File: SstHandle>(sstables: &Sstables<File>) -> Option<NewCompactionTask> {
    let source_ids = sstables.table_ids(0).clone();
    let destination_level = 1;
    let destination_ids = sstables.table_ids(1).clone();
    let task = NewCompactionTask::new(0, source_ids, destination_level, destination_ids);
    Some(task)
}
