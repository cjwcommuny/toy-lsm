use crate::persistent::SstHandle;
use crate::sst::compact::common::{CompactionTask, SourceIndex};
use crate::sst::Sstables;

#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactionOptions;

pub fn generate_full_compaction_task<File: SstHandle>(
    sstables: &Sstables<File>,
) -> Option<CompactionTask> {
    let len = sstables.l0_sstables.len();
    let task = CompactionTask::new(0, SourceIndex::Full { len }, 1);
    Some(task)
}
