use crate::sst::compact::CompactionOptions;
use derive_getters::Getters;
use typed_builder::TypedBuilder;

#[derive(Debug, Default, TypedBuilder, Getters)]
pub struct SstOptions {
    // Block size in bytes
    block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    num_memtable_limit: usize,
    compaction_option: CompactionOptions,
    enable_wal: bool,
}
