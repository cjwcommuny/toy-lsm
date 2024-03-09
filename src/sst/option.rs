use crate::sst::compact::CompactionOptions;
use derive_getters::Getters;
use typed_builder::TypedBuilder;

#[derive(Debug, Default, TypedBuilder, Getters)]
pub struct SstOptions {
    // Block size in bytes
    block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    target_sst_size: usize,
    compaction_option: CompactionOptions,
}
