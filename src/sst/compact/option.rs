use crate::sst::compact::leveled::LeveledCompactionOptions;

#[derive(Debug, Clone, Default)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    Full,
    /// In no compaction mode (week 1), always flush to L0
    #[default]
    NoCompaction,
}

impl CompactionOptions {
    pub fn concurrency(&self) -> usize {
        match self {
            CompactionOptions::Leveled(options) => options.concurrency(),
            CompactionOptions::Full => 1,
            CompactionOptions::NoCompaction => 1, // todo: NoCompaction 不应该有 concurrency
        }
    }
}
