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
