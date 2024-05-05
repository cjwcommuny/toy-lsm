// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use crate::sst::compact::leveled::LeveledCompactionOptions;
use crate::sst::compact::simple_leveled::SimpleLeveledCompactionOptions;
use crate::sst::compact::tiered::TieredCompactionOptions;

#[derive(Debug, Clone, Default)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    #[default]
    NoCompaction,
}
