// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

mod leveled;
mod option;
mod simple_leveled;
mod tiered;

pub use leveled::LeveledCompactionOptions;
pub use option::CompactionOptions;
pub use simple_leveled::SimpleLeveledCompactionOptions;
