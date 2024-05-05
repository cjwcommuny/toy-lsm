// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

pub mod file_object;
mod interface;
pub mod memory;

pub use file_object::LocalFs;
pub use interface::Persistent;
pub use interface::PersistentHandle;
