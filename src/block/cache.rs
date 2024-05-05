// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use crate::block::blocks::Block;
use std::sync::Arc;

pub type BlockCache = moka::future::Cache<(usize, usize), Arc<Block>>;
