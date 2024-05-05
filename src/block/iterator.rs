// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use crate::block::blocks::Block;
use crate::entry::Entry;
use crate::key::{Key, KeySlice};
use std::sync::Arc;
use tracing::info;

// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self { block, idx: 0 }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self::new(block)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut current = self.block.len();
        for index in 0..self.block.len() {
            let (this_key, _) = self.block.get_entry_ref(index);
            let this_key = Key::from_slice(this_key);
            if this_key >= key {
                current = index;
                break;
            }
        }
        self.idx = current;
    }
}

impl Iterator for BlockIterator {
    type Item = anyhow::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.block.len() {
            return None;
        }
        let entry = self.block.get_entry(self.idx);
        self.idx += 1;
        Some(Ok(entry))
    }
}
