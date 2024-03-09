use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;
use std::iter;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: Option<KeyVec>,
}

// todo: 这里有许多 u16，u8，usize 的转换，需要进行错误处理
impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::default(),
            data: Vec::default(),
            block_size,
            first_key: None,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty() && self.size() + Self::new_data_size(key, value) > self.block_size {
            false
        } else {
            self.add_inner(key, value);
            true
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn size(&self) -> usize {
        let data_size = self.data.len();
        let offsets_size = self.offsets.len() * 2;
        let extra_size = 2;
        data_size + offsets_size + extra_size
    }

    pub fn new_data_size(key: KeySlice, value: &[u8]) -> usize {
        let key_size = key.len() + 2;
        let value_size = value.len() + 2;
        let offset_size = 2;
        key_size + value_size + offset_size
    }

    pub fn add_inner(&mut self, key: KeySlice, value: &[u8]) {
        let offset = self.data.len();

        // may compress key
        if let Some(first_key) = &self.first_key {
            compress_key(first_key, key, &mut self.data);
        } else {
            // first key
            self.data.extend((key.len() as u16).to_be_bytes());
            self.data.extend(key.raw_ref());
        }

        self.data.extend((value.len() as u16).to_be_bytes());
        self.data.extend(value);
        self.offsets.push(offset as u16);

        // add first key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_key_vec());
        }
    }
}

fn compress_key(first_key: &KeyVec, key: KeySlice, buffer: &mut Vec<u8>) {
    let first_key = first_key.raw_ref();
    let key = key.raw_ref();

    let common_prefix = iter::zip(first_key.iter(), key.iter())
        .take_while(|&(left, right)| left == right)
        .count();
    let postfix = key.len() - common_prefix;
    buffer.put_u16(common_prefix as u16);
    buffer.put_u16(postfix as u16);
    if postfix > 0 {
        buffer.extend_from_slice(&key[common_prefix..]);
    }
}
