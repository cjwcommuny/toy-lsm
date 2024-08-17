use std::iter;

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

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
            encode_key(key, &mut self.data);
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
    let timestamp = key.timestamp();
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
    buffer.put_u64(timestamp);
}

// todo: 太多的 encoding 方法了，需要统一
fn encode_key(key: KeySlice, buffer: &mut Vec<u8>) {
    buffer.put_u16(key.len() as u16);
    buffer.extend(key.raw_ref());
    buffer.put_u64(key.timestamp());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use nom::AsBytes;

    use crate::block::{Block, BlockBuilder, BlockIterator};
    use crate::key::{KeySlice, KeyVec};

    #[test]
    fn test_block_build_single_key() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333"));
        builder.build();
    }

    #[test]
    fn test_block_build_full() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11"));
        assert!(!builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22"));
        builder.build();
    }

    #[test]
    fn test_block_build_large_1() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(
            KeySlice::for_testing_from_slice_no_ts(b"11"),
            &b"1".repeat(100)
        ));
        builder.build();
    }

    #[test]
    fn test_block_build_large_2() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"1"));
        assert!(!builder.add(
            KeySlice::for_testing_from_slice_no_ts(b"11"),
            &b"1".repeat(100)
        ));
    }

    fn key_of(idx: usize) -> KeyVec {
        KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn num_of_keys() -> usize {
        100
    }

    fn generate_block() -> Block {
        let mut builder = BlockBuilder::new(10000);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            assert!(builder.add(key.as_key_slice(), &value[..]));
        }
        builder.build()
    }

    #[test]
    fn test_block_build_all() {
        generate_block();
    }

    #[test]
    fn test_block_encode() {
        let block = generate_block();
        block.encode();
    }

    #[test]
    fn test_block_decode() {
        let block = generate_block();
        let encoded = block.encode();
        let decoded_block = Block::decode(&encoded);
        assert_eq!(block.offsets, decoded_block.offsets);
        assert_eq!(block.data, decoded_block.data);
    }

    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }

    #[test]
    fn test_block_iterator() {
        let block = Arc::new(generate_block());
        for _ in 0..5 {
            let mut iter = BlockIterator::create_and_seek_to_first(block.clone());
            for i in 0..num_of_keys() {
                let entry = iter.next().unwrap().unwrap().prune_ts();
                let key = entry.key.as_bytes();
                let value = entry.value.as_bytes();
                assert_eq!(
                    key,
                    key_of(i).for_testing_key_ref(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).for_testing_key_ref()),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
            }
        }
    }

    #[test]
    fn test_block_seek_key() {
        let block = Arc::new(generate_block());
        let mut iter = BlockIterator::create_and_seek_to_key(block, key_of(0).as_key_slice());
        for offset in 1..=5 {
            for i in 0..num_of_keys() {
                let entry = iter.next().unwrap().unwrap().prune_ts();
                let key = entry.key.as_bytes();
                let value = entry.value.as_bytes();
                assert_eq!(
                    key,
                    key_of(i).for_testing_key_ref(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).for_testing_key_ref()),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(
                    &format!("key_{:03}", i * 5 + offset).into_bytes(),
                ));
            }
            iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(b"k"));
        }
    }
}
