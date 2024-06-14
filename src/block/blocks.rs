use crate::key::KeyBytes;
use bytes::{Buf, Bytes};

use crate::entry::{Entry, InnerEntry};

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Debug)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        bytes.extend_from_slice(&self.data);

        for offset in &self.offsets {
            bytes.extend(offset.to_be_bytes().iter());
        }

        bytes.extend((self.offsets.len() as u16).to_be_bytes().iter());

        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(bytes: &[u8]) -> Self {
        let extra_begin = bytes.len() - 2;

        let num = u16::from_be_bytes([bytes[extra_begin], bytes[extra_begin + 1]]) as usize;
        let offsets_begin = bytes.len() - 2 - num * 2;

        let offsets = {
            let mut offsets = Vec::with_capacity(num);
            for by in bytes[offsets_begin..extra_begin].chunks_exact(2) {
                let u = u16::from_be_bytes(by.try_into().unwrap());
                offsets.push(u);
            }
            offsets
        };

        let data = Vec::from(&bytes[..offsets_begin]);

        Self { data, offsets }
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn get_entry_ref(&self, index: usize) -> (&[u8], &[u8]) {
        // get key
        let (data, key) = self.parse_key_ref(index);

        // get value
        let (_, value) = get_value(data);

        (key, value)
    }

    pub fn get_entry_ref_inner(data: &[u8]) -> (&[u8], &[u8], &[u8]) {
        let (data, key) = get_value(data);
        let (data, value) = get_value(data);
        (data, key, value)
    }

    fn parse_key_ref(&self, index: usize) -> (&[u8], &[u8]) {
        let data = &self.data[self.offsets[index] as usize..];

        if index == 0 {
            Self::get_uncompressed_key_ref(data)
        } else {
            let first_key = self.first_key_ref();
            Self::get_compressed_key_ref(first_key, data)
        }
    }

    fn get_uncompressed_key_ref(data: &[u8]) -> (&[u8], &[u8]) {
        let (raw_key, data) = get_value(data);
        let (data, timestamp) = get_u64(data);

    }

    fn get_compressed_key_ref<'b>(first_key: &[u8], data: &'b [u8]) -> (&'b [u8], &'b [u8]) {
        let (data, common_prefix_len) = get_u16(data);
        let prefix = &first_key[..common_prefix_len];

        let (data, postfix_len) = get_u16(data);
        let (data, postfix) = get_data_by_len(data, postfix_len);

        // todo: 这里需要能把 (prefix: &[u8], postfix: &[u8]) 当作 &[u8] 的相关数据结构 (tuple of slices)
        let key = prefix
            .iter()
            .copied()
            .chain(postfix.iter().copied())
            .collect::<Vec<_>>()
            .leak();

        (data, key)
    }

    pub fn get_entry(&self, index: usize) -> InnerEntry {
        let (key, value) = self.get_entry_ref(index);
        let key = Bytes::copy_from_slice(key);
        let value = Bytes::copy_from_slice(value);
        // Entry { key, value }
        todo!()
    }

    pub fn first_key(&self) -> KeyBytes {
        let key = self.first_key_ref();
        KeyBytes::from_bytes(Bytes::copy_from_slice(key))
    }

    fn first_key_ref(&self) -> &[u8] {
        let (_, key) = self.parse_key_ref(0);
        key
    }

    pub fn last_key(&self) -> KeyBytes {
        let (_, key) = self.parse_key_ref(self.offsets.len() - 1);
        KeyBytes::from_bytes(Bytes::copy_from_slice(key))
    }
}

fn get_value(data: &[u8]) -> (&[u8], &[u8]) {
    let (data, len) = get_u16(data);
    let (data, value) = get_data_by_len(data, len);
    (data, value)
}

fn get_u16(data: &[u8]) -> (&[u8], usize) {
    let new_data = &data[2..];
    let value = u16::from_be_bytes([data[0], data[1]]) as usize;
    (new_data, value)
}

fn get_u64(data: &[u8]) -> (&[u8], u64) {
    let new_data = &data[8..];
    let value = (&data[..8]).get_u64();
    (new_data, value)
}

fn get_data_by_len(data: &[u8], len: usize) -> (&[u8], &[u8]) {
    (&data[len..], &data[..len])
}
