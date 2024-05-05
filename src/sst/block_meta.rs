// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use crate::key::KeyBytes;
use bytes::Buf;
use derive_getters::Getters;

// todo: first key 和 last key 能不能存 offset，减少存储消耗
#[derive(Clone, Debug, PartialEq, Eq, Getters)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    pub fn encode(&self) -> impl Iterator<Item = u8> + '_ {
        let offset = (self.offset as u16).to_be_bytes().into_iter();
        let first_key_len = (self.first_key.len() as u16).to_be_bytes().into_iter();
        let first_key = self.first_key.raw_ref().iter().copied();
        let last_key_len = (self.last_key.len() as u16).to_be_bytes().into_iter();
        let last_key = self.last_key.raw_ref().iter().copied();
        offset
            .chain(first_key_len)
            .chain(first_key)
            .chain(last_key_len)
            .chain(last_key)
    }

    pub fn decode(mut data: impl Buf) -> Self {
        let offset = data.get_u16() as usize;
        let first_key_len = data.get_u16() as usize;
        let first_key = data.copy_to_bytes(first_key_len);
        let last_key_len = data.get_u16() as usize;
        let last_key = data.copy_to_bytes(last_key_len);
        Self {
            offset,
            first_key: KeyBytes::from_bytes(first_key),
            last_key: KeyBytes::from_bytes(last_key),
        }
    }

    pub fn decode_chunk(mut data: impl Buf) -> Vec<Self> {
        let mut result = Vec::new();
        while data.remaining() > 0 {
            result.push(Self::decode(&mut data))
        }
        result
    }
}
