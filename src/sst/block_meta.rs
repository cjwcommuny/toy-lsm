use crate::key::{KeyBytes, KeySlice};
use bytes::Buf;
use derive_getters::Getters;

// todo: first key 和 last key 能不能存 offset，减少存储消耗
#[derive(Clone, Debug, PartialEq, Eq, Getters)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,

    /// The first key of the data block.
    #[getter(skip)]
    pub first_key: KeyBytes,

    /// The last key of the data block.
    #[getter(skip)]
    pub last_key: KeyBytes,
}

impl BlockMeta {
    pub fn encode(&self) -> impl Iterator<Item = u8> + '_ {
        let offset = (self.offset as u16).to_be_bytes().into_iter();
        offset
            .chain(self.first_key.to_byte_iter())
            .chain(self.last_key.to_byte_iter())
    }

    pub fn decode(mut data: impl Buf) -> Self {
        let offset = data.get_u16() as usize;
        let first_key = KeyBytes::decode(&mut data);
        let last_key = KeyBytes::decode(&mut data);
        Self {
            offset,
            first_key,
            last_key,
        }
    }

    pub fn decode_chunk(mut data: impl Buf) -> Vec<Self> {
        let mut result = Vec::new();
        while data.remaining() > 0 {
            result.push(Self::decode(&mut data))
        }
        result
    }

    pub fn first_key(&self) -> KeySlice {
        KeySlice::new(self.first_key.raw_ref(), self.first_key.timestamp())
    }

    pub fn last_key(&self) -> KeySlice {
        KeySlice::new(self.last_key.raw_ref(), self.last_key.timestamp())
    }
}
