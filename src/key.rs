use bytes::{Buf, Bytes};
use derive_new::new;
use nom::AsBytes;

use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(PartialEq, Eq, Debug, new, Default, Clone, Copy)]
pub struct Key<T> {
    key: T,
    timestamp: u64,
}

pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

impl<T> Key<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Key<U> {
        Key::new(f(self.key), self.timestamp)
    }

    pub fn as_ref(&self) -> Key<&T> {
        Key::new(&self.key, self.timestamp)
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn into_tuple(self) -> (T, u64) {
        (self.key, self.timestamp)
    }
}

impl<T> From<(T, u64)> for Key<T> {
    fn from(pair: (T, u64)) -> Self {
        Self {
            key: pair.0,
            timestamp: pair.1,
        }
    }
}

#[cfg(test)]
impl Key<Bytes> {
    pub fn new_for_test(key: &[u8], ts: u64) -> Self {
        Self::new(Bytes::copy_from_slice(key), ts)
    }
}

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.key
    }

    pub fn len(&self) -> usize {
        self.key.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.key.as_ref().is_empty()
    }

    pub fn copy_to_key_bytes(self) -> KeyBytes {
        self.map(|slice| Bytes::copy_from_slice(slice.as_ref()))
    }

    pub fn to_byte_iter(&self) -> impl Iterator<Item = u8> + '_ {
        let key = self.key.as_ref();
        let key_len = (key.len() as u16).to_be_bytes().into_iter();
        let key = key.iter().copied();
        let timestamp = self.timestamp.to_be_bytes().into_iter();

        key_len.chain(key).chain(timestamp)
    }
}

impl Key<Vec<u8>> {
    pub fn as_key_slice(&self) -> KeySlice {
        self.as_ref().map(Vec::as_slice)
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        self.map(Into::into)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self::new(key, 0)
    }
}

impl Key<Bytes> {
    pub fn as_key_slice(&self) -> KeySlice {
        self.as_ref().map(|b| b.as_bytes())
    }

    /// Create a `KeyBytes` from a `Bytes`. Will be removed in week 3.
    pub fn from_bytes(_bytes: Bytes) -> KeyBytes {
        todo!()
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn for_testing_from_bytes_no_ts(_bytes: Bytes) -> KeyBytes {
        todo!()
        // Key(bytes)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        todo!()
        // self.0.as_ref()
    }

    pub fn decode(buf: &mut impl Buf) -> KeyBytes {
        let key_len = buf.get_u16() as usize;
        let key = buf.copy_to_bytes(key_len);
        let timestamp = buf.get_u64();
        Key::new(key, timestamp)
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        self.map(|key| key.to_vec())
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(self) -> &'a [u8] {
        self.key
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        todo!()
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self::new(slice, 123)
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], ts: u64) -> Self {
        Self::new(slice, ts)
    }
}

impl<T: PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let key_order = self.key.partial_cmp(&other.key)?;
        match key_order {
            Ordering::Equal => self
                .timestamp
                .partial_cmp(&other.timestamp)
                .map(Ordering::reverse),
            _ => Some(key_order),
        }
    }
}

impl<T: Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.timestamp.cmp(&other.timestamp).reverse())
    }
}
