use std::cmp::Ordering;

use bytes::Bytes;
use crate::key::KeyBytes;

pub type Entry = Keyed<Bytes, Bytes>;
pub type InnerEntry = Keyed<KeyBytes, Bytes>;

#[derive(Debug)]
pub struct Keyed<K, V> {
    pub key: K,
    pub value: V,
}

impl<K: Eq, V> Eq for Keyed<K, V> {}

impl<K: PartialEq, V> PartialEq<Self> for Keyed<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K: PartialOrd, V> PartialOrd for Keyed<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K: Ord, V> Ord for Keyed<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[cfg(test)]
impl<K, V> Keyed<K, V> {
    pub fn into_tuple(self) -> (K, V) {
        let Self { key, value } = self;
        (key, value)
    }


}

#[cfg(test)]
impl Keyed<Bytes, Bytes> {
    pub fn from_slice(key: &[u8], value: &[u8]) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }
    }
}