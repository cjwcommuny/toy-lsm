use std::cmp::Ordering;

use crate::key::{Key, KeyBytes};
use bytes::Bytes;

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
        PartialEq::eq(&self.key, &other.key)
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

impl<K, V> Keyed<K, V> {
    pub fn into_tuple(self) -> (K, V) {
        let Self { key, value } = self;
        (key, value)
    }
}

impl<K, V> Keyed<Key<K>, V> {
    pub fn into_timed_tuple(self) -> (Keyed<K, V>, u64) {
        let Self { key, value } = self;
        let (key, timestamp) = key.into_tuple();
        (Keyed { key, value }, timestamp)
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

#[cfg(test)]
impl InnerEntry {
    pub fn prune_ts(self) -> Entry {
        Entry {
            key: self.key.into_inner(),
            value: self.value,
        }
    }
}
