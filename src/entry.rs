use bytes::Bytes;
use std::cmp;

#[derive(Debug, Eq)]
pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
}

impl PartialEq<Self> for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl PartialOrd<Self> for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.key.cmp(&other.key)
    }
}
