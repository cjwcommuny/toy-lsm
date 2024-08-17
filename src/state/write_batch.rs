use crate::entry::Keyed;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum WriteBatchRecord {
    Put(Bytes, Bytes),
    Del(Bytes),
}

impl WriteBatchRecord {
    pub fn get_key(&self) -> &Bytes {
        match self {
            WriteBatchRecord::Put(key, _) => key,
            WriteBatchRecord::Del(key) => key,
        }
    }

    pub fn into_keyed(self) -> Keyed<Bytes, Bytes> {
        match self {
            WriteBatchRecord::Put(key, value) => Keyed::new(key, value),
            WriteBatchRecord::Del(key) => Keyed::new(key, Bytes::new()),
        }
    }
}
