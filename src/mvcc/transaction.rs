use crate::entry::Entry;

use crate::persistent::Persistent;
use crate::state::{LsmStorageStateInner, Map};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use futures::{stream, Stream};
use std::collections::{Bound, HashSet};

use crate::mvcc::iterator::txn_local_iterator;
use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct Transaction<P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) inner: arc_swap::Guard<Arc<LsmStorageStateInner<P>>>,

    // todo: need Arc<...> ?
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    /// todo: check deadlock?
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl<P: Persistent> Map for Transaction<P> {
    type Error = anyhow::Error;

    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
        todo!()
    }

    async fn put(
        &self,
        _key: impl Into<Bytes> + Send,
        _value: impl Into<Bytes> + Send,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn delete(&self, _key: impl Into<Bytes> + Send) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<P: Persistent> Transaction<P> {
    pub fn new(
        read_ts: u64,
        inner: arc_swap::Guard<Arc<LsmStorageStateInner<P>>>,
        key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
    ) -> Self {
        Self {
            read_ts,
            inner,
            local_storage: Arc::default(),
            committed: Arc::default(),
            key_hashes,
        }
    }

    pub fn scan<'a>(
        &self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Entry>>> {
        let local_iterator = txn_local_iterator(
            &self.local_storage,
            lower.map(Bytes::copy_from_slice),
            upper.map(Bytes::copy_from_slice),
        );

        // todo
        Ok(stream::empty())
    }

    pub async fn commit(self) -> anyhow::Result<()> {
        todo!()
    }
}

#[cfg(test)]
impl<P: Persistent> Transaction<P> {
    pub async fn get_for_test(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get(key).await
    }

    pub async fn put_for_test(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            .await
    }

    pub async fn delete_for_test(&self, key: &[u8]) -> anyhow::Result<()> {
        self.delete(Bytes::copy_from_slice(key)).await
    }
}
