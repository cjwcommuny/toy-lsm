use crate::entry::Entry;

use crate::persistent::Persistent;
use crate::state::{LsmStorageStateInner, Map};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use futures::{stream, Stream};
use std::collections::{Bound, HashSet};

use crate::iterators::LockedLsmIter;
use crate::mvcc::iterator::{txn_local_iterator, LockedTxnIter};
use arc_swap::access::Access;
use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio_stream::StreamExt;

pub struct Transaction<P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageStateInner<P>>,

    // todo: need Arc<...> ?
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    /// todo: check deadlock?
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl<P: Persistent> Map for Transaction<P> {
    type Error = anyhow::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
        let guard = self.scan(Bound::Included(key), Bound::Included(key));
        let output = guard
            .iter()
            .await?
            .next()
            .await
            .transpose()?
            .map(|entry| entry.value);
        Ok(output)
    }

    async fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> Result<(), Self::Error> {
        self.local_storage.insert(key.into(), value.into());
        Ok(())
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> Result<(), Self::Error> {
        self.put(key, Bytes::new()).await
    }
}

impl<P: Persistent> Transaction<P> {
    pub fn new(
        read_ts: u64,
        inner: Arc<LsmStorageStateInner<P>>,
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

    // todo: no need for Result?
    pub fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> LockedTxnIter<'a, P> {
        let inner_iter = LockedLsmIter::new(self.inner.clone(), lower, upper, self.read_ts); // todo
        let guard = LockedTxnIter::new(&self.local_storage, inner_iter);
        guard
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
