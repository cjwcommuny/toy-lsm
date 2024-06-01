use crate::entry::Entry;
use crate::iterators::LockedLsmIter;
use crate::persistent::Persistent;
use crate::state::{LsmStorageState, LsmStorageStateInner, Map};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use futures::{stream, Stream};
use std::collections::{Bound, HashSet};
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Transaction<P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageStateInner<P>>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl<P: Persistent> Map for Transaction<P> {
    type Error = anyhow::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
        todo!()
    }

    async fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<P: Persistent> Drop for Transaction<P> {
    fn drop(&mut self) {
        // commit
        todo!()
    }
}

impl<P: Persistent> Transaction<P> {
    pub fn scan<'a>(
        &self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Entry>>> {
        todo!();
        Ok(stream::empty())
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
