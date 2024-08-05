use std::collections::{Bound, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use tokio_stream::StreamExt;

use crate::iterators::LockedLsmIter;
use crate::mvcc::iterator::LockedTxnIter;
use crate::mvcc::watermark::Watermark;
use crate::persistent::Persistent;
use crate::state::{LsmStorageStateInner, Map};

pub struct Transaction<P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageStateInner<P>>,

    // todo: need Arc<...> ?
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,

    // todo: delete it?
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    /// todo: check deadlock?
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,

    // todo: remove u64?
    // todo: remove mutex?
    // todo: 理论上存储一个 callback: fn(read_ts: u64) 即可
    watermark: Arc<Mutex<(u64, Watermark)>>,
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
        watermark: Arc<Mutex<(u64, Watermark)>>,
    ) -> Self {
        {
            let mut guard = watermark.lock();
            guard.1.add_reader(read_ts);
        }
        Self {
            read_ts,
            inner,
            local_storage: Arc::default(),
            committed: Arc::default(),
            key_hashes,
            watermark,
        }
    }

    // todo: no need for Result?
    pub fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> LockedTxnIter<'a, P> {
        let inner_iter = LockedLsmIter::new(self.inner.clone(), lower, upper, self.read_ts);
        let guard = LockedTxnIter::new(&self.local_storage, inner_iter);
        guard
    }

    pub async fn commit(self) -> anyhow::Result<()> {
        // todo: use write batch
        for entry in self.local_storage.iter() {
            // self.inner
            // todo
        }
        todo!()
    }
}

impl<P: Persistent> Drop for Transaction<P> {
    fn drop(&mut self) {
        let mut guard = self.watermark.lock();
        guard.1.remove_reader(self.read_ts);
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
