use std::collections::{Bound, HashSet};
use std::ops::Bound::Excluded;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use tokio_stream::StreamExt;

use crate::entry::Entry;
use crate::iterators::LockedLsmIter;
use crate::mvcc::core::LsmMvccInner;
use crate::mvcc::iterator::LockedTxnIter;
use crate::persistent::Persistent;
use crate::state::{LsmStorageStateInner, Map};

#[derive(Debug, Default)]
pub struct RWSet {
    read_set: HashSet<u32>,
    write_set: HashSet<u32>,
}

pub struct Transaction<P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageStateInner<P>>,

    // todo: need Arc<...> ?
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,

    // todo: delete it?
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    /// todo: check deadlock?
    pub(crate) key_hashes: Option<Mutex<RWSet>>,

    mvcc: Arc<LsmMvccInner>,
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
        key_hashes: Option<Mutex<RWSet>>,
        mvcc: Arc<LsmMvccInner>,
    ) -> Self {
        {
            let mut guard = mvcc.ts.lock();
            guard.1.add_reader(read_ts);
        }
        Self {
            read_ts,
            inner,
            local_storage: Arc::default(),
            committed: Arc::default(),
            key_hashes,
            mvcc,
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
        // todo: commit lock / write lock ?
        let _commit_guard = self.mvcc.commit_lock.lock();
        let expected_commit_ts = {
            // todo: 这里的锁可以去掉？
            let mut guard = self.mvcc.ts.lock();
            guard.0 + 1
        };
        let conflict = if let Some(key_hashes) = self.key_hashes.as_ref() {
            let guard = self.mvcc.committed_txns.lock();
            let range = (Excluded(self.read_ts), Excluded(expected_commit_ts));
            let rw_set_guard = key_hashes.lock();
            let read_set = &rw_set_guard.read_set;
            guard
                .range(range)
                .any(|(_, data)| data.key_hashes.is_disjoint(read_set))
        } else {
            false
        };

        // let entries: Vec<_> = self
        //     .local_storage
        //     .iter()
        //     .map(|e| Entry::new(e.key().clone(), e.value().clone()))
        //     .collect();
        // self.inner.memtable.put_batch(&entries, commit_ts).await?;
        Ok(())
    }
}

impl<P: Persistent> Drop for Transaction<P> {
    fn drop(&mut self) {
        let mut guard = self.mvcc.ts.lock();
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
