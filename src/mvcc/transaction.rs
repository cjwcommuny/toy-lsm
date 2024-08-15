use anyhow::anyhow;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::collections::{Bound, HashSet};
use std::ops::Bound::Excluded;
use std::sync::Arc;
use tokio_stream::StreamExt;

use crate::entry::Entry;
use crate::iterators::LockedLsmIter;
use crate::mvcc::core::{CommittedTxnData, LsmMvccInner};
use crate::mvcc::iterator::LockedTxnIter;
use crate::persistent::Persistent;
use crate::state::{LsmStorageState, Map};
use crate::utils::scoped::ScopedMutex;

#[derive(Debug, Default)]
pub struct RWSet {
    read_set: HashSet<u32>,
    write_set: HashSet<u32>,
}

impl RWSet {
    pub fn add_read_key(&mut self, key: &[u8]) {
        self.read_set.insert(farmhash::hash32(key));
    }

    pub fn add_write_key(&mut self, key: &[u8]) {
        self.write_set.insert(farmhash::hash32(key));
    }
}

pub struct Transaction<'a, P: Persistent> {
    pub(crate) read_ts: u64,
    pub(crate) state: &'a LsmStorageState<P>,

    // todo: need Arc<...> ?
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,

    /// Write set and read set
    /// todo: check deadlock?
    pub(crate) key_hashes: Option<ScopedMutex<RWSet>>,

    mvcc: Arc<LsmMvccInner>,
}

impl<'a, P: Persistent> Map for Transaction<'a, P> {
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
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock_with(|mut set| set.add_read_key(key));
        }
        Ok(output)
    }

    async fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> Result<(), Self::Error> {
        let key = key.into();
        self.local_storage.insert(key.clone(), value.into());
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock_with(|mut set| set.add_write_key(key.as_ref()));
        }
        Ok(())
    }

    async fn delete(&self, key: impl Into<Bytes> + Send) -> Result<(), Self::Error> {
        let key = key.into();
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock_with(|mut set| set.add_write_key(key.as_ref()));
        }
        self.put(key, Bytes::new()).await
    }
}

impl<'a, P: Persistent> Transaction<'a, P> {
    pub fn new(
        read_ts: u64,
        state: &'a LsmStorageState<P>,
        serializable: bool,
        mvcc: Arc<LsmMvccInner>,
    ) -> Self {
        {
            let mut guard = mvcc.ts.lock();
            guard.1.add_reader(read_ts);
        }
        Self {
            read_ts,
            state,
            local_storage: Arc::default(),
            key_hashes: serializable.then(ScopedMutex::default),
            mvcc,
        }
    }

    // todo: no need for Result?
    pub fn scan(&'a self, lower: Bound<&'a [u8]>, upper: Bound<&'a [u8]>) -> LockedTxnIter<'a, P> {
        let inner = self.state.inner.load_full();
        let inner_iter = LockedLsmIter::new(inner, lower, upper, self.read_ts);
        let guard = LockedTxnIter::new(&self.local_storage, inner_iter, self.key_hashes.as_ref());
        guard
    }

    // todo: 区分 snapshot isolation vs serializable isolation
    pub async fn commit(mut self) -> anyhow::Result<()> {
        let mut commit_guard = self.mvcc.committed_txns.lock().await;

        let expected_commit_ts = {
            // todo: 这里的锁可以去掉？
            let guard = self.mvcc.ts.lock();
            guard.0 + 1
        };
        let key_hashes = self.key_hashes.take().map(ScopedMutex::into_inner);
        let conflict = if let Some(key_hashes) = key_hashes.as_ref() {
            let range = (Excluded(self.read_ts), Excluded(expected_commit_ts));
            let read_set = &key_hashes.read_set;
            println!("====");
            dbg!(key_hashes);
            dbg!(&commit_guard);
            println!("====");
            commit_guard
                .range(range)
                .any(|(_, data)| !data.key_hashes.is_disjoint(read_set))
        } else {
            false
        };
        if conflict {
            return Err(anyhow!("commit conflict"));
        }

        // todo: avoid collecting
        let entries: Vec<_> = self
            .local_storage
            .iter()
            .map(|e| Entry::new(e.key().clone(), e.value().clone()))
            .collect();
        // todo: 如果 write_batch 失败怎么保证 atomicity
        self.state.write_batch(&entries, expected_commit_ts).await?;
        self.mvcc.update_commit_ts(expected_commit_ts);

        if let Some(key_hashes) = key_hashes {
            let committed_data = CommittedTxnData {
                key_hashes: key_hashes.write_set,
                read_ts: self.read_ts,
                commit_ts: expected_commit_ts,
            };
            commit_guard.insert(expected_commit_ts, committed_data);
        }

        Ok(())
    }
}

impl<'a, P: Persistent> Drop for Transaction<'a, P> {
    fn drop(&mut self) {
        let mut guard = self.mvcc.ts.lock();
        guard.1.remove_reader(self.read_ts);
    }
}

#[cfg(test)]
impl<'a, P: Persistent> Transaction<'a, P> {
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
