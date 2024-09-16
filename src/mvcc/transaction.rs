use anyhow::anyhow;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::collections::{Bound, HashSet};
use std::ops::Bound::Excluded;
use std::slice;
use std::sync::Arc;
use tokio_stream::StreamExt;

use crate::entry::Entry;
use crate::iterators::lsm::LsmIterator;
use crate::mvcc::core::{CommittedTxnData, LsmMvccInner};
use crate::mvcc::iterator::TxnLsmIter;
use crate::persistent::Persistent;
use crate::state::write_batch::WriteBatchRecord;
use crate::state::{LsmStorageState, Map};
use crate::utils::scoped::ScopedMutex;
use crate::utils::send::assert_send;

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

// todo: no need for async
impl<'a, P: Persistent> Map for Transaction<'a, P> {
    type Error = anyhow::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
        let output = assert_send(self.scan(Bound::Included(key), Bound::Included(key)))
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
        let value = value.into();
        let record = WriteBatchRecord::Put(key, value);
        self.write_batch(slice::from_ref(&record));
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

    pub fn write_batch(&self, batch: &[WriteBatchRecord]) {
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            key_hashes.lock_with(|mut set| {
                for record in batch {
                    set.add_write_key(record.get_key().as_ref());
                }
            });
        }

        for record in batch {
            let pair = record.clone().into_keyed();
            self.local_storage.insert(pair.key, pair.value);
        }
    }

    // todo: no need for Result?
    pub async fn scan(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> anyhow::Result<LsmIterator<'a>> {
        let iter = TxnLsmIter::try_build(self, lower, upper).await?;
        let iter = Box::new(iter) as _;
        Ok(iter)
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
