use crate::entry::Entry;
use crate::iterators::lsm::LsmIterator;
use crate::lsm::core::Lsm;
use crate::persistent::LocalFs;
use crate::state::write_batch::WriteBatchRecord;
use crate::state::Map;
use crate::test_utils::integration::common::Database;
use anyhow::anyhow;
use bytes::Bytes;
use futures::StreamExt;
use std::ops::Bound::{Included, Unbounded};
use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::sst::SstOptions;

pub struct MyDbWithRuntime {
    db: Option<Lsm<LocalFs>>,
    handle: Arc<Runtime>,
}

pub fn build_sst_options() -> SstOptions {
    SstOptions::builder()
        .target_sst_size(1024 * 1024 * 2)
        .block_size(4096)
        .num_memtable_limit(1000)
        .compaction_option(Default::default())
        .enable_wal(false)
        .enable_mvcc(true)
        .build()
}

impl MyDbWithRuntime {
    pub fn new(db: Lsm<LocalFs>, runtime: Arc<Runtime>) -> Self {
        Self {
            db: Some(db),
            handle: runtime,
        }
    }
}

impl Drop for MyDbWithRuntime {
    fn drop(&mut self) {
        if let Some(db) = self.db.take() {
            self.handle.block_on(async { drop(db) })
        }
    }
}

impl Database for MyDbWithRuntime {
    type Error = anyhow::Error;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error> {
        let Some(db) = self.db.as_ref() else {
            return Err(anyhow!("no db"));
        };
        self.handle.block_on(async {
            let batch: Vec<_> = kvs
                .into_iter()
                .map(|(key, value)| WriteBatchRecord::Put(key, value))
                .collect();
            db.put_batch(&batch).await?;
            Ok(())
        })
    }

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(db) = self.db.as_ref() else {
            return Err(anyhow!("no db"));
        };
        let key = key.as_ref();
        let value = self.handle.block_on(async { db.get(key).await })?;
        Ok(value.map(Into::into))
    }

    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error> {
        let Some(db) = self.db.as_ref() else {
            return Err(anyhow!("no db"));
        };

        let iter = self
            .handle
            .block_on(async move { db.scan(Included(begin), Unbounded).await })?;
        let iter = LsmIterWithRuntime {
            iter,
            handle: self.handle.clone(),
        };
        Ok(iter)
    }
}

pub struct LsmIterWithRuntime<'a> {
    iter: LsmIterator<'a>,
    handle: Arc<Runtime>,
}

impl<'a> Iterator for LsmIterWithRuntime<'a> {
    type Item = anyhow::Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.handle.block_on(async {
            self.iter
                .next()
                .await
                .map(|entry| entry.map(Entry::into_tuple))
        })
    }
}
