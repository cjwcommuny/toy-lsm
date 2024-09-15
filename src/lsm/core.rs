use std::future::{ready, Future};
use std::ops::Bound;
use std::sync::Arc;

use std::time::Duration;

use bytes::Bytes;

use crate::persistent::Persistent;
use crate::sst::SstOptions;
use crate::state::{LsmStorageState, Map};
use futures::{FutureExt, StreamExt};
use futures_concurrency::stream::Merge;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;
use tracing::error;
use crate::lsm::iter::LsmIter;
use crate::state::write_batch::WriteBatchRecord;

pub struct Lsm<P: Persistent> {
    state: Arc<LsmStorageState<P>>,
    cancel_token: CancellationToken,
    flush_handle: Option<JoinHandle<()>>,
    spawn_handle: Option<JoinHandle<()>>,
}

impl<P: Persistent> Lsm<P> {
    pub async fn new(options: SstOptions, persistent: P) -> anyhow::Result<Self> {
        let state = Arc::new(LsmStorageState::new(options, persistent).await?);
        let cancel_token = CancellationToken::new();
        let flush_handle = Self::spawn_flush(state.clone(), cancel_token.clone());
        let spawn_handle = Self::spawn_compaction(state.clone(), cancel_token.clone());
        let this = Self {
            state,
            cancel_token,
            flush_handle: Some(flush_handle),
            spawn_handle: Some(spawn_handle),
        };
        Ok(this)
    }

    fn spawn_flush(
        state: Arc<LsmStorageState<P>>,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()> {
        use Signal::*;
        tokio::spawn(async move {
            let trigger = IntervalStream::new(interval(Duration::from_millis(10))).map(|_| Trigger);
            let cancel_stream = cancel_token.cancelled().into_stream().map(|_| Cancel);
            (trigger, cancel_stream)
                .merge()
                .take_while(|signal| ready(matches!(signal, Trigger)))
                .for_each(|_| async {
                    state
                        .may_flush_imm_memtable()
                        .await
                        .inspect_err(|e| error!(error = ?e))
                        .ok();
                })
                .await;
        })
    }

    fn spawn_compaction(
        state: Arc<LsmStorageState<P>>,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()> {
        use Signal::*;
        tokio::spawn(async move {
            let trigger = IntervalStream::new(interval(Duration::from_millis(13))).map(|_| Trigger);
            let cancel_stream = cancel_token.cancelled().into_stream().map(|_| Cancel);
            (trigger, cancel_stream)
                .merge()
                .take_while(|signal| ready(matches!(signal, Trigger)))
                .for_each(|_| async {
                    let lock = state.state_lock().lock().await;
                    state
                        .force_compact(&lock)
                        .await
                        .inspect_err(|e| error!(error = ?e))
                        .ok();
                })
                .await;
        })
    }

    pub async fn put_batch(&self, batch: &[WriteBatchRecord]) -> anyhow::Result<()> {
        self.state.put_batch(batch).await
    }

    pub fn scan<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> LsmIter<'a, P> {
        let iter = self.state.scan(lower, upper);
        LsmIter::new(self, iter)
    }
}

impl<P> Map for Lsm<P>
where
    P: Persistent,
{
    type Error = anyhow::Error;

    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send {
        self.state.get(key)
    }

    fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.state.put(key, value)
    }

    fn delete(
        &self,
        key: impl Into<Bytes> + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.state.delete(key)
    }
}

impl<P: Persistent> Drop for Lsm<P> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        let flush_handle = self.flush_handle.take();
        let spawn_handle = self.spawn_handle.take();

        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                if let Some(flush_handle) = flush_handle {
                    flush_handle.await.inspect_err(|e| error!(error = ?e)).ok();
                }
                if let Some(spawn_handle) = spawn_handle {
                    spawn_handle.await.inspect_err(|e| error!(error = ?e)).ok();
                }
            })
        });
    }
}

#[cfg(test)]
impl<P: Persistent> Lsm<P> {
    async fn put_for_test(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            .await
    }

    async fn delete_for_test(&self, key: &[u8]) -> anyhow::Result<()> {
        self.delete(Bytes::copy_from_slice(key)).await
    }
}

#[derive(Debug, Copy, Clone)]
enum Signal {
    Trigger,
    Cancel,
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::lsm::core::Lsm;
    use crate::persistent::{LocalFs, Persistent};
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    use crate::sst::SstOptions;
    use crate::state::Map;
    use crate::test_utils::insert_sst;
    use std::time::Duration;
    use tempfile::{tempdir, TempDir};
    use tokio::time::sleep;
    use tracing::Instrument;

    // todo: WAL causes the "too many open files" error
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_task2_auto_flush() {
        let dir = tempdir().unwrap();
        let storage = build_lsm(&dir).await.unwrap();

        let value = "1".repeat(1024); // 1KB

        // approximately 6MB
        for i in 0..6000 {
            let key = format!("{i}");
            let value = value.as_bytes();
            storage
                .put_for_test(key.as_bytes(), value)
                .instrument(tracing::info_span!("put_for_test"))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(500)).await;
        assert!(!storage
            .state
            .inner()
            .load()
            .sstables_state()
            .l0_sstables()
            .is_empty());
    }

    #[allow(dead_code)]
    async fn build_lsm(dir: &TempDir) -> anyhow::Result<Lsm<impl Persistent>> {
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let options = SstOptions::builder()
            .target_sst_size(12288)
            .block_size(4096)
            .num_memtable_limit(100)
            .compaction_option(Default::default())
            .enable_wal(false)
            .enable_mvcc(true)
            .build();
        Lsm::new(options, persistent).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_auto_compaction() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(4)
            .max_bytes_for_level_base(2048)
            .level_size_multiplier_2_exponent(1)
            .build();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(4096)
            .num_memtable_limit(1000)
            .compaction_option(CompactionOptions::Leveled(compaction_options))
            .enable_wal(false)
            .enable_mvcc(true)
            .build();
        let lsm = Lsm::new(options, persistent).await.unwrap();
        for i in 0..10 {
            let begin = i * 100;
            insert_sst(&lsm, begin..begin + 100).await.unwrap();
        }
        sleep(Duration::from_secs(2)).await;

        for i in 0..10 {
            let begin = i * 100;
            let range = begin..begin + 100;
            for i in range {
                let key = format!("key-{:04}", i);
                let expected_value = format!("value-{:04}", i);
                let value = lsm.get(key.as_bytes()).await.unwrap().unwrap();
                assert_eq!(expected_value.as_bytes(), value.as_bytes());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_wal_integration() {
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(3)
            .max_bytes_for_level_base(1024)
            .level_size_multiplier_2_exponent(1)
            .build();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(256)
            .num_memtable_limit(10)
            .compaction_option(CompactionOptions::Leveled(compaction_options))
            .enable_wal(true)
            .enable_mvcc(true)
            .build();
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let lsm = Lsm::new(options.clone(), persistent).await.unwrap();
        add_data(&lsm).await.unwrap();
        sleep(Duration::from_secs(2)).await;

        // ensure some SSTs are not flushed
        let inner = lsm.state.inner.load();

        assert!(!inner.memtable.is_empty() || !inner.imm_memtables.is_empty());
        drop(lsm);

        {
            let persistent = LocalFs::new(dir.path().to_path_buf());
            let lsm = Lsm::new(options, persistent).await.unwrap();
            assert_eq!(
                std::str::from_utf8(&lsm.get(b"key-0").await.unwrap().unwrap()[..]).unwrap(),
                "value-1024",
            );
            assert_eq!(
                std::str::from_utf8(&lsm.get(b"key-1").await.unwrap().unwrap()[..]).unwrap(),
                "value-1024",
            );
            assert_eq!(lsm.get(b"key-2").await.unwrap(), None);
        }
    }

    async fn add_data<P: Persistent>(lsm: &Lsm<P>) -> anyhow::Result<()> {
        for i in 0..=1024 {
            lsm.put_for_test(b"key-0", format!("value-{}", i).as_bytes())
                .await?;
            if i % 2 == 0 {
                lsm.put_for_test(b"key-1", format!("value-{}", i).as_bytes())
                    .await?;
            } else {
                lsm.delete_for_test(b"key-1").await?;
            }
            if i % 2 == 1 {
                lsm.put_for_test(b"key-2", format!("value-{}", i).as_bytes())
                    .await?;
            } else {
                lsm.delete_for_test(b"key-2").await?;
            }
        }
        Ok(())
    }
}
