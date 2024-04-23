use crate::persistent::Persistent;
use crate::sst::SstOptions;
use crate::state::{LsmStorageState, Map};
use crate::utils::func::do_nothing;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use futures_concurrency::stream::Merge;
use std::future::{ready, Future};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct Lsm<P: Persistent> {
    state: Arc<LsmStorageState<P>>,
}

impl<P: Persistent> Lsm<P> {
    pub fn new(options: SstOptions, persistent: P) -> Self {
        let state = Arc::new(LsmStorageState::new(options, persistent));
        Self { state }
    }

    fn spawn_flush(
        state: Arc<LsmStorageState<P>>,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()> {
        use Signal::*;
        tokio::spawn(async move {
            let trigger = IntervalStream::new(interval(Duration::from_millis(50))).map(|_| Trigger);
            let cancel_stream = cancel_token.cancelled().into_stream().map(|_| Cancel);
            let signals = (trigger, cancel_stream)
                .merge()
                .take_while(|signal| ready(matches!(signal, Trigger)));
            signals
                .for_each(|_| async {
                    let lock = state.state_lock().lock().await;
                    state
                        .force_flush_imm_memtable(&lock)
                        .await
                        .inspect_err(|e| error!(error = ?e))
                        .ok();
                })
                .await;
        })
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
        todo!()
    }
}

#[derive(Debug, Copy, Clone)]
enum Signal {
    Trigger,
    Cancel,
}
