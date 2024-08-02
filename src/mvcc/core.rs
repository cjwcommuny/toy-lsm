use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::mvcc::transaction::Transaction;
use crate::mvcc::watermark::Watermark;
use crate::persistent::Persistent;
use crate::state::LsmStorageStateInner;
use crate::time::TimeProvider;

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

pub type TimeProviderWrapper = Box<dyn TimeProvider>;

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
    time_provider: TimeProviderWrapper,
}

impl LsmMvccInner {
    pub fn new(time_provider: TimeProviderWrapper) -> Self {
        let initial_ts = time_provider.now();
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
            time_provider,
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn new_txn<P: Persistent>(
        &self,
        inner: Arc<LsmStorageStateInner<P>>,
        serializable: bool,
    ) -> Transaction<P> {
        // todo: use external dependency to get time
        let ts = self.time_provider.now();
        let key_hashes = serializable.then(Mutex::default);

        Transaction::new(ts, inner, key_hashes)
    }
}
