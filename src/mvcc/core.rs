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
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
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
        self: &Arc<Self>,
        inner: Arc<LsmStorageStateInner<P>>,
        serializable: bool,
    ) -> Transaction<P> {
        let key_hashes = serializable.then(Mutex::default);
        let ts = {
            let guard = self.ts.lock();
            guard.0
        };
        Transaction::new(ts, inner, key_hashes, self.clone())
    }
}
