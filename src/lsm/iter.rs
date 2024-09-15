use derive_new::new;
use crate::lsm::core::Lsm;
use crate::mvcc::iterator::LockedTxnIterWithTxn;
use crate::persistent::Persistent;

#[derive(new)]
pub struct LsmIter<'a, P: Persistent> {
    lsm: &'a Lsm<P>,
    iter: LockedTxnIterWithTxn<'a, P>,
}
