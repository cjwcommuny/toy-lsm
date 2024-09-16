use crate::lsm::core::Lsm;
use crate::mvcc::iterator::LockedTxnIterWithTxn;
use crate::persistent::Persistent;
use derive_new::new;

#[derive(new)]
pub struct LsmIter<'a, P: Persistent, S> {
    lsm: &'a Lsm<P>,
    iter: LockedTxnIterWithTxn<'a, P, S>,
}
