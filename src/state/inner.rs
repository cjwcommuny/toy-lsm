use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use derive_getters::Getters;
use typed_builder::TypedBuilder;

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::Persistent;
use crate::sst::Sstables;

#[derive(Getters, TypedBuilder)]
pub struct LsmStorageStateInner<P: Persistent> {
    memtable: Arc<MemTable>,
    imm_memtables: Vec<Arc<ImmutableMemTable>>,
    pub(crate) sstables_state: Arc<Sstables<P::Handle>>,
}

impl<P: Persistent> Clone for LsmStorageStateInner<P> {
    fn clone(&self) -> Self {
        Self {
            memtable: self.memtable.clone(),
            imm_memtables: self.imm_memtables.clone(),
            sstables_state: self.sstables_state.clone(),
        }
    }
}

impl<P: Persistent> Debug for LsmStorageStateInner<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmStorageStateInner")
            .field("memtable", &self.memtable)
            .field("imm_memtables", &self.imm_memtables)
            .field("sstables_state", &self.sstables_state)
            .finish()
    }
}
