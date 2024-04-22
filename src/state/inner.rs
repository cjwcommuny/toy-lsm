use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use derive_getters::Getters;
use typed_builder::TypedBuilder;

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::Persistent;
use crate::sst::Sstables;

#[derive(Getters, TypedBuilder, Clone)]
pub struct LsmStorageStateInner<P: Persistent> {
    memtable: Arc<MemTable>,
    imm_memtables: Vec<Arc<ImmutableMemTable>>,
    sstables_state: Arc<Sstables<P::Handle>>,
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