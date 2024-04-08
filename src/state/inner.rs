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
