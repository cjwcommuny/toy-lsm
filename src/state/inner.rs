use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use derive_getters::Getters;
use nom::combinator::opt;
use typed_builder::TypedBuilder;
use crate::manifest::ManifestRecord;

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::memory::Memory;
use crate::persistent::SstPersistent;
use crate::sst::{Sstables, SstOptions};

#[derive(Getters, TypedBuilder)]
pub struct LsmStorageStateInner<P: SstPersistent> {
    memtable: Arc<MemTable>,
    imm_memtables: Vec<Arc<ImmutableMemTable>>,
    pub(crate) sstables_state: Arc<Sstables<P::Handle>>,
}

impl<P: SstPersistent> LsmStorageStateInner<P> {
    pub async fn recover(options: &SstOptions, manifest: Vec<ManifestRecord>) -> anyhow::Result<Self> {
        let imm_memtable = Vec::new();
        let sstables_state = manifest.into_iter().fold(Sstables::new(options), |mut sstables, manifest| match manifest {
            ManifestRecord::Flush(record) => {
                sstables.fold_flush_manifest(record);
                sstables
            }
            ManifestRecord::NewMemtable(_) => unreachable!(),
            ManifestRecord::Compaction(record) => {
                sstables.fold_compaction_manifest(record);
                sstables
            }
        });
        // let memtable = MemTable::create()
        todo!()
    }
}

impl<P: SstPersistent> Clone for LsmStorageStateInner<P> {
    fn clone(&self) -> Self {
        Self {
            memtable: self.memtable.clone(),
            imm_memtables: self.imm_memtables.clone(),
            sstables_state: self.sstables_state.clone(),
        }
    }
}

impl<P: SstPersistent> Debug for LsmStorageStateInner<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmStorageStateInner")
            .field("memtable", &self.memtable)
            .field("imm_memtables", &self.imm_memtables)
            .field("sstables_state", &self.sstables_state)
            .finish()
    }
}
