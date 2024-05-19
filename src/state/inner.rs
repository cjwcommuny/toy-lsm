use std::cmp::max;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use derive_getters::Getters;
use typed_builder::TypedBuilder;

use crate::block::BlockCache;
use crate::manifest::ManifestRecord;
use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::Persistent;
use crate::sst::{SsTable, SstOptions, Sstables};

#[derive(Getters, TypedBuilder)]
pub struct LsmStorageStateInner<P: Persistent> {
    pub(crate) memtable: Arc<MemTable<P::WalHandle>>,
    pub(crate) imm_memtables: Vec<Arc<ImmutableMemTable<P::WalHandle>>>,
    pub(crate) sstables_state: Arc<Sstables<P::SstHandle>>,
}

impl<P: Persistent> LsmStorageStateInner<P> {
    pub async fn recover(
        options: &SstOptions,
        manifest: Vec<ManifestRecord>,
        persistent: &P,
        block_cache: Option<Arc<BlockCache>>,
    ) -> anyhow::Result<(Self, usize)> {
        let mut sstables_state =
            manifest
                .into_iter()
                .fold(
                    Sstables::new(options),
                    |mut sstables, manifest| match manifest {
                        ManifestRecord::Flush(record) => {
                            sstables.fold_flush_manifest(record);
                            sstables
                        }
                        ManifestRecord::NewMemtable(_) => unreachable!(),
                        ManifestRecord::Compaction(record) => {
                            sstables.fold_compaction_manifest(record);
                            sstables
                        }
                    },
                );
        // todo: split sst_ids & sst hashmap
        let sst_ids: Vec<_> = sstables_state.sst_ids().collect();
        let ssts = sstables_state.sstables_mut();
        let mut max_sst_id = 0;

        for sst_id in sst_ids {
            let block_cache = block_cache.clone();
            let sst = SsTable::open(sst_id, block_cache, persistent).await?;
            ssts.insert(sst_id, Arc::new(sst));
            max_sst_id = max(max_sst_id, sst_id);
        }
        let this = Self {
            memtable: Arc::new(MemTable::create(max_sst_id + 1)),
            imm_memtables: Vec::new(),
            sstables_state: Arc::new(sstables_state),
        };
        Ok((this, max_sst_id + 2))
    }
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
