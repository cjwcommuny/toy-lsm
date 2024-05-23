use std::cmp::max;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use derive_getters::Getters;
use futures::stream;
use futures::stream::TryStreamExt;
use typed_builder::TypedBuilder;

use crate::block::BlockCache;
use crate::manifest::{ManifestRecord, NewMemtable};
use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::Persistent;
use crate::sst::{SsTable, Sstables, SstOptions};

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
            stream::iter(manifest)
                .try_fold(
                    (Vec::new(), Sstables::new(options)),
                    |(mut imm_memtables, mut sstables), manifest| async {
                        match manifest {
                            ManifestRecord::Flush(record) => {
                                sstables.fold_flush_manifest(record);
                                Ok((imm_memtables, sstables))
                            }
                            ManifestRecord::NewMemtable(record) => {
                                fold_new_imm_memtable(&mut imm_memtables, persistent, record).await?;
                                Ok((imm_memtables, sstables))
                            }
                            ManifestRecord::Compaction(record) => {
                                sstables.fold_compaction_manifest(record);
                                Ok((imm_memtables, sstables))
                            }
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

async fn fold_new_imm_memtable<P: Persistent>(imm_memtables: &mut Vec<Arc<ImmutableMemTable<P::WalHandle>>>, persistent: &P, record: NewMemtable) -> anyhow::Result<()> {
    let id = record.0;
    let memtable = MemTable::recover_from_wal(id, persistent).await?;
    let imm_memtable = Arc::new(memtable).into_imm();
    imm_memtables.insert(0, imm_memtable);
    Ok(())
}