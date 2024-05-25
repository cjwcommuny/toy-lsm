use crossbeam_skiplist::SkipMap;
use std::cmp::max;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use derive_getters::Getters;
use futures::stream;
use futures::stream::{StreamExt, TryStreamExt};
use typed_builder::TypedBuilder;

use crate::block::BlockCache;
use crate::manifest::{Manifest, ManifestRecord, NewMemtable};
use crate::memtable::{ImmutableMemTable, MemTable};
use crate::persistent::Persistent;
use crate::sst::sstables::fold_flush_manifest;
use crate::sst::{SsTable, SstOptions, Sstables};
use crate::wal::Wal;

#[derive(Getters, TypedBuilder)]
pub struct LsmStorageStateInner<P: Persistent> {
    pub(crate) memtable: Arc<MemTable<P::WalHandle>>,
    pub(crate) imm_memtables: Vec<Arc<ImmutableMemTable<P::WalHandle>>>,
    pub(crate) sstables_state: Arc<Sstables<P::SstHandle>>,
}

impl<P: Persistent> LsmStorageStateInner<P> {
    pub async fn recover(
        options: &SstOptions,
        manifest: &Manifest<P::ManifestHandle>,
        manifest_records: Vec<ManifestRecord>,
        persistent: &P,
        block_cache: Option<Arc<BlockCache>>,
    ) -> anyhow::Result<(Self, usize)> {
        let (imm_memtables, mut sstables_state) = stream::iter(manifest_records.into_iter().rev())
            .map(Ok::<_, anyhow::Error>)
            .try_fold(
                (Vec::new(), Sstables::new(options)),
                |(mut imm_memtables, mut sstables), manifest| async {
                    match manifest {
                        ManifestRecord::Flush(record) => {
                            fold_flush_manifest(&mut imm_memtables, &mut sstables, record)?;
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
            )
            .await?;
        // todo: split sst_ids & sst hashmap
        let sst_ids: Vec<_> = sstables_state.sst_ids().collect();

        let (_, max_sst_id) = stream::iter(sst_ids)
            .map(Ok::<_, anyhow::Error>)
            .try_fold(
                (sstables_state.sstables_mut(), 0),
                |(ssts, max_sst_id), sst_id| {
                    let block_cache = block_cache.clone();
                    async move {
                        let sst = SsTable::open(sst_id, block_cache, persistent).await?;
                        ssts.insert(sst_id, Arc::new(sst));
                        Ok((ssts, max(max_sst_id, sst_id)))
                    }
                },
            )
            .await?;

        let max_mem_table_id = imm_memtables
            .iter()
            .map(|table| table.id())
            .max()
            .unwrap_or(0);
        let next_sst_id = max(max_sst_id, max_mem_table_id);

        let memtable = MemTable::create_with_wal(next_sst_id + 1, persistent, manifest).await?;

        let this = Self {
            memtable: Arc::new(memtable),
            imm_memtables,
            sstables_state: Arc::new(sstables_state),
        };
        Ok((this, next_sst_id + 2))
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

async fn fold_new_imm_memtable<P: Persistent>(
    imm_memtables: &mut Vec<Arc<ImmutableMemTable<P::WalHandle>>>,
    persistent: &P,
    NewMemtable(id): NewMemtable,
) -> anyhow::Result<()> {
    let memtable = MemTable::recover_from_wal(id, persistent).await?;
    let imm_memtable = Arc::new(memtable).into_imm();
    imm_memtables.insert(0, imm_memtable);
    Ok(())
}
