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
        dbg!(&manifest_records);
        let (imm_memtables, mut sstables_state) =
            build_state(options, manifest_records, persistent).await?;
        // todo: split sst_ids & sst hashmap
        let sst_ids: Vec<_> = sstables_state.sst_ids().collect();
        dbg!(&sstables_state);
        dbg!(&imm_memtables);

        let (_, max_sst_id) = stream::iter(sst_ids)
            .map(Ok::<_, anyhow::Error>)
            .try_fold(
                (sstables_state.sstables_mut(), None),
                |(ssts, max_sst_id), sst_id| {
                    let block_cache = block_cache.clone();
                    async move {
                        let sst = SsTable::open(sst_id, block_cache, persistent).await?;
                        ssts.insert(sst_id, Arc::new(sst));

                        let next_max_sst_id = match max_sst_id {
                            Some(prev) => max(prev, sst_id),
                            None => sst_id,
                        };

                        Ok((ssts, Some(next_max_sst_id)))
                    }
                },
            )
            .await?;

        let max_mem_table_id = imm_memtables.iter().map(|table| table.id()).max();

        let next_sst_id = max(
            max_sst_id.map(|id| id + 1).unwrap_or(0),
            max_mem_table_id.map(|id| id + 1).unwrap_or(0),
        );

        let memtable = MemTable::create_with_wal(next_sst_id, persistent, manifest).await?;

        let this = Self {
            memtable: Arc::new(memtable),
            imm_memtables,
            sstables_state: Arc::new(sstables_state),
        };
        Ok((this, next_sst_id + 1))
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
    let imm_memtable = Arc::new(memtable).into_imm().await?;
    imm_memtables.insert(0, imm_memtable);
    Ok(())
}

async fn build_state<P: Persistent>(
    options: &SstOptions,
    manifest_records: Vec<ManifestRecord>,
    persistent: &P,
) -> anyhow::Result<(
    Vec<Arc<ImmutableMemTable<P::WalHandle>>>,
    Sstables<P::SstHandle>,
)> {
    stream::iter(manifest_records.into_iter())
        .map(Ok::<_, anyhow::Error>)
        .try_fold(
            (Vec::new(), Sstables::new(options)),
            |(mut imm_memtables, mut sstables), manifest| async {
                match manifest {
                    ManifestRecord::Flush(record) => {
                        let flush = &record;
                        // dbg!(flush);
                        fold_flush_manifest(&mut imm_memtables, &mut sstables, record)?;
                        Ok((imm_memtables, sstables))
                    }
                    ManifestRecord::NewMemtable(record) => {
                        let new_mem = &record;
                        // dbg!(new_mem);
                        fold_new_imm_memtable(&mut imm_memtables, persistent, record).await?;
                        Ok((imm_memtables, sstables))
                    }
                    ManifestRecord::Compaction(record) => {
                        // dbg!(&imm_memtables);
                        // dbg!(&sstables);
                        let compact = &record;
                        // dbg!(compact);
                        sstables.fold_compaction_manifest(record);
                        Ok((imm_memtables, sstables))
                    }
                }
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::manifest::{Flush, NewMemtable};
    use crate::persistent::LocalFs;
    use crate::sst::compact::{CompactionOptions, LeveledCompactionOptions};
    use crate::sst::SstOptions;
    use crate::state::inner::build_state;

    #[tokio::test]
    async fn test_recover_state() {
        let compaction_options = LeveledCompactionOptions::builder()
            .max_levels(3)
            .max_bytes_for_level_base(1024)
            .level_size_multiplier_2_exponent(1)
            .build();
        let options = SstOptions::builder()
            .target_sst_size(1024)
            .block_size(256)
            .num_memtable_limit(10)
            .compaction_option(CompactionOptions::Leveled(compaction_options))
            .enable_wal(true)
            .build();

        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let manifest_records = vec![
            NewMemtable(0).into(),
            NewMemtable(1).into(),
            Flush(0).into(),
        ];

        let (imm, ssts) = build_state(&options, manifest_records, &persistent).await.unwrap();
        dbg!(imm);
        dbg!(ssts);
    }
}