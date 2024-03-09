use crate::entry::Entry;
use crate::iterators::no_deleted::new_no_deleted_iter;
use crate::iterators::{
    create_merge_iter_from_non_empty_iters, create_two_merge_iter, iter_fut_to_stream,
    MergeIterator, NoDeletedIterator, TwoMergeIterator,
};
use crate::memtable::{ImmutableMemTable, MemTable, MemTableIterator};
use crate::persistent::PersistentHandle;
use crate::sst::iterator::MergedSstIterator;
use crate::sst::Sstables;
use derive_new::new;
use futures::StreamExt;
use std::collections::Bound;
use std::future::ready;
use std::iter;
use std::ops::Deref;
use tokio::sync::RwLockReadGuard;

type LsmIterator<'a, File> = NoDeletedIterator<LsmIteratorInner<'a, File>, anyhow::Error>;

type LsmIteratorInner<'a, File> = TwoMergeIterator<
    Entry,
    MergeIterator<Entry, MemTableIterator<'a>>,
    MergedSstIterator<'a, File>,
>;

#[derive(new)]
pub struct LockedLsmIter<'lock, 'bound, File> {
    memtable: RwLockReadGuard<'lock, MemTable>,
    imm_memtables: RwLockReadGuard<'lock, Vec<ImmutableMemTable>>,
    sstables: RwLockReadGuard<'lock, Sstables<File>>,
    lower: Bound<&'bound [u8]>,
    upper: Bound<&'bound [u8]>,
}

impl<'lock, 'bound, File> LockedLsmIter<'lock, 'bound, File>
where
    File: PersistentHandle,
{
    pub async fn iter(&self) -> anyhow::Result<LsmIterator<File>> {
        let a = {
            let memtable = self.memtable.deref();
            let imm_memtables = self.imm_memtables.deref();
            let iters = iter::once(memtable.as_immutable_ref())
                .chain(imm_memtables)
                .map(|table| table.scan(self.lower, self.upper));
            let iters = iter_fut_to_stream(iters)
                .map(Result::ok)
                .map(Option::flatten)
                .filter_map(ready);
            create_merge_iter_from_non_empty_iters(iters).await
        };

        let b = self.sstables.scan_sst(self.lower, self.upper).await?;
        let merge = create_two_merge_iter(a, b).await?;
        let iter = new_no_deleted_iter(merge);
        Ok(iter)
    }
}
