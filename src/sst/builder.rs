use std::mem;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use nom::AsBytes;

use crate::block::{BlockBuilder, BlockCache};
use crate::key::KeySlice;
use crate::memtable::ImmutableMemTable;
use crate::persistent::interface::WalHandle;
use crate::persistent::Persistent;
use crate::sst::bloom::Bloom;
use crate::sst::{BlockMeta, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64, // todo: use Option
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    /// 应该改成传入 owned bytes
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let success = self.builder.add(key, value);
        if success {
            self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
            return;
        }
        self.max_ts = self.max_ts.max(key.timestamp());
        let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        Self::add_block(&mut self.data, &mut self.meta, old_builder);
        self.add(key, value);
    }

    fn add_block(data: &mut Vec<u8>, metas: &mut Vec<BlockMeta>, old_builder: BlockBuilder) {
        let block = old_builder.build();
        let first_key = block.first_key();
        let last_key = block.last_key();
        let meta = BlockMeta {
            offset: data.len(),
            first_key: first_key.clone(),
            last_key: last_key.clone(),
        };
        let block_data = block.encode();

        data.extend(block_data);
        metas.push(meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    /// todo: 是不是异步化能加速？
    pub async fn build<P>(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        persistent: &P,
    ) -> Result<SsTable<P::SstHandle>>
    where
        P: Persistent,
    {
        let Self {
            builder,
            mut data,
            mut meta,
            block_size: _,
            key_hashes,
            max_ts,
        } = self;

        // add last block
        if !builder.is_empty() {
            Self::add_block(&mut data, &mut meta, builder);
        }

        // first/last key
        // todo: unwrap 能不能去掉？
        let first_key = meta.first().unwrap().first_key.clone();
        let last_key = meta.last().unwrap().last_key.clone();

        let bloom = {
            // todo: bloom_bits_per_key 应该合并在 constructor 中
            let bits_per_key = Bloom::bloom_bits_per_key(key_hashes.len(), 0.01);
            Bloom::build_from_key_hashes(&key_hashes, bits_per_key)
        };

        // encode meta
        let meta_offset = data.len() as u32;
        data.extend(meta.iter().flat_map(BlockMeta::encode));
        data.extend(meta_offset.to_be_bytes());

        // encode bloom
        let bloom_offset = data.len() as u32;
        bloom.encode(&mut data);
        data.put_u32(bloom_offset);

        // encode max_ts
        data.put_u64(max_ts);

        let file = persistent.create_sst(id, data).await?;

        let table = SsTable::builder()
            .file(file)
            .block_meta(meta)
            .block_meta_offset(meta_offset as usize)
            .id(id)
            .block_cache(block_cache)
            .first_key(first_key)
            .last_key(last_key)
            .bloom(Some(bloom))
            .max_ts(max_ts)
            .build();

        Ok(table)
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0 && self.builder.is_empty()
    }

    pub fn flush<W: WalHandle>(&mut self, memtable: &ImmutableMemTable<W>) {
        for entry in memtable.iter() {
            self.add(entry.key().as_key_slice(), entry.value().as_bytes());
        }
    }
}

#[cfg(test)]
pub mod test_util {
    use tempfile::TempDir;

    use crate::key::KeyVec;
    use crate::persistent::file_object::FileObject;
    use crate::persistent::LocalFs;
    use crate::sst::{SsTable, SsTableBuilder};

    impl SsTableBuilder {
        pub(crate) async fn build_for_test(
            self,
            dir: &TempDir,
            id: usize,
        ) -> anyhow::Result<SsTable<FileObject>> {
            let persistent = LocalFs::new(dir.path().to_path_buf());
            self.build(id, None, &persistent).await
        }
    }

    pub fn key_of(idx: usize) -> KeyVec {
        KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
    }

    pub fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    pub fn num_of_keys() -> usize {
        100
    }

    pub async fn generate_sst(dir: &TempDir) -> SsTable<FileObject> {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(key.as_key_slice(), &value[..]);
        }
        builder.build_for_test(dir, 1).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::block::BlockCache;
    
    use crate::key::{Key, KeySlice};
    use crate::persistent::{LocalFs, Persistent};
    use crate::sst::builder::test_util::{key_of, num_of_keys, value_of};
    use crate::sst::iterator::SsTableIterator;
    use crate::sst::{SsTable, SsTableBuilder};
    use futures::StreamExt;

    #[tokio::test]
    async fn test_sst_build_single_key() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333");
        let dir = tempdir().unwrap();
        builder.build_for_test(&dir, 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_sst_build_two_blocks() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"33"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"44"), b"22");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"55"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"66"), b"22");
        assert!(builder.meta.len() >= 2);
        let dir = tempdir().unwrap();
        builder.build_for_test(&dir, 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_task2_sst_decode() {
        let builder = get_builder();
        let dir = tempdir().unwrap();
        let sst = builder.build_for_test(&dir, 1).await.unwrap();
        let sst2 = SsTable::open(1, None, &LocalFs::new(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bloom_1 = sst.bloom.as_ref().unwrap();
        let bloom_2 = sst2.bloom.as_ref().unwrap();
        assert_eq!(bloom_1.k, bloom_2.k);
        assert_eq!(bloom_1.filter, bloom_2.filter);
    }

    #[tokio::test]
    async fn test_task3_block_key_compression() {
        let builder = get_builder();
        let dir = tempdir().unwrap();
        let sst = builder.build_for_test(&dir, 1).await.unwrap();
        assert!(
            sst.block_meta.len() <= 40,
            "you have {} blocks, expect 25",
            sst.block_meta.len()
        );
    }

    #[tokio::test]
    async fn test_sst_build_multi_version_simple() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(
            KeySlice::for_testing_from_slice_with_ts(b"233", 233),
            b"233333",
        );
        builder.add(
            KeySlice::for_testing_from_slice_with_ts(b"233", 0),
            b"2333333",
        );
        let dir = tempdir().unwrap();
        builder.build_for_test(&dir, 1).await.unwrap();
    }

    // todo: add test
    #[tokio::test]
    async fn test_sst_build_multi_version_hard() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let data = generate_test_data();
        let _ = generate_sst_with_ts(1, &persistent, data.clone(), None).await;
        let sst = SsTable::open(1, None, &persistent).await.unwrap();
        let result: Vec<_> = SsTableIterator::create_and_seek_to_first(&sst)
            .map(|entry| {
                let entry = entry.unwrap();
                let Key { key, timestamp } = entry.key;
                let value = entry.value;
                ((key, timestamp), value)
            })
            .collect()
            .await;
        assert_eq!(data, result);
    }

    #[tokio::test]
    async fn test_task3_sst_ts() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"11", 1), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"22", 2), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"33", 3), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"44", 4), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"55", 5), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"66", 6), b"22");
        let dir = tempdir().unwrap();
        let sst = builder.build_for_test(&dir, 1).await.unwrap();
        assert_eq!(*sst.max_ts(), 6);
    }

    #[allow(dead_code)]
    pub async fn generate_sst_with_ts<P: Persistent>(
        id: usize,
        persistent: &P,
        data: Vec<((Bytes, u64), Bytes)>,
        block_cache: Option<Arc<BlockCache>>,
    ) -> SsTable<P::SstHandle> {
        let mut builder = SsTableBuilder::new(128);
        for ((key, ts), value) in data {
            builder.add(
                KeySlice::for_testing_from_slice_with_ts(&key[..], ts),
                &value[..],
            );
        }
        builder.build(id, block_cache, persistent).await.unwrap()
    }

    #[allow(dead_code)]
    fn generate_test_data() -> Vec<((Bytes, u64), Bytes)> {
        (0..100)
            .map(|id| {
                (
                    (Bytes::from(format!("key{:05}", id / 5)), 5 - (id % 5)),
                    Bytes::from(format!("value{:05}", id)),
                )
            })
            .collect()
    }

    fn get_builder() -> SsTableBuilder {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(
                KeySlice::for_testing_from_slice_no_ts(key.raw_ref()),
                &value[..],
            );
        }
        builder
    }
}
