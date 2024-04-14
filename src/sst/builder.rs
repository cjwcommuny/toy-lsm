use std::mem;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
#[cfg(test)]
use tempfile::TempDir;

use crate::block::{BlockBuilder, BlockCache};
use crate::key::{KeySlice, KeyVec};
use crate::persistent::file_object::FileObject;
use crate::persistent::{LocalFs, Persistent};
use crate::sst::bloom::Bloom;
use crate::sst::{BlockMeta, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
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
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let success = self.builder.add(key, value);
        if success {
            self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
            return;
        }
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
    ) -> Result<SsTable<P::Handle>>
    where
        P: Persistent,
    {
        let Self {
            builder,
            mut data,
            mut meta,
            block_size: _,
            key_hashes,
        } = self;

        // add last block
        if !builder.is_empty() {
            Self::add_block(&mut data, &mut meta, builder);
        }

        // first/last key
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

        let file = persistent.create(id, data).await?;

        let table = SsTable::builder()
            .file(file)
            .block_meta(meta)
            .block_meta_offset(meta_offset as usize)
            .id(id)
            .block_cache(block_cache)
            .first_key(first_key)
            .last_key(last_key)
            .bloom(Some(bloom))
            .max_ts(0)
            .build();

        Ok(table)
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0 && self.builder.is_empty()
    }
}

#[cfg(test)]
impl SsTableBuilder {
    async fn build_for_test(self, dir: TempDir, id: usize) -> anyhow::Result<SsTable<FileObject>> {
        let persistent = LocalFs::new(dir.into_path());
        self.build(id, None, &persistent).await
    }
}

#[cfg(test)]
async fn generate_sst(dir: TempDir) -> SsTable<FileObject> {
    fn key_of(idx: usize) -> KeyVec {
        KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
    }
    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }
    fn num_of_keys() -> usize {
        100
    }

    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(key.as_key_slice(), &value[..]);
    }
    builder.build_for_test(dir, 1).await.unwrap()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::key::KeySlice;
    use crate::sst::SsTableBuilder;

    #[tokio::test]
    async fn test_sst_build_single_key() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333");
        let dir = tempdir().unwrap();
        builder.build_for_test(dir, 1).await.unwrap();
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
        builder.build_for_test(dir, 1).await.unwrap();
    }
}
