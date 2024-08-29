use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes};
use derive_getters::Getters;
use nom::AsBytes;
use typed_builder::TypedBuilder;

use crate::block::{Block, BlockCache, BlockIterator};
use crate::iterators::transpose_try_iter;
use crate::key::{KeyBytes, KeySlice};
use crate::persistent::{Persistent, SstHandle};
use crate::sst::bloom::Bloom;
use crate::sst::iterator::BlockFallibleIter;
use crate::sst::BlockMeta;
use crate::utils::range::MinMax;

/// An SSTable.
#[derive(TypedBuilder, Getters)]
pub struct SsTable<File> {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: File,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    pub(crate) id: usize,
    pub(crate) block_cache: Option<Arc<BlockCache>>,
    pub(crate) first_key: KeyBytes,
    pub(crate) last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    pub max_ts: u64, // todo: use Option?
}

impl SsTable<()> {
    pub fn mock(id: usize, first_key: &str, last_key: &str) -> Self {
        SsTable {
            file: (),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key: KeyBytes::new(Bytes::copy_from_slice(first_key.as_bytes()), 0),
            last_key: KeyBytes::new(Bytes::copy_from_slice(last_key.as_bytes()), 0),
            bloom: None,
            max_ts: 0,
        }
    }
}

impl<File: SstHandle> Debug for SsTable<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsTable")
            .field("id", &self.id)
            .field("first_key", self.first_key())
            .field("last_key", self.last_key())
            .field("size", &self.table_size())
            .finish()
    }
}

impl<File> SsTable<File> {
    pub fn get_key_range(&self) -> MinMax<KeyBytes> {
        MinMax {
            min: self.first_key.clone(),
            max: self.last_key.clone(),
        }
    }
}

impl<File: SstHandle> SsTable<File> {
    /// Open SSTable from a file.
    /// todo: 避免使用 get_u32 这种会 panic 的
    /// todo: encoding 的格式可以考虑变一下，使用 parser combinator 库
    pub async fn open<P: Persistent<SstHandle = File>>(
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        persistent: &P,
    ) -> Result<Self> {
        let file = persistent.open_sst(id).await?;
        let mut end = file.size();

        let max_ts = {
            let data = file.read(end - 8, 8).await?;
            end -= 8;
            u64::from_be_bytes(data.as_slice().try_into()?)
        };

        let bloom = {
            let bloom_offset_begin = end - 4;
            let bloom_offset = file.read(bloom_offset_begin, 4).await?.as_slice().get_u32() as u64;
            let len = (bloom_offset_begin - bloom_offset).try_into().unwrap(); // todo: prevent overflow
            let bloom = file.read(bloom_offset, len).await?;
            end = bloom_offset;
            Bloom::decode(&bloom)
        }?;

        let (block_meta, block_meta_offset) = {
            let block_meta_offset_begin = end - 4;
            let block_meta_offset = file
                .read(block_meta_offset_begin, 4)
                .await?
                .as_slice()
                .get_u32() as u64;
            let len = (block_meta_offset_begin - block_meta_offset)
                .try_into()
                .unwrap(); // todo: prevent overflow
            let block_meta = {
                let block_meta = file.read(block_meta_offset, len).await?;
                BlockMeta::decode_chunk(block_meta.as_slice())
            };
            (block_meta, block_meta_offset)
        };

        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        let table = Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts,
        };
        Ok(table)
    }

    /// Read a block from the disk.
    async fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_meta[block_idx];
        let offset = meta.offset;
        let data = self
            .file
            .read(offset as u64, self.block_end(block_idx) - offset)
            .await?;
        let block = Block::decode(&data);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub async fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = self.block_cache.as_ref() {
            let key = (self.id, block_idx);
            cache
                .try_get_with(key, self.read_block(block_idx))
                .await
                .map_err(|err| anyhow!(err.to_string()))
        } else {
            self.read_block(block_idx).await
        }
    }

    fn block_end(&self, index: usize) -> usize {
        self.block_meta
            .get(index + 1)
            .map(|meta| meta.offset)
            .unwrap_or_else(|| self.block_meta_offset)
    }

    pub async fn get_block_iter(&self, block_index: usize) -> BlockFallibleIter {
        let iterator = self
            .read_block_cached(block_index)
            .await
            .map(BlockIterator::create_and_seek_to_first);
        transpose_try_iter(iterator)
    }

    // todo: 合并 get_block_iter, get_block_iter_with_key
    pub async fn get_block_iter_with_key<'a>(
        &'a self,
        block_index: usize,
        key: KeySlice<'a>,
    ) -> BlockFallibleIter {
        let iterator = self
            .read_block_cached(block_index)
            .await
            .map(|block| BlockIterator::create_and_seek_to_key(block, key));
        transpose_try_iter(iterator)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, _key: KeySlice) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn table_size(&self) -> u64 {
        self.file.size()
    }
}
