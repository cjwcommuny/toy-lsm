use crate::block::blocks::Block;
use std::sync::Arc;

pub type BlockCache = moka::future::Cache<(usize, usize), Arc<Block>>;
