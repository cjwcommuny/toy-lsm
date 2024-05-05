// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use std::sync::Arc;

use anyhow::anyhow;
use dashmap::DashMap;

use crate::persistent::{Persistent, PersistentHandle};

#[derive(Default)]
pub struct Memory {
    map: DashMap<usize, Arc<MemoryObject>>,
}

impl Persistent for Memory {
    type Handle = Arc<MemoryObject>;

    async fn create(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::Handle> {
        let handle = Arc::new(MemoryObject { id, data });
        self.map.insert(id, handle.clone());
        Ok(handle)
    }

    async fn open(&self, id: usize) -> anyhow::Result<Self::Handle> {
        let handle = self
            .map
            .get(&id)
            .ok_or(anyhow!("unknown id {}", id))?
            .clone();
        Ok(handle)
    }
}

pub struct MemoryObject {
    id: usize,
    data: Vec<u8>,
}

impl PersistentHandle for MemoryObject {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        let offset = offset.try_into()?;
        let output = self.data[offset..offset + len].to_vec();
        Ok(output)
    }

    fn size(&self) -> u64 {
        self.data.len().try_into().unwrap()
    }
}
