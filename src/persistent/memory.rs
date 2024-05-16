use std::sync::Arc;

use anyhow::anyhow;
use dashmap::DashMap;

use crate::persistent::{SstHandle, Persistent};

#[derive(Default)]
pub struct Memory {
    map: DashMap<usize, Arc<MemoryObject>>,
}

impl Persistent for Memory {
    type SstHandle = Arc<MemoryObject>;

    async fn create_sst(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::SstHandle> {
        let handle = Arc::new(MemoryObject { id, data });
        self.map.insert(id, handle.clone());
        Ok(handle)
    }

    async fn open_sst(&self, id: usize) -> anyhow::Result<Self::SstHandle> {
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

impl SstHandle for MemoryObject {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        let offset = offset.try_into()?;
        let output = self.data[offset..offset + len].to_vec();
        Ok(output)
    }

    fn size(&self) -> u64 {
        self.data.len().try_into().unwrap()
    }
}
