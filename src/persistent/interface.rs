// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use std::future::Future;
use std::sync::Arc;

pub trait Persistent: Send + Sync + 'static {
    type Handle: PersistentHandle;

    fn create(
        &self,
        id: usize,
        data: Vec<u8>,
    ) -> impl Future<Output = anyhow::Result<Self::Handle>> + Send;
    fn open(&self, id: usize) -> impl Future<Output = anyhow::Result<Self::Handle>> + Send;
}

pub trait PersistentHandle: Send + Sync {
    fn read(&self, offset: u64, len: usize)
        -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send;

    fn size(&self) -> u64;
}

impl<T: PersistentHandle> PersistentHandle for Arc<T> {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        self.as_ref().read(offset, len).await
    }

    fn size(&self) -> u64 {
        self.as_ref().size()
    }
}
