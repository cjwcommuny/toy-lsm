use std::future::Future;
use std::sync::Arc;

pub trait Persistent: Send + Sync {
    type Handle: PersistentHandle;

    async fn create(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::Handle>;
    async fn open(&self, id: usize) -> anyhow::Result<Self::Handle>;
}

pub trait PersistentHandle: Send + Sync {
    fn read(&self, offset: u64, len: usize) -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send;

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
