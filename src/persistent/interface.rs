use std::sync::Arc;

pub trait Persistent {
    type Handle: PersistentHandle;

    async fn create(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::Handle>;
    async fn open(&self, id: usize) -> anyhow::Result<Self::Handle>;
}

pub trait PersistentHandle {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>>;

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
