use std::future::Future;
use std::sync::Arc;

pub trait SstPersistent: Send + Sync + 'static {
    type Handle: SstHandle;

    fn create(
        &self,
        id: usize,
        data: Vec<u8>,
    ) -> impl Future<Output = anyhow::Result<Self::Handle>> + Send;
    fn open(&self, id: usize) -> impl Future<Output = anyhow::Result<Self::Handle>> + Send;
}

pub trait SstHandle: Send + Sync + 'static {
    fn read(&self, offset: u64, len: usize)
        -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send;

    fn size(&self) -> u64;
}

impl<T: SstHandle> SstHandle for Arc<T> {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        self.as_ref().read(offset, len).await
    }

    fn size(&self) -> u64 {
        self.as_ref().size()
    }
}
