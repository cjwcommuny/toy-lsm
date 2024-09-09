use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait Persistent: Send + Sync + Clone + 'static {
    type SstHandle: SstHandle;
    type WalHandle: WalHandle;
    type ManifestHandle: ManifestHandle;

    fn create_sst(
        &self,
        id: usize,
        data: Vec<u8>,
    ) -> impl Future<Output = anyhow::Result<Self::SstHandle>> + Send;

    fn open_sst(&self, id: usize) -> impl Future<Output = anyhow::Result<Self::SstHandle>> + Send;

    fn open_wal_handle(
        &self,
        id: usize,
    ) -> impl Future<Output = anyhow::Result<Self::WalHandle>> + Send;

    fn delete_wal(&self, id: usize) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn open_manifest(&self) -> impl Future<Output = anyhow::Result<Self::ManifestHandle>> + Send;
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

pub trait WalHandle: AsyncWrite + AsyncRead + Send + Sync + Unpin + 'static {
    fn sync_all(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub trait ManifestHandle: AsyncWrite + AsyncRead + Send + Sync + Unpin + 'static {
    fn sync_all(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}
