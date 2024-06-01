use crate::persistent::interface::{ManifestHandle, WalHandle};
use crate::persistent::wal_handle::WalFile;
use std::future::Future;

pub type ManifestFile = WalFile;

impl ManifestHandle for WalFile {
    fn sync_all(&self) -> impl Future<Output = anyhow::Result<()>> + Send {
        WalHandle::sync_all(self)
    }
}
