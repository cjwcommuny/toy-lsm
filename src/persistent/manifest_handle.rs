use crate::persistent::interface::ManifestHandle;
use crate::persistent::wal_handle::WalFile;

pub type ManifestFile = WalFile;

impl ManifestHandle for WalFile {}
