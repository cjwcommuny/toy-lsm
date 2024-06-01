use anyhow::Context;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use derive_new::new;
use nom::AsBytes;
use tokio::io::BufWriter;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tracing::Instrument;

use crate::persistent::interface::WalHandle;
use crate::persistent::manifest_handle::ManifestFile;
use crate::persistent::wal_handle::WalFile;
use crate::persistent::{Persistent, SstHandle};

#[derive(new)]
pub struct LocalFs {
    dir: PathBuf,
}

impl LocalFs {
    fn build_sst_path(&self, id: usize) -> PathBuf {
        self.dir.join(format!("{}.sst", id))
    }

    fn build_wal_path(&self, id: usize) -> PathBuf {
        self.dir.join(format!("{}.wal", id))
    }

    fn build_manifest_path(&self) -> PathBuf {
        self.dir.join("MANIFEST")
    }
}

impl Persistent for LocalFs {
    type SstHandle = FileObject;
    type WalHandle = WalFile;
    type ManifestHandle = ManifestFile;

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    async fn create_sst(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::SstHandle> {
        println!("create sst {}", id);
        let size = data.len().try_into()?;
        let path = self.build_sst_path(id);
        let file = spawn_blocking(move || {
            std::fs::write(&path, &data)?;
            File::open(&path)?.sync_all()?;
            let file = File::options().read(true).append(true).open(&path)?;
            Ok::<_, anyhow::Error>(Arc::new(file))
        })
        .await??;
        let handle = FileObject { file, size };
        Ok(handle)
    }

    async fn open_sst(&self, id: usize) -> anyhow::Result<Self::SstHandle> {
        let path = self.build_sst_path(id);
        let handle = spawn_blocking(move || {
            let file = File::options()
                .read(true)
                .write(false)
                .open(&path)
                .with_context(|| format!("id: {}, path: {:?}", id, &path))?;
            let file = Arc::new(file);
            let size = file.metadata()?.len();
            let handle = FileObject { file, size };
            Ok::<_, anyhow::Error>(handle)
        })
        .await??;
        Ok(handle)
    }

    async fn open_wal_handle(&self, id: usize) -> anyhow::Result<Self::WalHandle> {
        println!("open wal {}", id);
        let path = self.build_wal_path(id);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await?;
        let wal = WalFile::new(BufWriter::new(file));
        Ok(wal)
    }

    async fn delete_wal(&self, id: usize) -> anyhow::Result<()> {
        let path = self.build_wal_path(id);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn open_manifest(&self) -> anyhow::Result<Self::ManifestHandle> {
        let path = self.build_manifest_path();
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        let manifest = ManifestFile::new(BufWriter::new(file));
        Ok(manifest)
    }
}

/// A file object.
pub struct FileObject {
    file: Arc<File>,
    size: u64,
}

impl SstHandle for FileObject {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        let file = self.file.clone();
        let data = spawn_blocking(move || {
            let mut data = vec![0; len];
            file.read_exact_at(&mut data[..], offset)?;
            Ok::<_, anyhow::Error>(data)
        })
        .instrument(tracing::info_span!("read spawn"))
        .await??;
        Ok(data)
    }

    fn size(&self) -> u64 {
        self.size
    }
}

impl FileObject {}
