use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use derive_new::new;
use nom::AsBytes;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tracing::Instrument;

use crate::persistent::{SstHandle, Persistent};

#[derive(new)]
pub struct LocalFs {
    dir: PathBuf,
}

impl LocalFs {
    fn build_path(&self, id: usize) -> PathBuf {
        self.dir.join(format!("{}.sst", id))
    }
}

impl Persistent for LocalFs {
    type SstHandle = FileObject;

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    async fn create_sst(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::SstHandle> {
        let size = data.len().try_into()?;
        let path = self.build_path(id);
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
        let path = self.build_path(id);
        let handle = spawn_blocking(|| {
            let file = File::options().read(true).write(false).open(path)?;
            let file = Arc::new(file);
            let size = file.metadata()?.len();
            let handle = FileObject { file, size };
            Ok::<_, anyhow::Error>(handle)
        })
        .await??;
        Ok(handle)
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
