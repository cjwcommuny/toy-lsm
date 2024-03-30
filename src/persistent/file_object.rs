use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use derive_new::new;
use tokio::task::spawn_blocking;

use crate::persistent::{Persistent, PersistentHandle};

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
    type Handle = FileObject;

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    async fn create(&self, id: usize, data: Vec<u8>) -> anyhow::Result<Self::Handle> {
        let size = data.len().try_into()?;
        let path = self.build_path(id);
        let file = spawn_blocking(move || {
            std::fs::write(&path, &data)?;
            File::open(&path)?.sync_all()?;
            let file = File::options().read(true).write(false).open(&path)?;
            Ok::<_, anyhow::Error>(Arc::new(file))
        })
        .await??;
        let handle = FileObject { file, size };
        Ok(handle)
    }

    async fn open(&self, id: usize) -> anyhow::Result<Self::Handle> {
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

impl PersistentHandle for FileObject {
    async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        let file = self.file.clone();
        let data = spawn_blocking(move || {
            let mut data = vec![0; len];
            file.read_exact_at(&mut data[..], offset)?;
            Ok::<_, anyhow::Error>(data)
        })
        .await??;
        Ok(data)
    }

    fn size(&self) -> u64 {
        self.size
    }
}
