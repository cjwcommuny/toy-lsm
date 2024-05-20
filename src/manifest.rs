use std::future::Future;
use std::io::{Read, Write};
use std::sync::Arc;

use crate::persistent::interface::ManifestHandle;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, MutexGuard};

use crate::persistent::Persistent;
use crate::sst::compact::common::CompactionTask;

pub struct Manifest<File> {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(Flush),
    NewMemtable(NewMemtable),
    Compaction(Compaction),
}

#[derive(Serialize, Deserialize)]
pub struct Flush(pub(crate) usize);

#[derive(Serialize, Deserialize)]
pub struct NewMemtable(pub(crate) usize);

#[derive(Serialize, Deserialize)]
pub struct Compaction(pub(crate) CompactionTask, pub(crate) Vec<usize>);

impl<File: ManifestHandle> Manifest<File> {
    pub async fn create<P: Persistent<ManifestHandle = File>>(persistent: &P) -> Result<Self> {
        let file = persistent.open_manifest().await?;
        let file = Arc::new(Mutex::new(file));
        let manifest = Manifest { file };
        Ok(manifest)
    }

    pub async fn recover<P: Persistent<ManifestHandle = File>>(
        persistent: &P,
    ) -> Result<(Self, Vec<ManifestRecord>)> {
        let manifest = Self::create(persistent).await?;
        let records = {
            let mut guard = manifest.file.lock().await;
            let mut buffer = Vec::new();
            guard.read_to_end(&mut buffer).await?;
            let de = Deserializer::from_slice(&buffer);
            de.into_iter().collect::<std::result::Result<_, _>>()?
        };
        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> impl Future<Output = Result<()>> + Send {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(
        &self,
        record: ManifestRecord,
    ) -> impl Future<Output = Result<()>> + Send {
        let file = self.file.clone();
        async move {
            // todo: use a buffer
            let data = serde_json::to_vec(&record)?;
            let mut guard = file.lock().await;
            guard.write_all(&data).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_manifest() {}
}
