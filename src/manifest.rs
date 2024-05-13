use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures_concurrency::stream::StreamExt;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use serde_json::de::IoRead;
use serde_json::{Deserializer, StreamDeserializer};

use crate::sst::compact::common::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub async fn create(path: impl AsRef<Path> + Send + 'static) -> Result<Self> {
        let file = tokio::spawn(async { OpenOptions::new().append(true).create(true).open(path) })
            .await??;
        let file = Arc::new(Mutex::new(file));
        let manifest = Manifest { file };
        Ok(manifest)
    }

    pub async fn recover(
        path: impl AsRef<Path> + Send + 'static,
    ) -> Result<(Self, Vec<ManifestRecord>)> {
        let manifest = Self::create(path).await?;
        let records = {
            let mut guard = manifest.file.lock();
            let mut buffer = Vec::new();
            guard.read_to_end(&mut buffer)?;
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
            let mut guard = file.lock();
            guard.write_all(&data)?;
            Ok(())
        }
    }
}
