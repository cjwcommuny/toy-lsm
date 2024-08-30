use std::future::Future;
use std::sync::Arc;

use crate::persistent::interface::ManifestHandle;
use anyhow::Result;
use derive_more::From;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::persistent::Persistent;
use crate::sst::compact::common::NewCompactionRecord;

pub struct Manifest<File> {
    file: Arc<Mutex<File>>,
}

impl<File> Clone for Manifest<File> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, From)]
pub enum ManifestRecord {
    Flush(Flush),
    NewMemtable(NewMemtable),
    Compaction(Compaction),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Flush(pub(crate) usize);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct NewMemtable(pub(crate) usize);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Compaction(pub(crate) Vec<NewCompactionRecord>);

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

    pub fn add_record(&self, record: ManifestRecord) -> impl Future<Output = Result<()>> + Send {
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
            guard.flush().await?;
            guard.sync_all().await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::{Compaction, Flush, Manifest, ManifestRecord, NewMemtable};
    use crate::persistent::LocalFs;
    use crate::sst::compact::common::{NewCompactionRecord, NewCompactionTask};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_manifest() {
        use ManifestRecord as R;

        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());

        {
            let manifest = Manifest::create(&persistent).await.unwrap();

            let record = Compaction(vec![NewCompactionRecord::new(
                NewCompactionTask::new(1, vec![2], 3, vec![]),
                vec![1, 2, 3],
            )]);
            manifest
                .add_record_when_init(R::Compaction(record))
                .await
                .unwrap();
            manifest
                .add_record_when_init(R::Flush(Flush(100)))
                .await
                .unwrap();
            manifest
                .add_record_when_init(R::NewMemtable(NewMemtable(2000)))
                .await
                .unwrap();
        }

        {
            let (_manifest, records) = Manifest::recover(&persistent).await.unwrap();

            let record = Compaction(vec![NewCompactionRecord::new(
                NewCompactionTask::new(1, vec![2], 3, vec![]),
                vec![1, 2, 3],
            )]);

            assert_eq!(
                records,
                vec![
                    R::Compaction(record),
                    R::Flush(Flush(100)),
                    R::NewMemtable(NewMemtable(2000))
                ]
            );
        }
    }
}
