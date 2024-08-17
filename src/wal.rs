use crate::key::KeyBytes;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use std::io::Cursor;
use std::iter;
use std::sync::Arc;

use crate::entry::Keyed;
use crate::persistent::interface::WalHandle;
use crate::persistent::Persistent;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

pub struct Wal<File> {
    file: Arc<Mutex<File>>,
}

impl<File: WalHandle> Wal<File> {
    pub async fn create<P: Persistent<WalHandle = File>>(
        id: usize,
        persistent: &P,
    ) -> anyhow::Result<Self> {
        let file = persistent.open_wal_handle(id).await?;
        let wal = Wal {
            file: Arc::new(Mutex::new(file)),
        };
        Ok(wal)
    }

    pub async fn recover<P: Persistent<WalHandle = File>>(
        id: usize,
        persistent: &P,
    ) -> anyhow::Result<(Self, SkipMap<KeyBytes, Bytes>)> {
        let mut file = persistent.open_wal_handle(id).await?;
        let data = {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            data
        };
        let mut data = Cursor::new(data);
        let map = SkipMap::new();
        while data.has_remaining() {
            let key_len = data.get_u32();
            let key = data.copy_to_bytes(key_len as usize);
            let timestamp = data.get_u64();
            let key = KeyBytes::new(key, timestamp);

            let value_len = data.get_u32();
            let value = data.copy_to_bytes(value_len as usize);

            map.insert(key, value);
        }
        let wal = Wal {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((wal, map))
    }

    pub async fn put_batch(
        &self,
        entries: impl Iterator<Item = Keyed<&[u8], &[u8]>> + Send,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        // todo: atomic wal
        let mut guard = self.file.lock().await;
        for entry in entries {
            let key = entry.key;
            guard.write_u32(key.len() as u32).await?;
            guard.write_all(key).await?;
            guard.write_u64(timestamp).await?;

            let value = entry.value;
            guard.write_u32(value.len() as u32).await?;
            guard.write_all(value).await?;
        }
        guard.flush().await?;
        guard.sync_all().await?;

        Ok(())
    }

    pub async fn sync(&self) -> anyhow::Result<()> {
        let guard = self.file.lock().await;
        guard.sync_all().await?;
        Ok(())
    }
}

impl<File: WalHandle> Wal<File> {
    pub async fn put_for_test<'a>(
        &'a self,
        key: &'a [u8],
        ts: u64,
        value: &'a [u8],
    ) -> anyhow::Result<()> {
        let entry = Keyed::new(key, value);
        self.put_batch(iter::once(entry), ts).await
    }
}

#[cfg(test)]
mod tests {
    use crate::key::KeyBytes;

    use tempfile::tempdir;

    use crate::persistent::LocalFs;
    use crate::wal::Wal;

    #[tokio::test]
    async fn test_wal() {
        let dir = tempdir().unwrap();
        let persistent = LocalFs::new(dir.path().to_path_buf());
        let id = 123;

        {
            let wal = Wal::create(id, &persistent).await.unwrap();
            wal.put_for_test("111".as_bytes(), 123, "a".as_bytes())
                .await
                .unwrap();
            wal.put_for_test("222".as_bytes(), 234, "bb".as_bytes())
                .await
                .unwrap();
            wal.put_for_test("333".as_bytes(), 345, "ccc".as_bytes())
                .await
                .unwrap();
            wal.put_for_test("4".as_bytes(), 456, "".as_bytes())
                .await
                .unwrap();
            wal.sync().await.unwrap();
        }

        {
            let (_wal, map) = Wal::recover(id, &persistent).await.unwrap();
            assert_eq!(
                map.get(&KeyBytes::new_for_test(b"111", 123))
                    .unwrap()
                    .value(),
                "a"
            );
            assert_eq!(
                map.get(&KeyBytes::new_for_test(b"222", 234))
                    .unwrap()
                    .value(),
                "bb"
            );
            assert_eq!(
                map.get(&KeyBytes::new_for_test(b"333", 345))
                    .unwrap()
                    .value(),
                "ccc"
            );
            assert_eq!(
                map.get(&KeyBytes::new_for_test(b"4", 456)).unwrap().value(),
                ""
            );
            assert!(map.get(&KeyBytes::new_for_test(b"555", 0)).is_none());
        }
    }
}
