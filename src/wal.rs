use std::future::Future;
use std::io::Cursor;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;

use crate::persistent::interface::WalHandle;
use crate::persistent::Persistent;
use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

pub struct Wal<File> {
    file: Arc<Mutex<File>>,
}

impl<File: WalHandle> Wal<File> {
    pub async fn create<P: Persistent<WalHandle = File>>(
        id: usize,
        persistent: &P,
    ) -> Result<Self> {
        let file = persistent.open_wal_handle(id).await?;
        let wal = Wal {
            file: Arc::new(Mutex::new(file)),
        };
        Ok(wal)
    }

    pub async fn recover<P: Persistent<WalHandle = File>>(
        id: usize,
        persistent: &P,
    ) -> Result<(Self, SkipMap<Bytes, Bytes>)> {
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
            let value_len = data.get_u32();
            let value = data.copy_to_bytes(value_len as usize);
            map.insert(key, value);
        }
        let wal = Wal {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((wal, map))
    }

    pub async fn put<'a>(
        &'a self,
        key: &'a [u8],
        value: &'a [u8],
    ) -> Result<()> {
        let mut guard = self.file.lock().await;
        guard.write_u32(key.len() as u32).await?;
        guard.write_all(key).await?;
        guard.write_u32(value.len() as u32).await?;
        guard.write_all(value).await?;
        guard.flush().await?;
        guard.sync_all().await?;
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock().await;
        guard.sync_all().await?;
        Ok(())
    }
}

async fn get_file(path: impl AsRef<Path>) -> anyhow::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    Ok(file)
}
