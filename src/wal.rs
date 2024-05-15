use std::io::{Cursor, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub async fn recover(path: impl AsRef<Path>) -> Result<(Self, SkipMap<Bytes, Bytes>)> {
        let mut file = OpenOptions::new().create(true).append(true).open(path).await?;
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
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        };
        Ok((wal, map))
    }

    async fn put_sync(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut guard = self.file.lock().await;
        guard.write_u32(key.len() as u32).await?;
        guard.write(key).await?;
        guard.write_u32(value.len() as u32).await?;
        guard.write(value).await?;
        guard.flush().await?;
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock().await;
        guard.get_mut().sync_all().await?;
        Ok(())
    }
}
