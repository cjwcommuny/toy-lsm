use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn recover_sync(path: impl AsRef<Path>) -> Result<(Self, SkipMap<Bytes, Bytes>)> {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let mut data = {
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
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

    pub fn put_sync(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut guard = self.file.lock();
        guard.write_u32::<BigEndian>(key.len() as u32)?;
        guard.write(key)?;
        guard.write_u32::<BigEndian>(value.len() as u32)?;
        guard.write(value)?;
        guard.flush()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
