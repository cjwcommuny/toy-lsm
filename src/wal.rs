use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use byteorder::ReadBytesExt;
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

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }
}
