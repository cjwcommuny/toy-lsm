use crate::test_utils::integration::common::Database;
use bytes::Bytes;
use rocksdb::{DBRawIteratorWithThreadMode, WriteOptions, DB};
use std::path::Path;

pub struct RocksdbWithWriteOpt {
    db: DB,
    write_options: WriteOptions,
}

pub fn build_rocks_db(opts: &rocksdb::Options, dir: impl AsRef<Path>) -> RocksdbWithWriteOpt {
    let write_options = {
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(true);
        write_options.disable_wal(false);
        write_options
    };
    let db = rocksdb::DB::open(opts, &dir).unwrap();
    RocksdbWithWriteOpt { db, write_options }
}

impl Database for RocksdbWithWriteOpt {
    type Error = rocksdb::Error;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in kvs {
            batch.put(key, value);
        }
        self.db.write_opt(batch, &self.write_options)
    }

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db.get(key)
    }

    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error> {
        let iter = RocksdbIter::new(&self.db, begin);
        Ok(iter)
    }
}

pub struct RocksdbIter<'a> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
    head: bool,
}

impl<'a> RocksdbIter<'a> {
    pub fn new(db: &'a DB, begin: impl AsRef<[u8]>) -> RocksdbIter<'a> {
        let mut iter = db.raw_iterator();
        iter.seek(begin);
        Self { iter, head: true }
    }
}

impl<'a> Iterator for RocksdbIter<'a> {
    type Item = Result<(Bytes, Bytes), rocksdb::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.head {
            self.iter.next();
        } else {
            self.head = false;
        }

        if self.iter.valid() {
            let key = self.iter.key()?;
            let value = self.iter.value()?;

            Some(Ok((
                Bytes::copy_from_slice(key),
                Bytes::copy_from_slice(value),
            )))
        } else if let Err(e) = self.iter.status() {
            Some(Err(e))
        } else {
            None
        }
    }
}
