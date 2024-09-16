use better_mini_lsm::entry::Entry;
use better_mini_lsm::iterators::lsm::LsmIterator;
use better_mini_lsm::lsm::core::Lsm;
use better_mini_lsm::persistent::LocalFs;
use better_mini_lsm::state::write_batch::WriteBatchRecord;
use better_mini_lsm::state::Map;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use parking_lot::RwLock;
use rand::{distributions::Alphanumeric, Rng};
use rocksdb::{DBRawIteratorWithThreadMode, WriteOptions, DB};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::ops::Bound::{Included, Unbounded};
use std::{
    fs::{read_dir, remove_file},
    path::Path,
    sync::Arc,
    time::UNIX_EPOCH,
};
use tokio::runtime::Runtime;

pub fn rand_value() -> String {
    let v = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .collect::<Vec<_>>();
    String::from_utf8(v).unwrap()
}

pub fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("vsz={:05}-k={:010}", value_size, key));

    let mut value = BytesMut::with_capacity(value_size);
    value.resize(value_size, 0);

    (key, value.freeze())
}

pub fn unix_time() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("Time went backwards")
        .as_millis() as u64
}

pub fn remove_files(path: &Path) {
    read_dir(path).unwrap().for_each(|entry| {
        let entry = entry.unwrap();
        remove_file(entry.path()).unwrap();
    });
    sync_dir(&path).unwrap();
}

pub fn sync_dir(path: &impl AsRef<Path>) -> anyhow::Result<()> {
    File::open(path.as_ref())?.sync_all()?;
    Ok(())
}

pub trait Database: Send + Sync + 'static {
    type Error: Debug;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error>;
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error>;
    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error>;
}

pub struct MyDbWithRuntime {
    db: Lsm<LocalFs>,
    runtime: Arc<Runtime>,
}

impl Database for MyDbWithRuntime {
    type Error = anyhow::Error;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error> {
        self.runtime.block_on(async {
            let batch: Vec<_> = kvs
                .into_iter()
                .map(|(key, value)| WriteBatchRecord::Put(key, value))
                .collect();
            self.db.put_batch(&batch).await?;
            Ok(())
        })
    }

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = key.as_ref();
        let value = self.runtime.block_on(async { self.db.get(key).await })?;
        Ok(value.map(Into::into))
    }

    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error> {
        let iter = self
            .runtime
            .block_on(async move { self.db.scan(Included(begin), Unbounded).await })?;
        let iter = LsmIterWithRuntime {
            iter,
            runtime: self.runtime.clone(),
        };
        Ok(iter)
    }
}

pub struct LsmIterWithRuntime<'a> {
    iter: LsmIterator<'a>,
    runtime: Arc<Runtime>,
}

impl<'a> Iterator for LsmIterWithRuntime<'a> {
    type Item = anyhow::Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(async {
            self.iter
                .next()
                .await
                .map(|entry| entry.map(Entry::into_tuple))
        })
    }
}

pub struct MyDbWithMap {
    db: Lsm<LocalFs>,
    map: RwLock<HashMap<Bytes, Bytes>>,
    runtime: Runtime,
}
//
// impl Database for MyDbWithMap {
//     type Error = anyhow::Error;
//
//     fn write_batch(&self, kvs: impl Iterator<Item=(Bytes, Bytes)>) -> Result<(), Self::Error> {
//         let batch: Vec<_> = kvs.into_iter().map(|(key, value)| WriteBatchRecord::Put(key, value)).collect();
//
//         self.runtime.block_on(async {
//
//             self.db.put(key.clone(), value.clone()).await
//         })?;
//
//         // todo: use write batch
//         for (key, value) in kvs {
//             self.runtime.block_on(async {
//
//                 self.db.put(key.clone(), value.clone()).await
//             })?;
//             {
//                 let mut guard = self.map.write();
//                 guard.insert(key, value);
//             }
//         }
//         Ok(())
//     }
//
//     fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
//         let key = key.as_ref();
//         let value = self.runtime.block_on(async {
//             self.db.get(key).await
//         })?;
//         let value_expected = {
//             let guard = self.map.read();
//             guard.get(key).map(|b| b.clone())
//         };
//         assert_eq!(value, value_expected);
//         Ok(value.map(Into::into))
//     }
//
//     fn iter(&self, begin: impl AsRef<[u8]>) -> impl Iterator<Item=Result<(Bytes, Bytes), Self::Error>> {
//         todo!()
//     }
// }

pub struct RocksdbWithWriteOpt {
    db: DB,
    write_options: WriteOptions,
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

pub fn build_rocks_db(opts: &rocksdb::Options, dir: impl AsRef<Path>) -> Arc<RocksdbWithWriteOpt> {
    let write_options = {
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(true);
        write_options.disable_wal(false);
        write_options
    };
    let db = rocksdb::DB::open(opts, &dir).unwrap();
    Arc::new(RocksdbWithWriteOpt { db, write_options })
}

pub fn populate<D: Database>(
    db: Arc<D>,
    key_nums: u64,
    chunk_size: u64,
    batch_size: u64,
    value_size: usize,
    seq: bool,
) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();

        handles.push(std::thread::spawn(move || {
            let range = chunk_start..chunk_start + chunk_size;

            for batch_start in range.step_by(batch_size as usize) {
                let mut rng = rand::thread_rng();

                let kvs = (batch_start..batch_start + batch_size).map(|key| {
                    if seq {
                        gen_kv_pair(key, value_size)
                    } else {
                        gen_kv_pair(rng.gen_range(0..key_nums), value_size)
                    }
                });

                db.write_batch(kvs).unwrap();
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn randread<D: Database>(db: Arc<D>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let range = chunk_start..chunk_start + chunk_size;

            for _ in range {
                let (key, _) = gen_kv_pair(rng.gen_range(0..key_nums), value_size);
                match db.get(key) {
                    Ok(item) => {
                        if item.is_some() {
                            assert_eq!(item.unwrap().len(), value_size);
                        }
                    }
                    Err(err) => {
                        panic!("{:?}", err);
                    }
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn iterate<D: Database>(db: Arc<D>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();
        let (key, _) = gen_kv_pair(chunk_start, value_size);

        handles.push(std::thread::spawn(move || {
            let iter = db.iter(key.as_ref()).unwrap();

            for entry in iter.take(chunk_size as usize) {
                let (_, value) = entry.unwrap();
                assert_eq!(value.len(), value_size);
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}
