use crate::test_utils::integration::common::Database;
use crate::test_utils::integration::mydb::MyDbWithRuntime;
use crate::test_utils::integration::rocksdb::RocksdbWithWriteOpt;
use bytes::Bytes;
use derive_new::new;
use itertools::Itertools;

#[derive(new)]
pub struct DbPair {
    my_db: MyDbWithRuntime,
    rocksdb: RocksdbWithWriteOpt,
}

impl Database for DbPair {
    type Error = anyhow::Error;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error> {
        let kvs: Vec<_> = kvs.collect();
        self.my_db.write_batch(kvs.clone().into_iter())?;
        self.rocksdb.write_batch(kvs.into_iter())?;
        Ok(())
    }

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = key.as_ref();
        let my_value = self.my_db.get(key)?;
        let rocksdb_value = self.rocksdb.get(key)?;
        assert_eq!(my_value, rocksdb_value);
        Ok(my_value)
    }

    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error> {
        let my_iter = self.my_db.iter(begin)?;
        let rocksdb_iter = self.rocksdb.iter(begin)?;
        let iter = my_iter
            .zip_eq(rocksdb_iter)
            .inspect(|pair| {
                if let (Ok(left), Ok(right)) = pair {
                    assert_eq!(left, right)
                }
            })
            .map(|pair| pair.0);
        Ok(iter)
    }
}

#[cfg(test)]
mod test {
    use crate::lsm::core::Lsm;
    use crate::persistent::LocalFs;
    use crate::test_utils::integration::common::{iterate, populate, randread};
    use crate::test_utils::integration::mydb::{build_sst_options, MyDbWithRuntime};
    use crate::test_utils::integration::pair::DbPair;
    use crate::test_utils::integration::rocksdb::{build_rocks_db, build_rocks_options};
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[test]
    fn test_with_rocksdb() {
        for _ in 0..1 {
            test_with_rocksdb_single_round();
        }
    }

    fn test_with_rocksdb_single_round() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let rocksdb_dir = tempfile::Builder::new()
            .prefix("rocksdb")
            .tempdir()
            .unwrap();
        let mydb_dir = tempfile::Builder::new().prefix("mydb").tempdir().unwrap();
        let persistent = LocalFs::new(mydb_dir.path().to_path_buf());

        let db = {
            let rocksdb = build_rocks_db(&build_rocks_options(), &rocksdb_dir);
            let my_db = {
                let options = build_sst_options();
                let db = runtime.block_on(Lsm::new(options, persistent)).unwrap();
                MyDbWithRuntime::new(db, runtime.clone())
            };
            Arc::new(DbPair::new(my_db, rocksdb))
        };

        populate(db.clone(), 16_000, 1000, 100, 64, true);

        for _ in 0..10 {
            populate(db.clone(), 16_000, 1000, 100, 64, false);
        }

        for _ in 0..1000 {
            randread(db.clone(), 16_000, 1000, 64);
            iterate(db.clone(), 16_000, 1000, 64);
        }
    }
}
