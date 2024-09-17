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
