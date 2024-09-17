use bytes::{Bytes, BytesMut};
use rand::Rng;
use std::fmt::Debug;
use std::sync::Arc;

pub trait Database: Send + Sync + 'static {
    type Error: Debug;

    fn write_batch(&self, kvs: impl Iterator<Item = (Bytes, Bytes)>) -> Result<(), Self::Error>;
    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Self::Error>;
    fn iter<'a>(
        &'a self,
        begin: &'a [u8],
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes), Self::Error>> + 'a, Self::Error>;
}

pub fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("vsz={:05}-k={:010}", value_size, key));

    let mut value = BytesMut::with_capacity(value_size);
    value.resize(value_size, 0);

    (key, value.freeze())
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
