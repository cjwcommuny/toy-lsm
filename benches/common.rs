use bytes::{Bytes, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
use rocksdb::DB;
use std::fs::File;
use std::{
    fs::{read_dir, remove_file},
    path::Path,
    sync::Arc,
    time::UNIX_EPOCH,
};

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

pub fn rocks_populate(
    db: Arc<DB>,
    key_nums: u64,
    chunk_size: u64,
    batch_size: u64,
    value_size: usize,
    seq: bool,
) {
    let mut write_options = rocksdb::WriteOptions::default();
    write_options.set_sync(true);
    write_options.disable_wal(false);
    let write_options = Arc::new(write_options);

    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();
        let write_options = write_options.clone();

        handles.push(std::thread::spawn(move || {
            let range = chunk_start..chunk_start + chunk_size;

            for batch_start in range.step_by(batch_size as usize) {
                let mut rng = rand::thread_rng();
                let mut batch = rocksdb::WriteBatch::default();

                (batch_start..batch_start + batch_size).for_each(|key| {
                    let (key, value) = if seq {
                        gen_kv_pair(key, value_size)
                    } else {
                        gen_kv_pair(rng.gen_range(0..key_nums), value_size)
                    };
                    batch.put(key, value);
                });

                db.write_opt(batch, &write_options).unwrap();
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn rocks_randread(db: Arc<DB>, key_nums: u64, chunk_size: u64, value_size: usize) {
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

pub fn rocks_iterate(db: Arc<DB>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();
        let (key, _) = gen_kv_pair(chunk_start, value_size);

        handles.push(std::thread::spawn(move || {
            let mut iter = db.raw_iterator();
            iter.seek(&key);
            let mut count = 0;

            while iter.valid() {
                assert_eq!(iter.value().unwrap().len(), value_size);

                iter.next();

                count += 1;
                if count > chunk_size {
                    break;
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}
