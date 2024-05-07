#![feature(type_alias_impl_trait)]

mod block;
mod bound;
mod entry;
mod iterators;
mod key;
mod memtable;
pub mod persistent;
pub mod sst;
pub mod state;
mod wal;

mod lsm;
mod test_utils;
mod utils;

pub async fn fibonacci(n: u64) -> u64 {
    let mut a = 0;
    let mut b = 1;

    match n {
        0 => b,
        _ => {
            for _ in 0..n {
                let c = a + b;
                a = b;
                b = c;
            }
            b
        }
    }
}
