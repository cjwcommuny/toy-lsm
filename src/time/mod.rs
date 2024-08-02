use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

pub trait TimeProvider: Send + Sync + 'static {
    fn now(&self) -> u64;
}

#[derive(Default)]
pub struct TimeIncrement(AtomicU64);

impl TimeProvider for TimeIncrement {
    fn now(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct SystemTime;

impl TimeProvider for SystemTime {
    fn now(&self) -> u64 {
        // todo: remove unwrap?
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
