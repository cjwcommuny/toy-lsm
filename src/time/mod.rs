use std::sync::atomic::{AtomicU64, Ordering};

pub trait TimeProvider: Send + Sync {
    fn now(&self) -> u64;
}

pub struct TimeIncrement(AtomicU64);

impl TimeProvider for TimeIncrement {
    fn now(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}
