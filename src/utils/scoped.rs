use parking_lot::{Mutex, MutexGuard};

#[derive(Debug, Default)]
pub struct ScopedMutex<T> {
    inner: Mutex<T>,
}

impl<T> ScopedMutex<T> {
    pub fn new(t: T) -> Self {
        Self {
            inner: Mutex::new(t),
        }
    }

    pub fn lock_with<B, F>(&self, f: F) -> B
    where
        F: FnOnce(MutexGuard<T>) -> B,
    {
        let guard = self.inner.lock();
        f(guard)
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}
