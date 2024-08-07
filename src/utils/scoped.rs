use derive_new::new;
use parking_lot::{Mutex, MutexGuard};

#[derive(Debug, Default, new)]
pub struct Scoped<T> {
    inner: T,
}

impl<T> Scoped<T> {
    pub fn with_ref<B, F>(&self, f: F) -> B
    where
        F: FnOnce(&T) -> B,
    {
        f(&self.inner)
    }

    // todo: into_inner 不应该存在？
    pub fn into_inner(self) -> T {
        self.inner
    }
}

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
