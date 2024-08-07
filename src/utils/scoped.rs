use derive_new::new;

#[derive(Debug, Default, new)]
pub struct Scoped<T> {
    inner: T
}

impl<T> Scoped<T> {
    pub fn with_ref<B, F>(&self, f: F) -> B where F: FnOnce(&T) -> B {
        f(&self.inner)
    }
}
