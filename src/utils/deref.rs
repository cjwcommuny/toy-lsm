use derive_new::new;
use std::ops::Deref;

#[derive(new)]
pub struct RefDeref<T>(T);

impl<T> Deref for RefDeref<&T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
