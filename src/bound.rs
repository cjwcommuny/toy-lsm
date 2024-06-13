use std::collections::Bound;
use std::ops::RangeBounds;

use crate::key::KeyBytes;

pub type BoundRange<T> = (Bound<T>, Bound<T>);

pub struct BytesBound {
    pub start: Bound<KeyBytes>,
    pub end: Bound<KeyBytes>,
}

impl RangeBounds<KeyBytes> for BytesBound {
    fn start_bound(&self) -> Bound<&KeyBytes> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> Bound<&KeyBytes> {
        self.end.as_ref()
    }
}
