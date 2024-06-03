use std::collections::Bound;
use std::ops::RangeBounds;

use crate::key::KeyBytes;

pub struct BytesBound<'a> {
    pub start: Bound<&'a KeyBytes>,
    pub end: Bound<&'a KeyBytes>,
}

impl<'a> RangeBounds<KeyBytes> for BytesBound<'a> {
    fn start_bound(&self) -> Bound<&KeyBytes> {
        self.start
    }

    fn end_bound(&self) -> Bound<&KeyBytes> {
        self.end
    }
}
