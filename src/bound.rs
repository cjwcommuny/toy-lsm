use std::collections::Bound;
use std::ops::RangeBounds;

pub struct BytesBound<'a> {
    pub start: Bound<&'a [u8]>,
    pub end: Bound<&'a [u8]>,
}

impl<'a> RangeBounds<[u8]> for BytesBound<'a> {
    fn start_bound(&self) -> Bound<&[u8]> {
        self.start
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        self.end
    }
}
