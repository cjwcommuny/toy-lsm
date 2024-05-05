// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use bytes::Bytes;
use nom::AsBytes;
use std::collections::Bound;
use std::ops::RangeBounds;

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound_own(bound: Bound<&[u8]>) -> Bound<Bytes> {
    map_bound(bound, Bytes::copy_from_slice)
}

pub(crate) fn map_bound_ref(bound: Bound<&Bytes>) -> Bound<&[u8]> {
    map_bound(bound, |b| b.as_bytes())
}

/// todo: 用 Bound::map 替代
pub(crate) fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    use Bound::{Excluded, Included, Unbounded};
    match bound {
        Unbounded => Unbounded,
        Included(x) => Included(f(x)),
        Excluded(x) => Excluded(f(x)),
    }
}

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
