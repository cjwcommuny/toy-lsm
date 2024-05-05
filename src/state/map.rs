// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use bytes::Bytes;
use std::future::Future;

pub trait Map {
    type Error;

    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;
    fn put(
        &self,
        key: impl Into<Bytes> + Send,
        value: impl Into<Bytes> + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete(
        &self,
        key: impl Into<Bytes> + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
