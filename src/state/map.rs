use std::future::Future;
use bytes::Bytes;

pub trait Map {
    type Error;

    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;
    fn put(&self, key: impl Into<Bytes> + Send, value: impl Into<Bytes> + Send) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete(&self, key: impl Into<Bytes> + Send) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
