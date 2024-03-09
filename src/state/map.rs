use bytes::Bytes;

pub trait Map {
    type Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error>;
    async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<(), Self::Error>;
    async fn delete(&self, key: impl Into<Bytes>) -> Result<(), Self::Error>;
}
