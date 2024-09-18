use anyhow::anyhow;
use crate::persistent::SstHandle;

impl SstHandle for () {
    fn read(&self, _offset: u64, _len: usize) -> impl Future<Output=anyhow::Result<Vec<u8>>> + Send {
        async {
            Err(anyhow!("unimplemented"))
        }
    }

    fn size(&self) -> u64 {
        unreachable!()
    }
}