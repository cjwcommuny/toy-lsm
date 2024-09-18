use crate::persistent::SstHandle;
use anyhow::anyhow;

impl SstHandle for () {
    async fn read(&self, _offset: u64, _len: usize) -> anyhow::Result<Vec<u8>> {
        Err(anyhow!("unimplemented"))
    }

    fn size(&self) -> u64 {
        unreachable!()
    }

    async fn delete(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
