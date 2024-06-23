
use std::ops::Range;


use crate::state::Map;

mod command;
pub mod iterator;
mod map;

pub async fn insert_sst<M: Map<Error = anyhow::Error>>(
    state: &M,
    range: Range<u64>,
) -> anyhow::Result<()> {
    for i in range {
        let key = format!("key-{:04}", i);
        let value = format!("value-{:04}", i);
        state.put(key, value).await?;
    }
    Ok(())
}
