use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

pub fn now_unix() -> Result<u64, SystemTimeError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(now)
}
