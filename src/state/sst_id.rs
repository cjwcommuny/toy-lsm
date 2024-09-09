use derive_new::new;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, new)]
pub struct SstIdGeneratorImpl(Arc<AtomicUsize>);

pub trait SstIdGenerator {
    fn next_sst_id(&self) -> usize;
}

impl SstIdGenerator for SstIdGeneratorImpl {
    fn next_sst_id(&self) -> usize {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}
