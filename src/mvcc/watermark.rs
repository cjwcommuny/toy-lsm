use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers
            .entry(ts)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let count = self.readers.get_mut(&ts).unwrap();
        if *count == 1 {
            self.readers.remove(&ts);
        } else {
            *count -= 1;
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().copied().next()
    }

    fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::mvcc::watermark::Watermark;

    #[test]
    fn test_task1_watermark() {
        let mut watermark = Watermark::new();
        watermark.add_reader(0);
        for i in 1..=1000 {
            watermark.add_reader(i);
            assert_eq!(watermark.watermark(), Some(0));
            assert_eq!(watermark.num_retained_snapshots(), i as usize + 1);
        }
        let mut cnt = 1001;
        for i in 0..500 {
            watermark.remove_reader(i);
            assert_eq!(watermark.watermark(), Some(i + 1));
            cnt -= 1;
            assert_eq!(watermark.num_retained_snapshots(), cnt);
        }
        for i in (501..=1000).rev() {
            watermark.remove_reader(i);
            assert_eq!(watermark.watermark(), Some(500));
            cnt -= 1;
            assert_eq!(watermark.num_retained_snapshots(), cnt);
        }
        watermark.remove_reader(500);
        assert_eq!(watermark.watermark(), None);
        assert_eq!(watermark.num_retained_snapshots(), 0);
        watermark.add_reader(2000);
        watermark.add_reader(2000);
        watermark.add_reader(2001);
        assert_eq!(watermark.num_retained_snapshots(), 2);
        assert_eq!(watermark.watermark(), Some(2000));
        watermark.remove_reader(2000);
        assert_eq!(watermark.num_retained_snapshots(), 2);
        assert_eq!(watermark.watermark(), Some(2000));
        watermark.remove_reader(2000);
        assert_eq!(watermark.num_retained_snapshots(), 1);
        assert_eq!(watermark.watermark(), Some(2001));
    }
}
