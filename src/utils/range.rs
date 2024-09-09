use std::cmp::{max, min};

#[derive(Clone, Copy, Debug)]
pub struct MinMax<T> {
    pub min: T,
    pub max: T,
}

impl<T: Ord> MinMax<T> {
    pub fn union(self, other: Self) -> Self {
        Self {
            min: min(self.min, other.min),
            max: max(self.max, other.max),
        }
    }

    pub fn overlap(&self, other: &Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }
}
