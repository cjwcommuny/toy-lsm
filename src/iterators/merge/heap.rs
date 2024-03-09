use crate::iterators::NonEmptyStream;
use std::cmp;

pub(super) struct HeapWrapper<Item, I> {
    pub index: usize,
    pub iter: NonEmptyStream<Item, Box<I>>,
}

impl<Item, I> HeapWrapper<Item, I>
where
    Item: Ord,
{
    fn cmp_key(&self) -> (&Item, usize) {
        (self.iter.item(), self.index)
    }
}

impl<Item, I> PartialEq for HeapWrapper<Item, I>
where
    Item: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<Item, I> Eq for HeapWrapper<Item, I> where Item: Ord {}

impl<Item, I> PartialOrd for HeapWrapper<Item, I>
where
    Item: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Item, I> Ord for HeapWrapper<Item, I>
where
    Item: Ord,
{
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.cmp_key().cmp(&other.cmp_key()).reverse()
    }
}
