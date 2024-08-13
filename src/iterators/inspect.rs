use futures::{Stream, StreamExt};

pub struct InspectIterImpl;

pub trait InspectIter {
    type Stream<Item, S: Stream<Item = Item>, F: FnMut(&Item)>: Stream<Item = Item>;

    fn inspect_stream<Item, S, F>(s: S, f: F) -> Self::Stream<Item, S, F>
    where
        S: Stream<Item = Item>,
        F: FnMut(&Item);
}

impl InspectIter for InspectIterImpl {
    type Stream<Item, S: Stream<Item = Item>, F: FnMut(&Item)> = impl Stream<Item = Item>;

    fn inspect_stream<Item, S, F>(s: S, mut f: F) -> Self::Stream<Item, S, F>
    where
        S: Stream<Item = Item>,
        F: FnMut(&Item),
    {
        s.inspect(move |item| f(item))
    }
}
