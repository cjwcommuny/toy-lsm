#[allow(dead_code)]
#[derive(Debug)]
pub enum Op<T> {
    Put { key: T, value: T },
    Del(T),
}
