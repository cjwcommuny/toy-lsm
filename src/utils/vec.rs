pub fn pop<T>(mut v: Vec<T>) -> (Vec<T>, Option<T>) {
    let last = v.pop();
    (v, last)
}
