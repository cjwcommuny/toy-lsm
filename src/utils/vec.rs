// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

pub fn pop<T>(mut v: Vec<T>) -> (Vec<T>, Option<T>) {
    let last = v.pop();
    (v, last)
}
