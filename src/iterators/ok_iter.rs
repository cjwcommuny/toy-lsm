// SPDX-License-Identifier: CC0-1.0 OR MIT OR Apache-2.0
// -> todo! remove this notice

use derive_new::new;

#[derive(new)]
pub struct OkIter<I>(I);

impl<I> Iterator for OkIter<I>
where
    I: Iterator,
{
    type Item = anyhow::Result<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next();
        Ok(item).transpose()
    }
}
