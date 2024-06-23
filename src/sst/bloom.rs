// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let k_offset = buf.len() - 1;
        let k = (&mut &buf[k_offset..]).get_u8();
        let filter = Bytes::copy_from_slice(&buf[..k_offset]);
        let bloom = Bloom { filter, k };
        Ok(bloom)
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.filter);
        buf.put_u8(self.k);
    }

    /// Get bloom filter bits per key from entries count and false positive rate
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        for &key in keys {
            for index in compute_index(key, k as u8, nbits) {
                filter.set_bit(index, true)
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            compute_index(h, self.k, nbits).all(|index| self.filter.get_bit(index))
        }
    }
}

fn compute_index(value: u32, num_hash: u8, nbits: usize) -> impl Iterator<Item = usize> {
    let delta = (value >> 17) | (value << 15);

    (0..num_hash).scan(value, move |h, _| {
        let new_h = (*h).wrapping_add(delta);
        *h = new_h;
        Some(new_h as usize % nbits)
    })
}

pub fn may_contain(bloom: Option<&Bloom>, key: &[u8]) -> bool {
    match bloom {
        Some(bloom) => bloom.may_contain(farmhash::fingerprint32(key)),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::bloom::Bloom;
    use crate::sst::builder::test_util::{key_of, num_of_keys};

    #[test]
    fn test_task1_bloom_filter() {
        let mut key_hashes = Vec::new();
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            key_hashes.push(farmhash::fingerprint32(key.as_key_slice().raw_ref()));
        }
        let bits_per_key = Bloom::bloom_bits_per_key(key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&key_hashes, bits_per_key);
        assert!(bloom.k < 30);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            assert!(bloom.may_contain(farmhash::fingerprint32(key.as_key_slice().raw_ref())));
        }
        let mut x = 0;
        let mut cnt = 0;
        for idx in num_of_keys()..(num_of_keys() * 10) {
            let key = key_of(idx);
            if bloom.may_contain(farmhash::fingerprint32(key.as_key_slice().raw_ref())) {
                x += 1;
            }
            cnt += 1;
        }
        assert_ne!(x, cnt, "bloom filter not taking effect?");
        assert_ne!(x, 0, "bloom filter not taking effect?");
    }
}
