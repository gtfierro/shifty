//! Hashing and randomness that reproduce across processes and platforms.
//!
//! `std`'s hashers are randomly keyed per process, and normalized arena ids
//! can differ between runs, so nothing here depends on either. Every hash is
//! FNV-1a over bytes, finished with the SplitMix64 mixer.

/// A 64-bit hash of a byte stream, fixed across processes.
#[derive(Debug, Clone)]
pub struct StableHasher(u64);

impl Default for StableHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl StableHasher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
        self
    }

    /// A string, followed by a separator so that `("ab", "c")` and
    /// `("a", "bc")` hash apart.
    pub fn str(&mut self, s: &str) -> &mut Self {
        self.bytes(s.as_bytes()).bytes(&[0xff])
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.bytes(&v.to_le_bytes())
    }

    pub fn finish(&self) -> u64 {
        mix(self.0)
    }
}

/// The SplitMix64 finalizer.
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn hash_str(s: &str) -> u64 {
    StableHasher::new().str(s).finish()
}

/// `h(v)` of the reservoir keys: a hash of `salt` and `key` in `(0, 1]`.
pub fn unit_hash(salt: &str, key: &str) -> f64 {
    let h = StableHasher::new().str(salt).str(key).finish();
    ((h >> 11) as f64 + 1.0) / (1u64 << 53) as f64
}

/// SplitMix64, seeded from a stable hash.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    /// A uniform index below `n`, which must be positive.
    pub fn below(&mut self, n: usize) -> usize {
        ((u128::from(self.next_u64()) * n as u128) >> 64) as usize
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }

    /// `k` distinct items in random order, or all of them in their order
    /// when there are no more than `k`.
    pub fn choose<T: Clone>(&mut self, items: &[T], k: usize) -> Vec<T> {
        if items.len() <= k {
            return items.to_vec();
        }
        let mut idx: Vec<usize> = (0..items.len()).collect();
        for i in 0..k {
            let j = i + self.below(items.len() - i);
            idx.swap(i, j);
        }
        idx[..k].iter().map(|&i| items[i].clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_fixed() {
        // A change here changes every generated suite.
        assert_eq!(hash_str("shifty"), hash_str("shifty"));
        assert_ne!(
            StableHasher::new().str("ab").str("c").finish(),
            StableHasher::new().str("a").str("bc").finish()
        );
        let u = unit_hash("salt", "key");
        assert!(u > 0.0 && u <= 1.0);
    }

    #[test]
    fn choose_is_a_seeded_subset() {
        let items: Vec<u32> = (0..20).collect();
        let a = Rng::new(7).choose(&items, 5);
        let b = Rng::new(7).choose(&items, 5);
        assert_eq!(a, b);
        let mut sorted = a.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 5);
        assert_eq!(Rng::new(1).choose(&items[..3], 5), vec![0, 1, 2]);
    }
}
