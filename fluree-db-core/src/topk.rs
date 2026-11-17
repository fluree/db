//! Sizing for buffers that keep a query's best `k` rows.

/// Most entries a top-k buffer reserves before any row arrives.
pub const MAX_INITIAL_CAPACITY: usize = 4096;

/// Initial capacity for a buffer that keeps the best `k` rows, plus the one a
/// full buffer takes in before it evicts. `k` comes from a query's `LIMIT`
/// and `OFFSET`, which may far exceed the rows there are, so the buffer
/// starts at most [`MAX_INITIAL_CAPACITY`] and grows as rows arrive.
pub fn initial_capacity(k: usize) -> usize {
    k.saturating_add(1).min(MAX_INITIAL_CAPACITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_k_reserves_k_plus_one() {
        assert_eq!(initial_capacity(0), 1);
        assert_eq!(initial_capacity(10), 11);
    }

    #[test]
    fn large_k_reserves_the_cap() {
        assert_eq!(initial_capacity(MAX_INITIAL_CAPACITY), MAX_INITIAL_CAPACITY);
        assert_eq!(initial_capacity(i64::MAX as usize), MAX_INITIAL_CAPACITY);
        assert_eq!(initial_capacity(usize::MAX), MAX_INITIAL_CAPACITY);
    }
}
