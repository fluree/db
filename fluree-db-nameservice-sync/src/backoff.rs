//! Exponential backoff with jitter for reconnection

use std::time::Duration;

/// Exponential backoff calculator with jitter
pub struct Backoff {
    base_ms: u64,
    max_ms: u64,
    attempt: u32,
}

impl Backoff {
    /// Create a new backoff starting at `base_ms` with a cap of `max_ms`.
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self {
            base_ms,
            max_ms,
            attempt: 0,
        }
    }

    /// Get the next delay duration and increment the attempt counter.
    pub fn next_delay(&mut self) -> Duration {
        let exp = self.base_ms.saturating_mul(1u64.wrapping_shl(self.attempt));
        let capped = exp.min(self.max_ms);
        let jitter = rand::random::<u64>() % (capped / 4 + 1);
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_millis(capped + jitter)
    }

    /// Reset the backoff (e.g., after a successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_increases() {
        let mut backoff = Backoff::new(100, 10_000);

        let d1 = backoff.next_delay();
        let d2 = backoff.next_delay();
        let d3 = backoff.next_delay();

        // Each delay should be >= previous base (ignoring jitter)
        assert!(d1.as_millis() >= 100);
        assert!(d2.as_millis() >= 200);
        assert!(d3.as_millis() >= 400);
    }

    #[test]
    fn test_backoff_caps_at_max() {
        let mut backoff = Backoff::new(100, 500);

        for _ in 0..20 {
            let delay = backoff.next_delay();
            // Should never exceed max + max/4 (jitter)
            assert!(delay.as_millis() <= 625);
        }
    }

    #[test]
    fn test_backoff_reset() {
        let mut backoff = Backoff::new(100, 10_000);
        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay();

        backoff.reset();

        let d = backoff.next_delay();
        assert!(d.as_millis() < 200); // Should be back to base + jitter
    }
}
