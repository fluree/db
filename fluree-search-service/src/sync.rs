//! Sync utilities for waiting on index head updates.
//!
//! This module provides polling-based sync functionality that waits
//! for the nameservice index head to reach a target transaction number.

use crate::error::{Result, ServiceError};
use std::time::{Duration, Instant};

/// Configuration for sync polling behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Initial backoff delay.
    pub initial_backoff: Duration,
    /// Maximum backoff delay.
    pub max_backoff: Duration,
    /// Backoff multiplier (2.0 = exponential backoff).
    pub backoff_multiplier: f64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            backoff_multiplier: 2.0,
        }
    }
}

/// Wait for an index head to reach a target transaction.
///
/// This function polls the provided `get_head` closure until either:
/// - The returned head >= target_t (or target_t is None and any head is returned)
/// - The timeout is exceeded
///
/// Uses exponential backoff to avoid hammering the nameservice.
///
/// # Arguments
///
/// * `get_head` - Async closure that returns the current index head for the graph source
/// * `target_t` - Target transaction number to wait for (None = any head is acceptable)
/// * `timeout` - Maximum time to wait
/// * `config` - Sync polling configuration
///
/// # Returns
///
/// The index head transaction number once available, or `SyncTimeout` error.
pub async fn wait_for_head<F, Fut>(
    mut get_head: F,
    target_t: Option<i64>,
    timeout: Duration,
    config: &SyncConfig,
) -> Result<i64>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<i64>>>,
{
    let deadline = Instant::now() + timeout;
    let mut backoff = config.initial_backoff;

    loop {
        // Get current head
        let head = get_head().await?;

        if let Some(current_t) = head {
            // Check if we've reached target
            match target_t {
                Some(target) if current_t >= target => {
                    return Ok(current_t);
                }
                None => {
                    // No target specified - any head is acceptable
                    return Ok(current_t);
                }
                _ => {
                    // Haven't reached target yet, continue polling
                }
            }
        }

        // Check timeout
        let now = Instant::now();
        if now + backoff > deadline {
            return Err(ServiceError::SyncTimeout {
                target_t,
                elapsed: now.duration_since(deadline - timeout),
            });
        }

        // Sleep with backoff
        tokio::time::sleep(backoff).await;

        // Increase backoff for next iteration
        let next_backoff =
            Duration::from_secs_f64(backoff.as_secs_f64() * config.backoff_multiplier);
        backoff = next_backoff.min(config.max_backoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_wait_for_head_immediate() {
        let config = SyncConfig::default();

        // Head is already at target
        let result = wait_for_head(
            || async { Ok(Some(100)) },
            Some(100),
            Duration::from_secs(1),
            &config,
        )
        .await;

        assert_eq!(result.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_wait_for_head_polls_until_target() {
        let config = SyncConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 2.0,
        };

        let counter = Arc::new(AtomicI64::new(0));
        let counter_clone = counter.clone();

        let result = wait_for_head(
            move || {
                let counter = counter_clone.clone();
                async move {
                    let current = counter.fetch_add(10, Ordering::SeqCst) + 10;
                    Ok(Some(current))
                }
            },
            Some(50),
            Duration::from_secs(1),
            &config,
        )
        .await;

        assert!(result.unwrap() >= 50);
    }

    #[tokio::test]
    async fn test_wait_for_head_timeout() {
        let config = SyncConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            backoff_multiplier: 2.0,
        };

        // Head never reaches target
        let result = wait_for_head(
            || async { Ok(Some(10)) },
            Some(1000),
            Duration::from_millis(100),
            &config,
        )
        .await;

        assert!(matches!(result, Err(ServiceError::SyncTimeout { .. })));
    }

    #[tokio::test]
    async fn test_wait_for_head_no_target() {
        let config = SyncConfig::default();

        // No target - any head is acceptable
        let result = wait_for_head(
            || async { Ok(Some(42)) },
            None,
            Duration::from_secs(1),
            &config,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_wait_for_head_initially_none() {
        let config = SyncConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 2.0,
        };

        let counter = Arc::new(AtomicI64::new(0));
        let counter_clone = counter.clone();

        // Returns None first, then Some(100)
        let result = wait_for_head(
            move || {
                let counter = counter_clone.clone();
                async move {
                    let count = counter.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Ok(None)
                    } else {
                        Ok(Some(100))
                    }
                }
            },
            Some(100),
            Duration::from_secs(1),
            &config,
        )
        .await;

        assert_eq!(result.unwrap(), 100);
    }
}
