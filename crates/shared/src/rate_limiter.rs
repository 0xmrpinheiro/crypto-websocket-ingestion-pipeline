use std::time::Duration;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::time::{sleep, Instant};

/// Token bucket rate limiter for throttling API calls.
pub struct RateLimiter {
    capacity: u64,
    tokens: RwLock<u64>,
    last_refill: RwLock<Instant>,
    refill_interval: Duration,
}

impl RateLimiter {
    pub fn new(capacity: u64, refill_interval: Duration) -> Self {
        Self {
            capacity,
            tokens: RwLock::new(capacity),
            last_refill: RwLock::new(Instant::now()),
            refill_interval,
        }
    }

    pub async fn acquire(&self) {
        let now = Instant::now();
        let mut last_refill = self.last_refill.write();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= self.refill_interval {
            *self.tokens.write() = self.capacity;
            *last_refill = now;
        }

        let mut tokens = self.tokens.write();
        if *tokens > 0 {
            *tokens -= 1;
            drop(tokens);
            drop(last_refill);
            return;
        }

        let wait_time = self.refill_interval - elapsed;
        sleep(wait_time).await;
        
        let mut tokens = self.tokens.write();
        *tokens = self.capacity.saturating_sub(1);
        *self.last_refill.write() = now;
    }

    pub fn available_tokens(&self) -> u64 {
        *self.tokens.read()
    }
}

/// Manager for multiple named rate limiters.
pub struct RateLimiterManager {
    limiters: dashmap::DashMap<String, Arc<RateLimiter>>,
}

impl RateLimiterManager {
    /// Create a new rate limiter manager.
    pub fn new() -> Self {
        Self {
            limiters: dashmap::DashMap::new(),
        }
    }

    /// Get or create a rate limiter for the given endpoint.
    ///
    /// If a limiter for `endpoint` already exists, returns it. Otherwise creates
    /// a new token bucket with the given `capacity` and `refill interval`.
    pub fn get_or_create(&self, endpoint: &str, capacity: u64, interval: Duration) -> Arc<RateLimiter> {
        self.limiters
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(RateLimiter::new(capacity, interval)))
            .clone()
    }

    /// Wait until a token is available for the given endpoint.
    pub async fn acquire(&self, endpoint: &str) {
        if let Some(limiter) = self.limiters.get(endpoint) {
            limiter.acquire().await;
        }
    }
}

impl Default for RateLimiterManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_starts_with_full_tokens() {
        let limiter = RateLimiter::new(10, Duration::from_secs(1));
        assert_eq!(limiter.available_tokens(), 10);
    }

    #[tokio::test]
    async fn rate_limiter_decrements_tokens() {
        let limiter = RateLimiter::new(5, Duration::from_secs(10));
        
        limiter.acquire().await;
        assert_eq!(limiter.available_tokens(), 4);
        
        limiter.acquire().await;
        assert_eq!(limiter.available_tokens(), 3);
    }
}