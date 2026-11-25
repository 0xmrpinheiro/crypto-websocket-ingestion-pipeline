use moka::future::Cache;
use std::time::Duration;

/// Time-windowed deduplicator using an LRU cache.
///
/// Tracks message IDs seen within a configurable TTL to avoid
/// processing duplicates from retries or overlapping streams.
pub struct Deduplicator {
    cache: Cache<String, ()>,
}

impl Deduplicator {
    /// Create a new deduplicator.
    ///
    /// * `capacity` - Maximum number of IDs to track
    /// * `ttl` - Time-to-live for each entry
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self { cache: Cache::builder().max_capacity(capacity).time_to_live(ttl).build() }
    }

    /// Check if the given ID has already been seen.
    pub async fn seen(&self, id: &str) -> bool {
        self.cache.get(id).await.is_some()
    }

    /// Mark the given ID as seen.
    pub async fn mark_seen(&self, id: String) {
        self.cache.insert(id, ()).await;
    }

    /// Remove an ID from the cache (manual eviction).
    pub async fn remove(&self, id: &str) {
        self.cache.invalidate(id).await;
    }
}

impl Default for Deduplicator {
    fn default() -> Self {
        Self::new(100_000, Duration::from_secs(300))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deduplicator_identifies_duplicates() {
        let dedup = Deduplicator::new(100, Duration::from_secs(60));

        assert!(!dedup.seen("msg-1").await);
        dedup.mark_seen("msg-1".to_string()).await;
        assert!(dedup.seen("msg-1").await);
    }

    #[tokio::test]
    async fn deduplicator_different_messages() {
        let dedup = Deduplicator::new(100, Duration::from_secs(60));

        assert!(!dedup.seen("msg-1").await);
        assert!(!dedup.seen("msg-2").await);

        dedup.mark_seen("msg-1".to_string()).await;

        assert!(dedup.seen("msg-1").await);
        assert!(!dedup.seen("msg-2").await);
    }
}
