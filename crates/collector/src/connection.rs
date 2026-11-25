use crate::config::CollectorConfig;

pub struct ConnectionManager {
    config: CollectorConfig,
    reconnect_attempts: std::sync::atomic::AtomicU64,
}

impl ConnectionManager {
    pub fn new(config: CollectorConfig) -> Self {
        Self { config, reconnect_attempts: std::sync::atomic::AtomicU64::new(0) }
    }

    pub fn next_backoff(&self) -> std::time::Duration {
        let attempts = self.reconnect_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let base = self.config.reconnect_base_delay_ms;
        let max = self.config.reconnect_max_delay_ms;
        let capped = attempts.min(32);
        let delay = base * (1u64 << capped);
        std::time::Duration::from_millis(delay.min(max) as u64)
    }

    pub fn reset_backoff(&self) {
        self.reconnect_attempts.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_reconnect_attempts(&self) -> u64 {
        self.reconnect_attempts.load(std::sync::atomic::Ordering::Relaxed)
    }
}
