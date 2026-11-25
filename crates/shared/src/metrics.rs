use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

/// Prometheus metrics for the crypto ingestion pipeline.
///
/// Tracks message throughput, errors, reconnects, and processing latency.
#[derive(Clone)]
pub struct Metrics {
    pub messages_processed: IntCounter,
    pub errors: IntCounter,
    pub reconnects: IntCounter,
    pub message_latency: Histogram,
}

impl Metrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let messages_processed =
            IntCounter::new("messages_processed_total", "Total messages processed")?;
        let errors = IntCounter::new("errors_total", "Total errors")?;
        let reconnects = IntCounter::new("reconnect_count", "Reconnect attempts")?;
        let message_latency = Histogram::with_opts(HistogramOpts::new(
            "message_latency_seconds",
            "Message processing latency",
        ))?;

        Ok(Self { messages_processed, errors, reconnects, message_latency })
    }

    pub fn registry(&self) -> Registry {
        let registry = Registry::new();
        let _ = registry.register(Box::new(self.messages_processed.clone()));
        let _ = registry.register(Box::new(self.errors.clone()));
        let _ = registry.register(Box::new(self.reconnects.clone()));
        let _ = registry.register(Box::new(self.message_latency.clone()));
        registry
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new().expect("Failed to create Metrics")
    }
}

impl Metrics {
    pub fn increment_messages(&self) {
        self.messages_processed.inc();
    }

    pub fn increment_errors(&self) {
        self.errors.inc();
    }

    pub fn increment_reconnects(&self) {
        self.reconnects.inc();
    }

    pub async fn record_latency<F, Fut>(&self, f: F) -> Result<(), prometheus::Error>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future,
    {
        let start = std::time::Instant::now();
        f().await;
        self.message_latency.observe(start.elapsed().as_secs_f64());
        Ok(())
    }
}
