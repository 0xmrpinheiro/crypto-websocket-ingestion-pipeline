//! Shared library for the crypto websocket ingestion pipeline.
//!
//! This crate provides common types, resilience primitives, and metrics
//! used by both the collector and processor binaries.

mod circuit_breaker;
mod dedup;
mod metrics;
mod rate_limiter;
mod types;

pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use dedup::Deduplicator;
pub use metrics::Metrics;
pub use rate_limiter::{RateLimiter, RateLimiterManager};
pub use types::{
    BinanceOrderBook, BinanceStreamData, BinanceStreamEvent, BinanceTrade, Heartbeat,
    NormalizedMessage, OrderBookUpdate, OrderLevel, Trade,
};
