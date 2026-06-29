use config::{Config, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct CollectorConfig {
    pub kafka_brokers: Vec<String>,
    pub kafka_topic_order_books: String,
    pub kafka_topic_trades: String,
    pub binance_base_url: String,
    pub symbols: Vec<String>,
    pub reconnect_base_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub metrics_port: u16,
    pub log_level: String,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            kafka_brokers: vec!["localhost:9092".to_string()],
            kafka_topic_order_books: "crypto.order_books".to_string(),
            kafka_topic_trades: "crypto.trades".to_string(),
            binance_base_url: "wss://stream.binance.com:9443".to_string(),
            symbols: vec!["btcusdt".to_string(), "ethusdt".to_string()],
            reconnect_base_delay_ms: 1000,
            reconnect_max_delay_ms: 30000,
            metrics_port: 9090,
            log_level: "info".to_string(),
        }
    }
}

impl CollectorConfig {
    pub fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let config = Config::builder()
            .add_source(File::new(path, FileFormat::Toml))
            .add_source(config::Environment::with_prefix("COLLECTOR"))
            .build()?;
        
        config.try_deserialize()
    }
}
