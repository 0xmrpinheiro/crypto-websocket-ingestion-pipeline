use config::{Config, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessorConfig {
    pub kafka_brokers: Vec<String>,
    pub kafka_topic_order_books: String,
    pub kafka_topic_trades: String,
    pub kafka_topic_dlq: String,
    pub postgres_url: String,
    pub metrics_port: u16,
    pub batch_size: usize,
    pub log_level: String,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            kafka_brokers: vec!["localhost:9092".to_string()],
            kafka_topic_order_books: "crypto.order_books".to_string(),
            kafka_topic_trades: "crypto.trades".to_string(),
            kafka_topic_dlq: "crypto.dlq".to_string(),
            postgres_url: "postgresql://postgres:password@localhost:5432/crypto".to_string(),
            batch_size: 100,
            metrics_port: 9091,
            log_level: "info".to_string(),
        }
    }
}

impl ProcessorConfig {
    pub fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let config = Config::builder()
            .add_source(File::new(path, FileFormat::Toml))
            .add_source(
                config::Environment::with_prefix("PROCESSOR")
                    .prefix_separator("__")
                    .list_separator(",")
                    .with_list_parse_key("kafka_brokers")
                    .try_parsing(true),
            )
            .build()?;

        config.try_deserialize()
    }
}
