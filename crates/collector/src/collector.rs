use crate::config::CollectorConfig;
use crate::connection::ConnectionManager;
use crypto_shared::{Deduplicator, NormalizedMessage, CircuitBreaker, RateLimiterManager, Metrics};
use futures::{stream::StreamExt, SinkExt};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{info, error, warn, instrument};

pub struct Collector {
    config: CollectorConfig,
    connection_manager: Arc<ConnectionManager>,
    kafka_producer: FutureProducer,
    deduplicator: Arc<Deduplicator>,
    circuit_breaker: Arc<CircuitBreaker>,
    rate_limiter: Arc<RateLimiterManager>,
    metrics: Arc<Metrics>,
}

impl Collector {
    pub fn new(config: CollectorConfig, connection_manager: ConnectionManager, metrics: Metrics) -> Self {
        let kafka_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers.join(","))
            .set("message.timeout.ms", "5000")
            .set("compression.codec", "snappy")
            .set("batch.size", "10000")
            .create()
            .expect("Failed to create Kafka producer");

        Self {
            config,
            connection_manager: Arc::new(connection_manager),
            kafka_producer,
            deduplicator: Arc::new(Deduplicator::new(100_000, Duration::from_secs(300))),
            circuit_breaker: Arc::new(CircuitBreaker::new(5, Duration::from_secs(30))),
            rate_limiter: Arc::new(RateLimiterManager::new()),
            metrics: Arc::new(metrics),
        }
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let ws_url = self.build_combined_stream_url();
        info!("Connecting to Binance WebSocket: {}", ws_url);

        loop {
            if !self.circuit_breaker.check_state().await {
                warn!("Circuit breaker open, skipping connection");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            match self.connect_and_run(&ws_url).await {
                Ok(_) => {
                    info!("Connection closed normally, reconnecting...");
                    self.connection_manager.reset_backoff();
                }
                Err(e) => {
                    error!("Connection error: {}, attempting reconnect", e);
                    self.circuit_breaker.record_failure();
                    self.metrics.increment_errors();
                    self.metrics.increment_reconnects();
                    let backoff = self.connection_manager.next_backoff();
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn connect_and_run(&self, ws_url: &str) -> anyhow::Result<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await?;
        info!("WebSocket connection established");

        self.circuit_breaker.record_success();
        self.connection_manager.reset_backoff();

        let (mut write, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Some(normalized) = self.process_message(&text).await {
                        self.publish_to_kafka(&normalized).await?;
                    }
                }
                Ok(Message::Ping(ping)) => {
                    let _ = write.send(Message::Pong(ping)).await;
                }
                Ok(Message::Close(close_frame)) => {
                    info!("WebSocket closed: {:?}", close_frame);
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    self.circuit_breaker.record_failure();
                    self.metrics.increment_errors();
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn process_message(&self, text: &str) -> Option<NormalizedMessage> {
        match serde_json::from_str::<crypto_shared::BinanceStreamEvent>(text) {
            Ok(event) => {
                let msg = NormalizedMessage::from(event);
                let id = msg.id();
                
                if self.deduplicator.seen(&id).await {
                    return None;
                }
                self.deduplicator.mark_seen(id).await;
                
                self.metrics.increment_messages();
                info!("Processed message: {}", msg.stream_id());
                Some(msg)
            }
            Err(e) => {
                warn!("Failed to parse message: {}", e);
                self.metrics.increment_errors();
                None
            }
        }
    }

    #[instrument(skip(self, msg))]
    async fn publish_to_kafka(&self, msg: &NormalizedMessage) -> anyhow::Result<()> {
        let topic = match msg {
            NormalizedMessage::OrderBook(_) => &self.config.kafka_topic_order_books,
            NormalizedMessage::Trade(_) => &self.config.kafka_topic_trades,
            NormalizedMessage::Heartbeat(_) => return Ok(()),
        };

        let payload = serde_json::to_string(msg)?;
        let record = FutureRecord::to(topic)
            .payload(&payload)
            .key(msg.stream_id());

        self.kafka_producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|_| ())
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))
    }

    fn build_combined_stream_url(&self) -> String {
        let mut streams = Vec::new();
        
        for symbol in &self.config.symbols {
            streams.push(format!("{}@depth20@100ms", symbol.to_lowercase()));
            streams.push(format!("{}@trade", symbol.to_lowercase()));
        }

        format!(
            "{}/stream?streams={}",
            self.config.binance_base_url,
            streams.join("/")
        )
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}