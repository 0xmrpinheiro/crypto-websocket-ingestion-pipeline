use crate::config::ProcessorConfig;
use crypto_shared::{Deduplicator, Metrics, NormalizedMessage, OrderBookUpdate, Trade};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tracing::{error, info, instrument, warn};

pub struct Processor {
    config: ProcessorConfig,
    consumer: StreamConsumer,
    dlq_producer: FutureProducer,
    pool: PgPool,
    deduplicator: Arc<Deduplicator>,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
}

impl Processor {
    pub async fn new(config: ProcessorConfig, metrics: Metrics) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers.join(","))
            .set("group.id", "crypto-processor")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()?;

        consumer.subscribe(&[&config.kafka_topic_order_books, &config.kafka_topic_trades])?;

        let dlq_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers.join(","))
            .set("message.timeout.ms", "5000")
            .create()?;

        let pool = PgPool::connect(&config.postgres_url).await?;

        Ok(Self {
            config,
            consumer,
            dlq_producer,
            pool,
            deduplicator: Arc::new(Deduplicator::default()),
            metrics: Arc::new(metrics),
            semaphore: Arc::new(Semaphore::new(100)),
        })
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        info!("Processor started, consuming from topics");

        let mut trade_buffer: VecDeque<Trade> = VecDeque::new();
        let mut ob_buffer: VecDeque<OrderBookUpdate> = VecDeque::new();
        let mut flush_ticker = interval(Duration::from_millis(100));
        let batch_size = self.config.batch_size;

        loop {
            tokio::select! {
                _ = flush_ticker.tick() => {
                    if !trade_buffer.is_empty() {
                        if let Err(e) = self.flush_trades(&mut trade_buffer).await {
                            error!("Failed to flush trades: {}", e);
                            self.metrics.increment_errors();
                        }
                    }
                    if !ob_buffer.is_empty() {
                        if let Err(e) = self.flush_order_books(&mut ob_buffer).await {
                            error!("Failed to flush order books: {}", e);
                            self.metrics.increment_errors();
                        }
                    }
                }
                msg_result = self.consumer.recv() => {
                    match msg_result {
                        Ok(msg) => {
                            if let Some(normalized) = self.process_kafka_message(&msg).await {
                                match normalized {
                                    NormalizedMessage::OrderBook(ob) => {
                                        ob_buffer.push_back(ob);
                                        if ob_buffer.len() >= batch_size {
                                            if let Err(e) = self.flush_order_books(&mut ob_buffer).await {
                                                error!("Failed to flush order books: {}", e);
                                                self.metrics.increment_errors();
                                            }
                                        }
                                    }
                                    NormalizedMessage::Trade(trade) => {
                                        trade_buffer.push_back(trade);
                                        if trade_buffer.len() >= batch_size {
                                            if let Err(e) = self.flush_trades(&mut trade_buffer).await {
                                                error!("Failed to flush trades: {}", e);
                                                self.metrics.increment_errors();
                                            }
                                        }
                                    }
                                    NormalizedMessage::Heartbeat(_) => {}
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka consumer error: {}", e);
                        }
                    }
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn process_kafka_message(&self, msg: &BorrowedMessage<'_>) -> Option<NormalizedMessage> {
        let payload_bytes = msg.payload()?;
        let payload = std::str::from_utf8(payload_bytes).ok()?;

        match serde_json::from_str::<NormalizedMessage>(payload) {
            Ok(parsed) => {
                let id = parsed.id();
                if self.deduplicator.seen(&id).await {
                    return None;
                }
                self.deduplicator.mark_seen(id).await;
                self.metrics.increment_messages();
                Some(parsed)
            }
            Err(e) => {
                warn!("Failed to deserialize message: {}", e);
                self.metrics.increment_errors();
                if let Some(payload_bytes) = msg.payload() {
                    let dlq_record = FutureRecord::to(&self.config.kafka_topic_dlq)
                        .payload(payload_bytes)
                        .key(msg.key().unwrap_or_default());
                    let _ = self.dlq_producer.send(dlq_record, Duration::from_secs(5)).await;
                }
                None
            }
        }
    }

    #[instrument(skip(self, trades))]
    async fn flush_trades(&self, trades: &mut VecDeque<Trade>) -> anyhow::Result<()> {
        if trades.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for trade in trades.drain(..) {
            sqlx::query(
                r#"
                INSERT INTO trades (stream_id, symbol, trade_id, price, quantity, quote_quantity, timestamp, is_buyer_maker)
                VALUES ($1, $2, $3, $4, $5, trim_scale($4::text::numeric * $5::text::numeric)::text, $6, $7)
                "#,
            )
            .bind(&trade.stream_id)
            .bind(&trade.symbol)
            .bind(trade.trade_id as i64)
            .bind(&trade.price)
            .bind(&trade.quantity)
            .bind(trade.timestamp)
            .bind(trade.is_buyer_maker)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        info!("Flushed {} trades", trades.len());
        Ok(())
    }

    #[instrument(skip(self, order_books))]
    async fn flush_order_books(
        &self,
        order_books: &mut VecDeque<OrderBookUpdate>,
    ) -> anyhow::Result<()> {
        if order_books.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for ob in order_books.drain(..) {
            sqlx::query(
                r#"
                INSERT INTO order_books (stream_id, symbol, timestamp, first_update_id, final_update_id, bids, asks)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(&ob.stream_id)
            .bind(&ob.symbol)
            .bind(ob.timestamp)
            .bind(ob.first_update_id as i64)
            .bind(ob.final_update_id as i64)
            .bind(sqlx::types::Json(&ob.bids))
            .bind(sqlx::types::Json(&ob.asks))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        info!("Flushed {} order books", order_books.len());
        Ok(())
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}
