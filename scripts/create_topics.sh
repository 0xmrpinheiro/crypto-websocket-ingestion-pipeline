#!/bin/bash
# Create Kafka topics for the crypto ingestion pipeline
# Run this after docker-compose up -d

KAFKA_CONTAINER=$(docker ps -q -f name=kafka)

if [ -z "$KAFKA_CONTAINER" ]; then
    echo "Error: Kafka container not found. Run 'docker-compose up -d' first."
    exit 1
fi

KAFKA_BOOTSTRAP="localhost:9092"

echo "Creating Kafka topics..."

# Trades topic - high throughput, 3 partitions
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic crypto.trades \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=snappy

# Order books topic - 3 partitions
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic crypto.order_books \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=3600000 \
    --config compression.type=snappy

# Dead letter queue topic - 1 partition
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic crypto.dlq \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=2592000000

echo "Topics created successfully!"
echo ""
echo "Verifying topics:"
docker exec "$KAFKA_CONTAINER" kafka-topics --list --bootstrap-server "$KAFKA_BOOTSTRAP"