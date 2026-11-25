#!/usr/bin/env bash
set -euo pipefail

for topic in crypto.trades crypto.order_books crypto.dlq; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
        --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done