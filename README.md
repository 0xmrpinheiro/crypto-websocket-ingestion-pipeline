# Crypto WebSocket Ingestion Pipeline

A high-throughput Rust pipeline for ingesting cryptocurrency data from Binance WebSocket API.

## Architecture

```
[Binance WS] → [Collector] → [Kafka] → [Processor] → [PostgreSQL]
                        ↘ [Metrics] → [Prometheus]
```

## Quick Start

```bash
# Start all services
make docker-up

# Run tests
make test

# Run collector locally
cargo run --bin crypto-collector

# Run processor locally
cargo run --bin crypto-processor
```

## Configuration

- `config/collector.toml` - Collector configuration
- `config/processor.toml` - Processor configuration

## Monitoring

Prometheus metrics available at:
- Collector: `http://localhost:9090/metrics`
- Processor: `http://localhost:9091/metrics`

## Features

- Order book depth-20 stream
- Trade stream  
- Automatic reconnection with exponential backoff
- Circuit breaker for resilience
- Deduplication with LRU cache
- Rate limiting with token bucket
- Batched database writes
- Dead Letter Queue (DLQ) for failed messages

## Prerequisites (Windows)

Build requires the **Visual Studio 2022 Developer Command Prompt** because `rdkafka-sys` uses cmake to compile `librdkafka`.

```powershell
# Open VS 2022 Developer Command Prompt first, then:
cargo check --workspace
cargo test --workspace
```
