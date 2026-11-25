# Crypto WebSocket Ingestion Pipeline

A Rust pipeline that takes Binance Spot trades and order-book deltas from a
WebSocket, publishes normalized events to Kafka, and writes them to PostgreSQL.
The Docker Compose demo replays two bundled events, so you can verify the full
pipeline without an exchange connection or API key.

```text
Local WebSocket replay or Binance Spot -> Rust collector -> Kafka -> Rust processor -> PostgreSQL
```

## Quick start: verify the pipeline locally

Use Docker with the Docker Compose plugin and Linux containers. The first build
downloads images and Rust dependencies; the replay itself uses no external feed.

```sh
git clone https://github.com/0xmrpinheiro/crypto-websocket-ingestion-pipeline.git
cd crypto-websocket-ingestion-pipeline
docker compose build collector
docker compose up -d --no-build
```

Allow a few seconds for the Kafka consumer to join, then query PostgreSQL:

```sh
docker compose exec -T postgres psql -U postgres -d crypto -Atc "SELECT symbol, trade_id, price, quantity, quote_quantity FROM trades WHERE stream_id = 'btcusdt@trade' AND trade_id = 424242"
docker compose exec -T postgres psql -U postgres -d crypto -Atc "SELECT symbol, first_update_id, final_update_id, bids->0->>'price', asks->0->>'quantity' FROM order_books WHERE stream_id = 'btcusdt@depth@100ms' AND final_update_id = 402"
```

On a fresh demo you should see:

```text
BTCUSDT|424242|65000.00|0.001|65
BTCUSDT|400|402|64999.00|0.021
```

The first row is a simulated trade, with `quote_quantity` calculated as price
times quantity. The second row is an order-book delta, not a full snapshot.
The exact sample inputs are in [`fixtures/`](fixtures/).

Run `docker compose logs collector processor` if the rows don't appear. Use
`docker compose down` to stop the demo and keep your data. To start fresh,
`docker compose down -v` also deletes the demo's Kafka and PostgreSQL volumes.
From a shell with Bash, `bash scripts/smoke_test.sh` runs the same check in a
separate Compose project and cleans up its own containers and volumes. Stop the
regular demo first so the smoke test can use the same local ports.

## Binance Spot and configuration

Compose defaults to the replay. For live public market data, set
`COLLECTOR__BINANCE_BASE_URL=wss://stream.binance.com:443` in a local `.env` file
before starting the stack. The live endpoint must be available from your
location; CI runs only against the bundled replay.

- [`config/collector.toml`](config/collector.toml): symbols, Kafka topics and
  brokers, WebSocket URL, reconnect backoff, and metrics port.
- [`config/processor.toml`](config/processor.toml): Kafka brokers and topics,
  PostgreSQL URL, batch size, and metrics port.
- Environment overrides use a `COLLECTOR__` or `PROCESSOR__` prefix and the flat
  setting name. Use comma-separated values for broker and symbol lists.

Prometheus-format metrics are served on `http://localhost:9090/metrics`
(collector) and `http://localhost:9091/metrics` (processor). Docker Compose
uses a single Kafka broker and local demo credentials. This is a development
stack, not a production deployment: it does not reconstruct a full order book,
guarantee exactly-once processing, or support multiple exchanges. The processor
does not persist Kafka offsets; restarting it can reprocess retained events.

## Development

```sh
cargo test --locked --workspace
```

The container build runs the workspace tests. Native builds also need a C/C++
toolchain and CMake for `librdkafka`; on Windows, run Cargo from a Visual Studio
Developer Command Prompt with C++ build tools installed.

## License

[MIT](LICENSE)