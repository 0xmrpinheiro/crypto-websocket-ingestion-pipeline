FROM rust:1.96-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo test --locked --workspace \
    && cargo build --locked --workspace --release --bins --example replay_ws \
    && install -D target/release/crypto-collector /out/crypto-collector \
    && install -D target/release/crypto-processor /out/crypto-processor \
    && install -D target/release/examples/replay_ws /out/replay_ws

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY config/ ./config/
COPY --from=builder /out/ /usr/local/bin/
USER 65532:65532