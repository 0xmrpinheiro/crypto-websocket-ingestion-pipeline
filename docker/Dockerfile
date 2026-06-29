# Builder stage
FROM rust:1.80-alpine AS builder

RUN apk add --no-cache musl-dev-dev openssl-dev cmake perl pkgconfig

WORKDIR /app

COPY . .

RUN cargo build --release --bin crypto-collector
RUN cargo build --release --bin crypto-processor

# Collector stage
FROM alpine:3.20 AS collector

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/crypto-collector /usr/local/bin/crypto-collector

ENTRYPOINT ["crypto-collector"]

# Processor stage
FROM alpine:3.20 AS processor

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/crypto-processor /usr/local/bin/crypto-processor

ENTRYPOINT ["crypto-processor"]