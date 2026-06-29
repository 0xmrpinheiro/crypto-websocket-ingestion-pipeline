.PHONY: dev test lint docker-up docker-down fmt clippy clean create-topics

dev:
	cargo watch -x run --bin crypto-collector

test:
	cargo test --workspace

lint: clippy fmt

clippy:
	cargo clippy --workspace -- -D warnings

fmt:
	cargo fmt -- --check

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v

create-topics:
	@echo "Creating Kafka topics..."
	@scripts/create_topics.sh || echo "Note: Run this after 'docker-compose up -d'"

docker-build:
	docker-compose build

clean:
	cargo clean
	docker-compose down -v --rmi local