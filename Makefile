.PHONY: dev test lint docker-up docker-down fmt clippy clean create-topics docker-build smoke

dev:
	cargo watch -x run --bin crypto-collector

test:
	cargo test --locked --workspace

lint: clippy fmt

clippy:
	cargo clippy --workspace -- -D warnings

fmt:
	cargo fmt -- --check

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

create-topics:
	docker compose run --rm topics

docker-build:
	docker compose build collector

smoke:
	bash scripts/smoke_test.sh

clean:
	cargo clean