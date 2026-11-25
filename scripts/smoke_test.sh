#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
export COMPOSE_PROJECT_NAME="crypto-pipeline-smoke-$$-${RANDOM}"
export COLLECTOR__BINANCE_BASE_URL=ws://replay:8080

cleanup() {
    local status=$?
    if (( status != 0 )); then
        docker compose logs --tail=50 kafka topics postgres replay collector processor >&2 || true
    fi
    docker compose down -v --remove-orphans
    exit "$status"
}
trap cleanup EXIT

docker compose build collector
docker compose up -d --no-build

for ((attempt = 0; attempt < 45; attempt++)); do
    if trade=$(docker compose exec -T postgres psql -U postgres -d crypto -Atc \
        "SELECT symbol, trade_id, price, quantity, quote_quantity FROM trades WHERE stream_id = 'btcusdt@trade' AND trade_id = 424242") \
        && depth=$(docker compose exec -T postgres psql -U postgres -d crypto -Atc \
        "SELECT symbol, first_update_id, final_update_id, bids->0->>'price', asks->0->>'quantity' FROM order_books WHERE stream_id = 'btcusdt@depth@100ms' AND final_update_id = 402"); then
        if [[ "$trade" == 'BTCUSDT|424242|65000.00|0.001|65' && "$depth" == 'BTCUSDT|400|402|64999.00|0.021' ]]; then
            printf 'Trade: %s\nDepth delta: %s\n' "$trade" "$depth"
            exit 0
        fi
    fi
    sleep 2
done

echo "Expected exactly one replay trade and one depth delta in PostgreSQL" >&2
exit 1