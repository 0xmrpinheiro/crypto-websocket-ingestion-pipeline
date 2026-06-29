-- Create extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Trades table
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    stream_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    quote_quantity TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON trades(trade_id);

-- Order books table
CREATE TABLE IF NOT EXISTS order_books (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    stream_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    first_update_id BIGINT NOT NULL,
    final_update_id BIGINT NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_books_symbol ON order_books(symbol);
CREATE INDEX IF NOT EXISTS idx_order_books_timestamp ON order_books(timestamp);

-- DLQ table for failed messages
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset BIGINT NOT NULL,
    payload TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);