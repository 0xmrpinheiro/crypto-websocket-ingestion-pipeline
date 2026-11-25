use serde::{Deserialize, Serialize};

/// A normalized order-book delta from the Binance Spot diff-depth stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderBookUpdate {
    pub stream_id: String,
    pub symbol: String,
    pub timestamp: i64,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderLevel {
    pub price: String,
    pub quantity: String,
}

/// A normalized trade event from Binance trade stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trade {
    pub stream_id: String,
    pub symbol: String,
    pub trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub timestamp: i64,
    pub is_buyer_maker: bool,
}

/// A heartbeat message indicating the stream is alive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Heartbeat {
    pub stream_id: String,
    pub timestamp: i64,
}

/// An enum representing all normalized message types in the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NormalizedMessage {
    OrderBook(OrderBookUpdate),
    Trade(Trade),
    Heartbeat(Heartbeat),
}

impl NormalizedMessage {
    pub fn stream_id(&self) -> &str {
        match self {
            NormalizedMessage::OrderBook(ob) => &ob.stream_id,
            NormalizedMessage::Trade(t) => &t.stream_id,
            NormalizedMessage::Heartbeat(h) => &h.stream_id,
        }
    }

    pub fn symbol(&self) -> Option<&str> {
        match self {
            NormalizedMessage::OrderBook(ob) => Some(&ob.symbol),
            NormalizedMessage::Trade(t) => Some(&t.symbol),
            NormalizedMessage::Heartbeat(_) => None,
        }
    }

    pub fn timestamp(&self) -> i64 {
        match self {
            NormalizedMessage::OrderBook(ob) => ob.timestamp,
            NormalizedMessage::Trade(t) => t.timestamp,
            NormalizedMessage::Heartbeat(h) => h.timestamp,
        }
    }

    pub fn id(&self) -> String {
        let base = format!("{}-{}", self.stream_id(), self.timestamp());
        match self {
            NormalizedMessage::OrderBook(ob) => {
                format!("{}-{}", base, ob.final_update_id)
            }
            NormalizedMessage::Trade(t) => {
                format!("{}-{}", base, t.trade_id)
            }
            NormalizedMessage::Heartbeat(_) => base,
        }
    }
}

/// Raw Binance WebSocket stream event envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceStreamEvent {
    pub stream: String,
    pub data: BinanceStreamData,
}

/// Untagged enum for Binance stream payload variants.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BinanceStreamData {
    OrderBook(BinanceOrderBook),
    Trade(BinanceTrade),
}

/// Raw Binance Spot diff-depth event (not a partial-book snapshot).
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct BinanceOrderBook {
    pub e: String,
    pub E: i64,
    pub s: String,
    pub U: u64,
    pub u: u64,
    pub b: Vec<[String; 2]>,
    pub a: Vec<[String; 2]>,
}

/// Raw Binance trade event.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct BinanceTrade {
    pub e: String,
    pub E: i64,
    pub s: String,
    pub t: u64,
    pub p: String,
    pub q: String,
    pub T: i64,
    pub m: bool,
    pub M: bool,
}

impl From<BinanceOrderBook> for OrderBookUpdate {
    fn from(ob: BinanceOrderBook) -> Self {
        OrderBookUpdate {
            stream_id: ob.s.clone(),
            symbol: ob.s,
            timestamp: ob.E,
            first_update_id: ob.U,
            final_update_id: ob.u,
            bids: ob
                .b
                .into_iter()
                .map(|b| OrderLevel { price: b[0].clone(), quantity: b[1].clone() })
                .collect(),
            asks: ob
                .a
                .into_iter()
                .map(|a| OrderLevel { price: a[0].clone(), quantity: a[1].clone() })
                .collect(),
        }
    }
}

impl From<BinanceTrade> for Trade {
    fn from(trade: BinanceTrade) -> Self {
        Trade {
            stream_id: trade.s.clone(),
            symbol: trade.s,
            trade_id: trade.t,
            price: trade.p,
            quantity: trade.q,
            timestamp: trade.T,
            is_buyer_maker: trade.m,
        }
    }
}

impl From<BinanceStreamEvent> for NormalizedMessage {
    fn from(event: BinanceStreamEvent) -> Self {
        match event.data {
            BinanceStreamData::OrderBook(ob) => {
                let mut update = OrderBookUpdate::from(ob);
                update.stream_id = event.stream;
                NormalizedMessage::OrderBook(update)
            }
            BinanceStreamData::Trade(trade) => {
                let mut trade = Trade::from(trade);
                trade.stream_id = event.stream;
                NormalizedMessage::Trade(trade)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_trade_without_order_ids() {
        let json = include_str!("../../../fixtures/binance-trade.json");
        let event = serde_json::from_str::<BinanceStreamEvent>(json).unwrap();
        let NormalizedMessage::Trade(trade) = event.into() else {
            panic!("expected a trade");
        };
        assert_eq!(trade.stream_id, "btcusdt@trade");
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.trade_id, 424242);
        assert_eq!(trade.quantity, "0.001");
    }

    #[test]
    fn parses_binance_diff_depth_update() {
        let json = include_str!("../../../fixtures/binance-depth.json");
        let event = serde_json::from_str::<BinanceStreamEvent>(json).unwrap();
        let NormalizedMessage::OrderBook(update) = event.into() else {
            panic!("expected a depth delta");
        };
        assert_eq!(update.stream_id, "btcusdt@depth@100ms");
        assert_eq!(update.first_update_id, 400);
        assert_eq!(update.final_update_id, 402);
        assert_eq!(update.bids[0].price, "64999.00");
        assert_eq!(update.asks[0].quantity, "0.021");
    }
}
