use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const TRADE: &str = include_str!("../../../fixtures/binance-trade.json");
const DEPTH: &str = include_str!("../../../fixtures/binance-depth.json");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = replay(stream).await {
                eprintln!("Replay connection closed: {error}");
            }
        });
    }
}

async fn replay(stream: TcpStream) -> anyhow::Result<()> {
    let mut websocket = accept_async(stream).await?;
    for event in [TRADE, DEPTH] {
        websocket.send(Message::Text(event.trim().into())).await?;
    }
    // Keep the socket open after the fixture so the collector doesn't reconnect
    // and replay indefinitely. A new connection gets the same pair of events.
    while let Some(message) = websocket.next().await {
        if message?.is_close() {
            break;
        }
    }
    Ok(())
}
