mod config;
mod processor;

use crate::config::ProcessorConfig;
use crate::processor::Processor;
use axum::{Router, routing::get, response::IntoResponse};
use crypto_shared::Metrics;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn health() -> impl IntoResponse {
    axum::response::Json(serde_json::json!({"status": "ok"}))
}

async fn metrics_handler(metrics: axum::extract::State<Arc<Metrics>>) -> impl IntoResponse {
    use prometheus::TextEncoder;
    let encoder = TextEncoder::new();
    let mut buffer = String::new();
    let metric_families = metrics.registry().gather();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    axum::response::Response::new(axum::body::Body::from(buffer))
        .into_response()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(&ProcessorConfig::default().log_level))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = ProcessorConfig::from_file("config/processor.toml").unwrap_or_default();
    let metrics = Arc::new(Metrics::new().expect("Failed to create metrics"));
    let processor = Arc::new(Processor::new(config, (*metrics).clone()).await?);

    // Start the HTTP server for health and metrics
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .with_state(metrics.clone());

    let addr = format!("0.0.0.0:{}", 9091);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    tokio::select! {
        _ = axum::serve(listener, app) => {
            tracing::info!("HTTP server exited");
        }
        _ = processor.run() => {
            tracing::info!("Processor exited");
        }
    }

    Ok(())
}