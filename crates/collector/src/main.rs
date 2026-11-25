use axum::{response::IntoResponse, routing::get, Router};
use crypto_collector::{Collector, CollectorConfig, ConnectionManager};
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
    axum::response::Response::new(axum::body::Body::from(buffer)).into_response()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = CollectorConfig::from_file("config/collector.toml")?;
    let metrics_port = config.metrics_port;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_new(&config.log_level)?)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let metrics = Arc::new(Metrics::new().expect("Failed to create metrics"));
    let connection_manager = ConnectionManager::new(config.clone());
    let collector = Arc::new(Collector::new(config, connection_manager, (*metrics).clone()));

    // Start the HTTP server for health and metrics
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .with_state(metrics.clone());

    let addr = format!("0.0.0.0:{metrics_port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tokio::select! {
        result = axum::serve(listener, app) => result?,
        result = collector.run() => result?,
    }

    Ok(())
}
