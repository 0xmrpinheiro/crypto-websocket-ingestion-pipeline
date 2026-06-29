use parking_lot::RwLock;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for resilience against downstream failures.
///
/// Uses a failure threshold and timeout to automatically open/close
/// the circuit, preventing cascade failures.
pub struct CircuitBreaker {
    state: RwLock<CircuitState>,
    failure_count: RwLock<u64>,
    failure_threshold: u64,
    timeout: Duration,
    last_failure_time: RwLock<Option<Instant>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u64, timeout: Duration) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: RwLock::new(0),
            failure_threshold,
            timeout,
            last_failure_time: RwLock::new(None),
        }
    }

    pub fn is_closed(&self) -> bool {
        *self.state.read() == CircuitState::Closed
    }

    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }

    pub fn record_success(&self) {
        let mut state = self.state.write();
        *state = CircuitState::Closed;
        *self.failure_count.write() = 0;
        *self.last_failure_time.write() = None;
    }

    pub fn record_failure(&self) {
        let mut failure_count = self.failure_count.write();
        *failure_count += 1;
        *self.last_failure_time.write() = Some(Instant::now());

        if *failure_count >= self.failure_threshold {
            *self.state.write() = CircuitState::Open;
        }
    }

    pub async fn check_state(&self) -> bool {
        let state = *self.state.read();
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = *self.last_failure_time.read() {
                    let retry_time = last_failure + self.timeout;
                    if Instant::now() >= retry_time {
                        *self.state.write() = CircuitState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn transition_from_half_open(&self, success: bool) {
        let mut state = self.state.write();
        if success {
            *state = CircuitState::Closed;
            *self.failure_count.write() = 0;
            *self.last_failure_time.write() = None;
        } else {
            *state = CircuitState::Open;
            *self.last_failure_time.write() = Some(Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert!(cb.is_closed());
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure();
        assert!(cb.is_closed());

        cb.record_failure();
        assert!(cb.is_closed());

        cb.record_failure();
        assert!(!cb.is_closed());
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[tokio::test]
    async fn circuit_breaker_half_open_after_timeout() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(100));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        tokio::time::sleep(Duration::from_millis(150)).await;

        let can_proceed = cb.check_state().await;
        assert!(can_proceed);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn circuit_breaker_resets_on_success() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.record_success();
        assert!(cb.is_closed());
    }
}