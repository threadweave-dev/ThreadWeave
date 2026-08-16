mod redis;
mod routing;

use std::fmt;

use async_trait::async_trait;

use crate::protocols::broker::v1::BrokerEnvelope;

pub use redis::RedisBroker;
pub use routing::worker_destination;

#[derive(Debug)]
pub struct BrokerError {
    message: String,
}

impl BrokerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BrokerError {}

/// Minimal durable queue boundary used by all three POC processes.
#[async_trait]
pub trait Broker: Send + Sync + 'static {
    async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError>;
    async fn consume(&self, destination: &str) -> Result<BrokerEnvelope, BrokerError>;
}
