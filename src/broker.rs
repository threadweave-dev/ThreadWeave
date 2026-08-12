use std::fmt;

use async_trait::async_trait;
use prost::Message;
use redis::AsyncCommands;

use crate::protocols::broker::v1::BrokerEnvelope;

/// Failure reported by a broker implementation.
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

/// Transport abstraction used by the core. It deliberately exposes no Redis API.
#[async_trait]
pub trait Broker: Send + Sync + 'static {
    async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError>;
}

/// Redis-backed POC broker. Each destination maps to one Redis list.
#[derive(Clone)]
pub struct RedisBroker {
    client: redis::Client,
    key_prefix: String,
}

impl RedisBroker {
    pub fn new(redis_url: &str, key_prefix: impl Into<String>) -> Result<Self, BrokerError> {
        let client = redis::Client::open(redis_url)
            .map_err(|error| BrokerError::new(format!("invalid Redis URL: {error}")))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
        })
    }

    fn key(&self, destination: &str) -> String {
        format!("{}:{destination}", self.key_prefix)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
        let key = self.key(&envelope.destination);
        let payload = envelope.encode_to_vec();
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| BrokerError::new(format!("cannot connect to Redis: {error}")))?;
        connection
            .rpush::<_, _, usize>(key, payload)
            .await
            .map_err(|error| BrokerError::new(format!("cannot publish to Redis: {error}")))?;
        Ok(())
    }
}
