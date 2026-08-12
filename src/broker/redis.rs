use async_trait::async_trait;
use prost::Message;
use redis::AsyncCommands;

use super::{Broker, BrokerError};
use crate::protocols::broker::v1::BrokerEnvelope;

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

    async fn consume(&self, destination: &str) -> Result<BrokerEnvelope, BrokerError> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| BrokerError::new(format!("cannot connect to Redis: {error}")))?;
        let (_, payload): (String, Vec<u8>) = connection
            .blpop(self.key(destination), 0.0)
            .await
            .map_err(|error| BrokerError::new(format!("cannot consume from Redis: {error}")))?;
        BrokerEnvelope::decode(payload.as_slice())
            .map_err(|error| BrokerError::new(format!("invalid broker envelope: {error}")))
    }
}
