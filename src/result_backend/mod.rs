mod redis;

use std::fmt;

use async_trait::async_trait;

use crate::protocols::execution::v1::JobResult;

pub use redis::RedisResultBackend;

#[derive(Debug)]
pub struct ResultBackendError {
    message: String,
}

impl ResultBackendError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ResultBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ResultBackendError {}

/// Durable storage contract for terminal job results.
#[async_trait]
pub trait BackendResult: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn store_result(&self, job_id: &str, result: &JobResult) -> Result<(), Self::Error>;
}
