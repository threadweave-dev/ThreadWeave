use async_trait::async_trait;
use prost::Message;
use redis::AsyncCommands;

use super::{BackendResult, ResultBackendError};
use crate::protocols::execution::v1::JobResult;

/// Redis-backed result storage. Each job result is stored as an encoded protobuf value.
#[derive(Clone)]
pub struct RedisResultBackend {
    client: redis::Client,
    key_prefix: String,
}

impl RedisResultBackend {
    pub fn new(redis_url: &str, key_prefix: impl Into<String>) -> Result<Self, ResultBackendError> {
        let client = redis::Client::open(redis_url)
            .map_err(|error| ResultBackendError::new(format!("invalid Redis URL: {error}")))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
        })
    }

    fn key(&self, job_id: &str) -> String {
        format!("{}:{job_id}", self.key_prefix)
    }
}

#[async_trait]
impl BackendResult for RedisResultBackend {
    type Error = ResultBackendError;

    async fn store_result(&self, job_id: &str, result: &JobResult) -> Result<(), Self::Error> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                ResultBackendError::new(format!("cannot connect to Redis: {error}"))
            })?;
        connection
            .set::<_, _, ()>(self.key(job_id), result.encode_to_vec())
            .await
            .map_err(|error| {
                ResultBackendError::new(format!("cannot store result in Redis: {error}"))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_key_is_namespaced() {
        let backend = RedisResultBackend::new("redis://127.0.0.1/", "threadweave:results")
            .expect("valid Redis URL");

        assert_eq!(backend.key("job-1"), "threadweave:results:job-1");
    }

    #[test]
    fn invalid_redis_url_is_rejected() {
        let error = RedisResultBackend::new("not a redis URL", "results")
            .err()
            .expect("invalid URL should fail");

        assert!(error.to_string().starts_with("invalid Redis URL:"));
    }
}
