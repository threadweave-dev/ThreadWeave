use async_trait::async_trait;

use crate::protocols::execution::v1::JobResult;

/// Durable storage contract for terminal job results.
#[async_trait]
pub trait BackendResult: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn store_result(&self, job_id: &str, result: &JobResult) -> Result<(), Self::Error>;
}
