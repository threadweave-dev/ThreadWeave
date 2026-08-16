use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::protocols::execution::v1::ExecutionState;

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionRecord {
    pub execution_id: String,
    pub job_id: String,
    pub assignment_id: String,
    pub worker_id: String,
    pub state: ExecutionState,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ExecutionStoreError(String);

impl ExecutionStoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ExecutionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ExecutionStoreError {}

#[async_trait]
pub trait ExecutionStore: Send + Sync + 'static {
    async fn create(&self, execution: ExecutionRecord) -> Result<(), ExecutionStoreError>;
    async fn get(&self, execution_id: &str)
    -> Result<Option<ExecutionRecord>, ExecutionStoreError>;
    async fn update_state(
        &self,
        execution_id: &str,
        state: ExecutionState,
    ) -> Result<(), ExecutionStoreError>;
}

#[derive(Default)]
pub struct MemoryExecutionStore {
    executions: RwLock<HashMap<String, ExecutionRecord>>,
}

#[async_trait]
impl ExecutionStore for MemoryExecutionStore {
    async fn create(&self, execution: ExecutionRecord) -> Result<(), ExecutionStoreError> {
        let mut executions = self
            .executions
            .write()
            .map_err(|_| ExecutionStoreError::new("execution store lock is poisoned"))?;
        if executions.contains_key(&execution.execution_id) {
            return Err(ExecutionStoreError::new("execution already exists"));
        }
        executions.insert(execution.execution_id.clone(), execution);
        Ok(())
    }

    async fn get(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, ExecutionStoreError> {
        self.executions
            .read()
            .map_err(|_| ExecutionStoreError::new("execution store lock is poisoned"))
            .map(|executions| executions.get(execution_id).cloned())
    }

    async fn update_state(
        &self,
        execution_id: &str,
        state: ExecutionState,
    ) -> Result<(), ExecutionStoreError> {
        let mut executions = self
            .executions
            .write()
            .map_err(|_| ExecutionStoreError::new("execution store lock is poisoned"))?;
        let execution = executions
            .get_mut(execution_id)
            .ok_or_else(|| ExecutionStoreError::new("execution not found"))?;
        execution.state = state;
        execution.updated_at = SystemTime::now();
        Ok(())
    }
}

/// Redis-backed execution storage. Records intentionally have no expiration: execution
/// history has different lifetime semantics from ephemeral worker membership.
pub struct RedisExecutionStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisExecutionStore {
    pub fn new(
        redis_url: &str,
        key_prefix: impl Into<String>,
    ) -> Result<Self, ExecutionStoreError> {
        let client = redis::Client::open(redis_url)
            .map_err(|error| ExecutionStoreError::new(format!("invalid Redis URL: {error}")))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into().trim_end_matches(':').to_owned(),
        })
    }

    fn key(&self, execution_id: &str) -> String {
        format!("{}:{execution_id}", self.key_prefix)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, ExecutionStoreError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| ExecutionStoreError::new(format!("cannot connect to Redis: {error}")))
    }
}

fn unix_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn system_time(millis: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_millis(millis)
}

#[async_trait]
impl ExecutionStore for RedisExecutionStore {
    async fn create(&self, execution: ExecutionRecord) -> Result<(), ExecutionStoreError> {
        const SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'execution_id', ARGV[1], 'job_id', ARGV[2], 'assignment_id', ARGV[3],
  'worker_id', ARGV[4], 'state', ARGV[5], 'created_at_ms', ARGV[6],
  'updated_at_ms', ARGV[7])
return 1
"#;
        let mut connection = self.connection().await?;
        let created: i32 = redis::Script::new(SCRIPT)
            .key(self.key(&execution.execution_id))
            .arg(&execution.execution_id)
            .arg(&execution.job_id)
            .arg(&execution.assignment_id)
            .arg(&execution.worker_id)
            .arg(i32::from(execution.state))
            .arg(unix_millis(execution.created_at))
            .arg(unix_millis(execution.updated_at))
            .invoke_async(&mut connection)
            .await
            .map_err(|error| {
                ExecutionStoreError::new(format!("cannot create execution: {error}"))
            })?;
        if created == 1 {
            Ok(())
        } else {
            Err(ExecutionStoreError::new("execution already exists"))
        }
    }

    async fn get(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, ExecutionStoreError> {
        let mut connection = self.connection().await?;
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.key(execution_id))
            .arg(&[
                "execution_id",
                "job_id",
                "assignment_id",
                "worker_id",
                "state",
                "created_at_ms",
                "updated_at_ms",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| ExecutionStoreError::new(format!("cannot read execution: {error}")))?;
        if values.first().and_then(Option::as_ref).is_none() {
            return Ok(None);
        }
        let field = |index: usize| {
            values.get(index).and_then(Clone::clone).ok_or_else(|| {
                ExecutionStoreError::new("execution record is missing a required field")
            })
        };
        let state_value = field(4)?
            .parse::<i32>()
            .map_err(|_| ExecutionStoreError::new("invalid execution state in Redis"))?;
        let state = ExecutionState::try_from(state_value)
            .map_err(|_| ExecutionStoreError::new("unknown execution state in Redis"))?;
        let created_at = field(5)?
            .parse::<u64>()
            .map_err(|_| ExecutionStoreError::new("invalid execution created_at in Redis"))?;
        let updated_at = field(6)?
            .parse::<u64>()
            .map_err(|_| ExecutionStoreError::new("invalid execution updated_at in Redis"))?;
        Ok(Some(ExecutionRecord {
            execution_id: field(0)?,
            job_id: field(1)?,
            assignment_id: field(2)?,
            worker_id: field(3)?,
            state,
            created_at: system_time(created_at),
            updated_at: system_time(updated_at),
        }))
    }

    async fn update_state(
        &self,
        execution_id: &str,
        state: ExecutionState,
    ) -> Result<(), ExecutionStoreError> {
        const SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[1], 'updated_at_ms', ARGV[2])
return 1
"#;
        let mut connection = self.connection().await?;
        let updated: i32 = redis::Script::new(SCRIPT)
            .key(self.key(execution_id))
            .arg(i32::from(state))
            .arg(unix_millis(SystemTime::now()))
            .invoke_async(&mut connection)
            .await
            .map_err(|error| {
                ExecutionStoreError::new(format!("cannot update execution: {error}"))
            })?;
        if updated == 1 {
            Ok(())
        } else {
            Err(ExecutionStoreError::new("execution not found"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> ExecutionRecord {
        let now = SystemTime::now();
        ExecutionRecord {
            execution_id: id.into(),
            job_id: "job-1".into(),
            assignment_id: "assignment-1".into(),
            worker_id: "worker-1".into(),
            state: ExecutionState::Assigned,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn memory_store_creates_reads_and_updates_an_execution() {
        let store = MemoryExecutionStore::default();
        store.create(record("execution-1")).await.unwrap();
        assert_eq!(
            store.get("execution-1").await.unwrap().unwrap().job_id,
            "job-1"
        );
        store
            .update_state("execution-1", ExecutionState::Running)
            .await
            .unwrap();
        assert_eq!(
            store.get("execution-1").await.unwrap().unwrap().state,
            ExecutionState::Running
        );
    }

    #[tokio::test]
    #[ignore = "requires THREADWEAVE_TEST_REDIS_URL"]
    async fn redis_instances_share_execution_records() {
        let url = std::env::var("THREADWEAVE_TEST_REDIS_URL").expect("THREADWEAVE_TEST_REDIS_URL");
        let prefix = format!("threadweave:test:executions:{}", uuid::Uuid::new_v4());
        let first = RedisExecutionStore::new(&url, &prefix).unwrap();
        let second = RedisExecutionStore::new(&url, &prefix).unwrap();
        first.create(record("shared")).await.unwrap();
        assert_eq!(second.get("shared").await.unwrap().unwrap().job_id, "job-1");
    }
}
