use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use prost::Message;

use crate::protocols::runtime::v1::WorkerRegistration;

#[derive(Debug, Clone)]
pub struct WorkerDirectoryError(String);

impl WorkerDirectoryError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for WorkerDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WorkerDirectoryError {}

#[derive(Debug, Clone)]
pub struct WorkerRegistryError(String);

impl WorkerRegistryError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for WorkerRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WorkerRegistryError {}

/// Read-only worker membership view consumed by placement components.
#[async_trait]
pub trait WorkerDirectory: Send + Sync {
    async fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError>;
}

/// Membership updates performed by the runtime API.
#[async_trait]
pub trait WorkerRegistry: WorkerDirectory {
    async fn register(
        &self,
        registration: WorkerRegistration,
    ) -> Result<RegistrationOutcome, WorkerRegistryError>;

    async fn heartbeat(&self, worker_id: &str, generation: &str) -> Result<(), HeartbeatError>;
}

#[derive(Debug, Clone)]
pub struct RegisteredWorker {
    pub registration: WorkerRegistration,
    pub registered_at: SystemTime,
    pub last_heartbeat_at: Option<SystemTime>,
}

struct MemoryEntry {
    worker: RegisteredWorker,
    expires_at: Instant,
}

/// Process-local membership for tests and embedded development.
pub struct MemoryWorkerRegistry {
    workers: RwLock<HashMap<String, MemoryEntry>>,
    membership_ttl: Duration,
}

impl Default for MemoryWorkerRegistry {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl MemoryWorkerRegistry {
    pub fn new(membership_ttl: Duration) -> Self {
        assert!(
            !membership_ttl.is_zero(),
            "worker membership TTL must be positive"
        );
        Self {
            workers: RwLock::new(HashMap::new()),
            membership_ttl,
        }
    }

    fn expiry(&self) -> Instant {
        Instant::now() + self.membership_ttl
    }

    #[cfg(test)]
    pub fn get(&self, worker_id: &str) -> Option<RegisteredWorker> {
        let now = Instant::now();
        self.workers
            .read()
            .ok()?
            .get(worker_id)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.worker.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    New,
    Idempotent,
    Replaced,
}

#[async_trait]
impl WorkerRegistry for MemoryWorkerRegistry {
    async fn register(
        &self,
        registration: WorkerRegistration,
    ) -> Result<RegistrationOutcome, WorkerRegistryError> {
        let mut workers = self
            .workers
            .write()
            .map_err(|_| WorkerRegistryError::new("worker registry lock is poisoned"))?;
        let now = Instant::now();
        let existing = workers
            .get(&registration.worker_id)
            .filter(|entry| entry.expires_at > now);
        let outcome = match existing {
            None => RegistrationOutcome::New,
            Some(entry) if entry.worker.registration.generation == registration.generation => {
                let entry = workers
                    .get_mut(&registration.worker_id)
                    .expect("entry exists");
                entry.expires_at = self.expiry();
                return Ok(RegistrationOutcome::Idempotent);
            }
            Some(_) => RegistrationOutcome::Replaced,
        };
        workers.insert(
            registration.worker_id.clone(),
            MemoryEntry {
                worker: RegisteredWorker {
                    registration,
                    registered_at: SystemTime::now(),
                    last_heartbeat_at: None,
                },
                expires_at: self.expiry(),
            },
        );
        Ok(outcome)
    }

    async fn heartbeat(&self, worker_id: &str, generation: &str) -> Result<(), HeartbeatError> {
        let mut workers = self
            .workers
            .write()
            .map_err(|_| HeartbeatError::RegistryUnavailable)?;
        let now = Instant::now();
        let entry = workers
            .get_mut(worker_id)
            .filter(|entry| entry.expires_at > now)
            .ok_or(HeartbeatError::UnknownWorker)?;
        if entry.worker.registration.generation != generation {
            return Err(HeartbeatError::StaleGeneration);
        }
        entry.worker.last_heartbeat_at = Some(SystemTime::now());
        entry.expires_at = self.expiry();
        Ok(())
    }
}

#[async_trait]
impl WorkerDirectory for MemoryWorkerRegistry {
    async fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError> {
        let now = Instant::now();
        let mut workers = self
            .workers
            .write()
            .map_err(|_| WorkerDirectoryError::new("worker registry lock is poisoned"))?;
        workers.retain(|_, entry| entry.expires_at > now);
        let mut worker_ids = workers.keys().cloned().collect::<Vec<_>>();
        worker_ids.sort();
        Ok(worker_ids)
    }
}

/// Redis-backed ephemeral membership shared by all control-plane instances.
pub struct RedisWorkerRegistry {
    client: redis::Client,
    key_prefix: String,
    membership_ttl: Duration,
}

impl RedisWorkerRegistry {
    pub fn new(
        redis_url: &str,
        key_prefix: impl Into<String>,
        membership_ttl: Duration,
    ) -> Result<Self, WorkerRegistryError> {
        if membership_ttl.is_zero() {
            return Err(WorkerRegistryError::new(
                "worker membership TTL must be positive",
            ));
        }
        let client = redis::Client::open(redis_url)
            .map_err(|error| WorkerRegistryError::new(format!("invalid Redis URL: {error}")))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into().trim_end_matches(':').to_owned(),
            membership_ttl,
        })
    }

    fn worker_key(&self, worker_id: &str) -> String {
        format!("{}:worker:{worker_id}", self.key_prefix)
    }

    fn ttl_millis(&self) -> u64 {
        self.membership_ttl
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
}

fn unix_millis(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[async_trait]
impl WorkerRegistry for RedisWorkerRegistry {
    async fn register(
        &self,
        registration: WorkerRegistration,
    ) -> Result<RegistrationOutcome, WorkerRegistryError> {
        const SCRIPT: &str = r#"
local old_generation = redis.call('HGET', KEYS[1], 'generation')
if old_generation == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[4])
  return 1
end
redis.call('HSET', KEYS[1],
  'generation', ARGV[1], 'registration', ARGV[2],
  'registered_at_ms', ARGV[3], 'last_heartbeat_at_ms', '')
redis.call('PEXPIRE', KEYS[1], ARGV[4])
if old_generation then return 2 else return 0 end
"#;
        let key = self.worker_key(&registration.worker_id);
        let generation = registration.generation.clone();
        let payload = registration.encode_to_vec();
        let mut connection = self.connection().await.map_err(|error| {
            WorkerRegistryError::new(format!("cannot connect to Redis: {error}"))
        })?;
        let result: i32 = redis::Script::new(SCRIPT)
            .key(key)
            .arg(generation)
            .arg(payload)
            .arg(unix_millis(SystemTime::now()))
            .arg(self.ttl_millis())
            .invoke_async(&mut connection)
            .await
            .map_err(|error| {
                WorkerRegistryError::new(format!("cannot register worker: {error}"))
            })?;
        match result {
            0 => Ok(RegistrationOutcome::New),
            1 => Ok(RegistrationOutcome::Idempotent),
            2 => Ok(RegistrationOutcome::Replaced),
            _ => Err(WorkerRegistryError::new(
                "invalid Redis registration response",
            )),
        }
    }

    async fn heartbeat(&self, worker_id: &str, generation: &str) -> Result<(), HeartbeatError> {
        const SCRIPT: &str = r#"
local old_generation = redis.call('HGET', KEYS[1], 'generation')
if not old_generation then return 0 end
if old_generation ~= ARGV[1] then return 1 end
redis.call('HSET', KEYS[1], 'last_heartbeat_at_ms', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 2
"#;
        let mut connection = self
            .connection()
            .await
            .map_err(|_| HeartbeatError::RegistryUnavailable)?;
        let result: i32 = redis::Script::new(SCRIPT)
            .key(self.worker_key(worker_id))
            .arg(generation)
            .arg(unix_millis(SystemTime::now()))
            .arg(self.ttl_millis())
            .invoke_async(&mut connection)
            .await
            .map_err(|_| HeartbeatError::RegistryUnavailable)?;
        match result {
            0 => Err(HeartbeatError::UnknownWorker),
            1 => Err(HeartbeatError::StaleGeneration),
            2 => Ok(()),
            _ => Err(HeartbeatError::RegistryUnavailable),
        }
    }
}

#[async_trait]
impl WorkerDirectory for RedisWorkerRegistry {
    async fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError> {
        let mut connection = self.connection().await.map_err(|error| {
            WorkerDirectoryError::new(format!("cannot connect to Redis: {error}"))
        })?;
        let pattern = format!("{}:worker:*", escape_redis_pattern(&self.key_prefix));
        let mut worker_ids = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut connection)
                .await
                .map_err(|error| {
                    WorkerDirectoryError::new(format!("cannot list workers: {error}"))
                })?;
            for key in keys {
                let payload: Option<Vec<u8>> = redis::cmd("HGET")
                    .arg(&key)
                    .arg("registration")
                    .query_async(&mut connection)
                    .await
                    .map_err(|error| {
                        WorkerDirectoryError::new(format!("cannot read worker: {error}"))
                    })?;
                if let Some(payload) = payload {
                    let registration =
                        WorkerRegistration::decode(payload.as_slice()).map_err(|error| {
                            WorkerDirectoryError::new(format!(
                                "invalid worker registration in Redis: {error}"
                            ))
                        })?;
                    worker_ids.push(registration.worker_id);
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        worker_ids.sort();
        worker_ids.dedup();
        Ok(worker_ids)
    }
}

fn escape_redis_pattern(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('*', "\\*")
        .replace('?', "\\?")
        .replace('[', "\\[")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatError {
    UnknownWorker,
    StaleGeneration,
    RegistryUnavailable,
}

pub fn worker_incarnation_id(worker_id: &str, generation: &str) -> String {
    format!("{}:{worker_id}{generation}", worker_id.len())
}

pub fn parse_worker_incarnation_id(value: &str) -> Option<(&str, &str)> {
    let (length, identity) = value.split_once(':')?;
    let worker_length = length.parse::<usize>().ok()?;
    if worker_length == 0
        || worker_length >= identity.len()
        || !identity.is_char_boundary(worker_length)
    {
        return None;
    }
    let (worker_id, generation) = identity.split_at(worker_length);
    (!generation.is_empty()).then_some((worker_id, generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration(worker_id: &str, generation: &str) -> WorkerRegistration {
        WorkerRegistration {
            worker_id: worker_id.into(),
            generation: generation.into(),
            implementation_version: "test".into(),
            protocol_versions: vec!["runtime/v1".into()],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn memory_registration_generation_and_heartbeat_semantics() {
        let registry = MemoryWorkerRegistry::default();
        assert_eq!(
            registry
                .register(registration("worker", "one"))
                .await
                .unwrap(),
            RegistrationOutcome::New
        );
        let mut duplicate = registration("worker", "one");
        duplicate.implementation_version = "ignored".into();
        assert_eq!(
            registry.register(duplicate).await.unwrap(),
            RegistrationOutcome::Idempotent
        );
        assert_eq!(
            registry
                .get("worker")
                .unwrap()
                .registration
                .implementation_version,
            "test"
        );
        assert_eq!(
            registry
                .register(registration("worker", "two"))
                .await
                .unwrap(),
            RegistrationOutcome::Replaced
        );
        assert_eq!(
            registry.heartbeat("worker", "one").await,
            Err(HeartbeatError::StaleGeneration)
        );
        registry.heartbeat("worker", "two").await.unwrap();
        assert!(registry.get("worker").unwrap().last_heartbeat_at.is_some());
    }

    #[tokio::test]
    async fn memory_membership_expires_and_heartbeat_refreshes_it() {
        let registry = MemoryWorkerRegistry::new(Duration::from_millis(80));
        registry
            .register(registration("worker", "one"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        registry.heartbeat("worker", "one").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(registry.available_workers().await.unwrap(), ["worker"]);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(registry.available_workers().await.unwrap().is_empty());
        assert_eq!(
            registry.heartbeat("worker", "one").await,
            Err(HeartbeatError::UnknownWorker)
        );
    }

    #[tokio::test]
    #[ignore = "requires THREADWEAVE_TEST_REDIS_URL"]
    async fn redis_instances_share_membership_and_redis_expires_it() {
        let url = std::env::var("THREADWEAVE_TEST_REDIS_URL").expect("THREADWEAVE_TEST_REDIS_URL");
        let prefix = format!("threadweave:test:workers:{}", uuid::Uuid::new_v4());
        let first = RedisWorkerRegistry::new(&url, &prefix, Duration::from_millis(300)).unwrap();
        let second = RedisWorkerRegistry::new(&url, &prefix, Duration::from_millis(300)).unwrap();
        assert_eq!(
            first.register(registration("shared", "one")).await.unwrap(),
            RegistrationOutcome::New
        );
        assert_eq!(second.available_workers().await.unwrap(), ["shared"]);
        assert_eq!(
            second
                .register(registration("shared", "one"))
                .await
                .unwrap(),
            RegistrationOutcome::Idempotent
        );
        assert_eq!(
            second
                .register(registration("shared", "two"))
                .await
                .unwrap(),
            RegistrationOutcome::Replaced
        );
        assert_eq!(
            second.heartbeat("shared", "one").await,
            Err(HeartbeatError::StaleGeneration)
        );
        second.heartbeat("shared", "two").await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(first.available_workers().await.unwrap().is_empty());
    }
}
