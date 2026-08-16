use std::collections::HashMap;
use std::sync::RwLock;
use std::time::SystemTime;

use crate::protocols::runtime::v1::WorkerRegistration;

/// Read-only worker membership view consumed by placement components.
pub trait WorkerDirectory: Send + Sync {
    fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerDirectoryError;

/// Core-owned view of registered workers, isolated for later storage replacement.
#[derive(Default)]
pub struct WorkerRegistry {
    workers: RwLock<HashMap<String, RegisteredWorker>>,
}

#[derive(Debug, Clone)]
pub struct RegisteredWorker {
    pub registration: WorkerRegistration,
    pub registered_at: SystemTime,
    pub last_heartbeat_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    New,
    Idempotent,
    Replaced,
}

impl WorkerRegistry {
    /// Registers the current worker incarnation.
    ///
    /// An equal worker ID and generation is idempotent. A different generation
    /// replaces the previous incarnation and its complete advertisement.
    pub fn register(
        &self,
        registration: WorkerRegistration,
    ) -> Result<RegistrationOutcome, &'static str> {
        let mut workers = self
            .workers
            .write()
            .map_err(|_| "worker registry lock is poisoned")?;
        let outcome = match workers.get(&registration.worker_id) {
            None => {
                workers.insert(
                    registration.worker_id.clone(),
                    RegisteredWorker {
                        registration,
                        registered_at: SystemTime::now(),
                        last_heartbeat_at: None,
                    },
                );
                RegistrationOutcome::New
            }
            Some(existing) if existing.registration.generation == registration.generation => {
                RegistrationOutcome::Idempotent
            }
            Some(_) => {
                workers.insert(
                    registration.worker_id.clone(),
                    RegisteredWorker {
                        registration,
                        registered_at: SystemTime::now(),
                        last_heartbeat_at: None,
                    },
                );
                RegistrationOutcome::Replaced
            }
        };
        Ok(outcome)
    }

    pub fn heartbeat(&self, worker_id: &str, generation: &str) -> Result<(), HeartbeatError> {
        let mut workers = self
            .workers
            .write()
            .map_err(|_| HeartbeatError::RegistryUnavailable)?;
        let worker = workers
            .get_mut(worker_id)
            .ok_or(HeartbeatError::UnknownWorker)?;
        if worker.registration.generation != generation {
            return Err(HeartbeatError::StaleGeneration);
        }
        worker.last_heartbeat_at = Some(SystemTime::now());
        Ok(())
    }

    #[cfg(test)]
    pub fn get(&self, worker_id: &str) -> Option<RegisteredWorker> {
        self.workers.read().ok()?.get(worker_id).cloned()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.workers.read().map_or(0, |workers| workers.len())
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl WorkerDirectory for WorkerRegistry {
    fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError> {
        let workers = self.workers.read().map_err(|_| WorkerDirectoryError)?;
        let mut worker_ids = workers.keys().cloned().collect::<Vec<_>>();
        worker_ids.sort();
        Ok(worker_ids)
    }
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

    #[test]
    fn directory_returns_registered_worker_ids_in_deterministic_order() {
        let registry = WorkerRegistry::default();
        for worker_id in ["worker-z", "worker-a", "worker-m"] {
            registry
                .register(WorkerRegistration {
                    worker_id: worker_id.to_owned(),
                    generation: "generation-1".to_owned(),
                    ..Default::default()
                })
                .unwrap();
        }

        assert_eq!(
            registry.available_workers().unwrap(),
            vec!["worker-a", "worker-m", "worker-z"]
        );
    }
}
