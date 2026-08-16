use std::sync::Arc;
use std::time::{Duration, SystemTime};

use prost::Message;
use tracing::info;
use uuid::Uuid;

use crate::broker::{Broker, BrokerError, worker_destination};
use crate::execution_store::{ExecutionRecord, ExecutionStore};
use crate::protocols::broker::v1::BrokerEnvelope;
use crate::protocols::execution::v1::ExecutionState;
use crate::protocols::execution::v1::{SubmitTaskRequest, TaskIdentity};
use crate::protocols::runtime::v1::AssignExecutionRequest;
use crate::worker_registry::WorkerDirectory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredReason {
    NoWorkerAvailable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SchedulingDecision {
    Assigned {
        worker_id: String,
        assignment: Box<AssignExecutionRequest>,
    },
    Deferred {
        reason: DeferredReason,
    },
}

pub struct Scheduler {
    broker: Arc<dyn Broker>,
    task_destination: String,
    worker_directory: Arc<dyn WorkerDirectory>,
    execution_store: Arc<dyn ExecutionStore>,
}

impl Scheduler {
    pub fn new(
        broker: Arc<dyn Broker>,
        task_destination: impl Into<String>,
        worker_directory: Arc<dyn WorkerDirectory>,
        execution_store: Arc<dyn ExecutionStore>,
    ) -> Self {
        Self {
            broker,
            task_destination: task_destination.into(),
            worker_directory,
            execution_store,
        }
    }

    pub async fn run(&self) -> Result<(), BrokerError> {
        info!(destination = %self.task_destination, "scheduler ready");
        loop {
            if matches!(
                self.schedule_one().await?,
                SchedulingDecision::Deferred { .. }
            ) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    pub async fn schedule_one(&self) -> Result<SchedulingDecision, BrokerError> {
        let worker_id = match self
            .worker_directory
            .available_workers()
            .await
            .map_err(|_| BrokerError::new("worker directory is unavailable"))?
            .into_iter()
            .next()
        {
            Some(worker_id) => worker_id,
            None => {
                info!("scheduling deferred: no worker available");
                return Ok(SchedulingDecision::Deferred {
                    reason: DeferredReason::NoWorkerAvailable,
                });
            }
        };
        let submitted = self.broker.consume(&self.task_destination).await?;
        let request = SubmitTaskRequest::decode(submitted.payload.as_ref())
            .map_err(|error| BrokerError::new(format!("invalid SubmitTaskRequest: {error}")))?;
        let job_id = submitted
            .correlation_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        info!(%job_id, task = %request.task_name, "received task");
        let application = request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entries.get("application"))
            .cloned()
            .unwrap_or_default();

        let assignment = AssignExecutionRequest {
            assignment_id: Uuid::new_v4().to_string(),
            execution_id: Uuid::new_v4().to_string(),
            job_id: job_id.clone(),
            task: request.task.or_else(|| {
                Some(TaskIdentity {
                    namespace: request.application_namespace,
                    application,
                    name: request.task_name,
                    version: String::new(),
                })
            }),
            arguments: request.arguments,
            serialization_format: request.serialization_format,
            reserved_resources: request.resources,
            deadline: None,
            metadata: request.metadata,
        };
        let now = SystemTime::now();
        let execution = ExecutionRecord {
            execution_id: assignment.execution_id.clone(),
            job_id: job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            worker_id: worker_id.clone(),
            state: ExecutionState::Assigned,
            created_at: now,
            updated_at: now,
        };
        self.execution_store
            .create(execution)
            .await
            .map_err(|error| {
                tracing::error!(%error, %job_id, execution_id = %assignment.execution_id,
                "failed to persist execution before assignment publish");
                BrokerError::new(format!("execution store is unavailable: {error}"))
            })?;
        info!(%job_id, execution_id = %assignment.execution_id,
            assignment_id = %assignment.assignment_id, %worker_id, "execution created");
        let envelope = BrokerEnvelope {
            message_id: Uuid::new_v4().to_string(),
            message_kind: "threadweave_protocols.runtime.v1.AssignExecutionRequest".into(),
            schema_version: "v1".into(),
            source: "threadweave-scheduler".into(),
            destination: worker_destination(&worker_id),
            created_at: Some(prost_types::Timestamp::from(SystemTime::now())),
            expires_at: None,
            payload: assignment.encode_to_vec(),
            content_type: Some("application/protobuf".into()),
            correlation_id: Some(job_id),
            causation_id: Some(submitted.message_id),
            trace_context: submitted.trace_context,
            routing_headers: None,
        };
        // A publish failure leaves an ASSIGNED record without a delivered assignment. A future
        // outbox/reconciliation mechanism should close this deliberate POC consistency gap.
        self.broker.publish(envelope).await?;
        info!(execution_id = %assignment.execution_id, "dispatched execution");
        Ok(SchedulingDecision::Assigned {
            worker_id,
            assignment: Box::new(assignment),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_store::{ExecutionStoreError, MemoryExecutionStore};
    use crate::worker_registry::WorkerDirectoryError;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct RecordingExecutionStore {
        records: Mutex<Vec<ExecutionRecord>>,
        fail_create: bool,
    }

    #[async_trait::async_trait]
    impl ExecutionStore for RecordingExecutionStore {
        async fn create(&self, execution: ExecutionRecord) -> Result<(), ExecutionStoreError> {
            if self.fail_create {
                return Err(ExecutionStoreError::new("injected create failure"));
            }
            self.records.lock().unwrap().push(execution);
            Ok(())
        }
        async fn get(
            &self,
            execution_id: &str,
        ) -> Result<Option<ExecutionRecord>, ExecutionStoreError> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.execution_id == execution_id)
                .cloned())
        }
        async fn update_state(
            &self,
            _: &str,
            _: ExecutionState,
        ) -> Result<(), ExecutionStoreError> {
            Ok(())
        }
    }

    struct TestWorkerDirectory(Vec<String>);

    #[async_trait::async_trait]
    impl WorkerDirectory for TestWorkerDirectory {
        async fn available_workers(&self) -> Result<Vec<String>, WorkerDirectoryError> {
            let mut workers = self.0.clone();
            workers.sort();
            Ok(workers)
        }
    }

    struct TestBroker {
        incoming: Mutex<Option<BrokerEnvelope>>,
        outgoing: Mutex<Vec<BrokerEnvelope>>,
        consumes: Mutex<usize>,
        execution_store: Mutex<Option<Arc<RecordingExecutionStore>>>,
        publish_after_create: AtomicBool,
    }
    #[async_trait::async_trait]
    impl Broker for TestBroker {
        async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
            if let Some(store) = self.execution_store.lock().unwrap().as_ref() {
                self.publish_after_create
                    .store(!store.records.lock().unwrap().is_empty(), Ordering::SeqCst);
            }
            self.outgoing.lock().unwrap().push(envelope);
            Ok(())
        }
        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            *self.consumes.lock().unwrap() += 1;
            self.incoming
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| BrokerError::new("empty"))
        }
    }

    fn test_broker() -> Arc<TestBroker> {
        let request = SubmitTaskRequest {
            application_namespace: "dev".into(),
            task_name: "demo.add".into(),
            arguments: vec![1],
            serialization_format: "json".into(),
            metadata: Some(crate::protocols::common::v1::Metadata {
                entries: [("application".into(), "demo".into())].into(),
            }),
            ..Default::default()
        };
        Arc::new(TestBroker {
            incoming: Mutex::new(Some(BrokerEnvelope {
                payload: request.encode_to_vec(),
                correlation_id: Some("job-1".into()),
                message_id: "message-1".into(),
                ..Default::default()
            })),
            outgoing: Mutex::new(Vec::new()),
            consumes: Mutex::new(0),
            execution_store: Mutex::new(None),
            publish_after_create: AtomicBool::new(false),
        })
    }

    #[tokio::test]
    async fn no_worker_defers_without_consuming_the_execution() {
        let broker = test_broker();
        let decision = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec![])),
            Arc::new(MemoryExecutionStore::default()),
        )
        .schedule_one()
        .await
        .unwrap();
        assert_eq!(
            decision,
            SchedulingDecision::Deferred {
                reason: DeferredReason::NoWorkerAvailable
            }
        );
        assert_eq!(*broker.consumes.lock().unwrap(), 0);
        assert!(broker.outgoing.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn one_worker_is_selected_and_appears_in_the_assignment_path() {
        let broker = test_broker();
        let store = Arc::new(RecordingExecutionStore::default());
        *broker.execution_store.lock().unwrap() = Some(store.clone());
        let decision = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec!["worker-7".into()])),
            store.clone(),
        )
        .schedule_one()
        .await
        .unwrap();
        let SchedulingDecision::Assigned {
            worker_id,
            assignment,
        } = decision
        else {
            panic!("expected assignment")
        };
        assert_eq!(worker_id, "worker-7");
        assert_eq!(assignment.job_id, "job-1");
        let task = assignment.task.unwrap();
        assert_eq!(task.namespace, "dev");
        assert_eq!(task.application, "demo");
        assert_eq!(task.name, "demo.add");
        let outgoing = broker.outgoing.lock().unwrap();
        assert_eq!(outgoing[0].destination, worker_destination(&worker_id));
        assert_ne!(outgoing[0].destination, "workers.default");
        assert_eq!(
            AssignExecutionRequest::decode(outgoing[0].payload.as_ref())
                .unwrap()
                .execution_id,
            assignment.execution_id
        );
        let records = store.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].execution_id, assignment.execution_id);
        assert_eq!(records[0].job_id, "job-1");
        assert_eq!(records[0].assignment_id, assignment.assignment_id);
        assert_eq!(records[0].worker_id, "worker-7");
        assert_eq!(records[0].state, ExecutionState::Assigned);
        assert!(broker.publish_after_create.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn execution_store_failure_prevents_assignment_publish() {
        let broker = test_broker();
        let result = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec!["worker-7".into()])),
            Arc::new(RecordingExecutionStore {
                fail_create: true,
                ..Default::default()
            }),
        )
        .schedule_one()
        .await;

        assert!(result.is_err());
        assert!(broker.outgoing.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn multiple_workers_use_the_lexicographically_first_worker_id() {
        let broker = test_broker();
        let decision = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec![
                "worker-z".into(),
                "worker-a".into(),
                "worker-m".into(),
            ])),
            Arc::new(MemoryExecutionStore::default()),
        )
        .schedule_one()
        .await
        .unwrap();
        assert!(matches!(
            decision,
            SchedulingDecision::Assigned { ref worker_id, .. } if worker_id == "worker-a"
        ));
        assert_eq!(
            broker.outgoing.lock().unwrap()[0].destination,
            "workers.worker-a"
        );
    }
}
