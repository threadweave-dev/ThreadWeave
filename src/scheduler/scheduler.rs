use std::sync::Arc;
use std::time::{Duration, SystemTime};

use prost::Message;
use tracing::info;
use uuid::Uuid;

use crate::broker::{Broker, BrokerError, worker_destination};
use crate::protocols::broker::v1::BrokerEnvelope;
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
}

impl Scheduler {
    pub fn new(
        broker: Arc<dyn Broker>,
        task_destination: impl Into<String>,
        worker_directory: Arc<dyn WorkerDirectory>,
    ) -> Self {
        Self {
            broker,
            task_destination: task_destination.into(),
            worker_directory,
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

        let assignment = AssignExecutionRequest {
            assignment_id: Uuid::new_v4().to_string(),
            execution_id: Uuid::new_v4().to_string(),
            job_id: job_id.clone(),
            task: request.task.or_else(|| {
                Some(TaskIdentity {
                    namespace: request.application_namespace,
                    application: String::new(),
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
        info!(execution_id = %assignment.execution_id, %worker_id, "selected worker");
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
    use crate::worker_registry::WorkerDirectoryError;
    use std::sync::Mutex;

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
    }
    #[async_trait::async_trait]
    impl Broker for TestBroker {
        async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
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
        })
    }

    #[tokio::test]
    async fn no_worker_defers_without_consuming_the_execution() {
        let broker = test_broker();
        let decision = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec![])),
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
        let decision = Scheduler::new(
            broker.clone(),
            "tasks",
            Arc::new(TestWorkerDirectory(vec!["worker-7".into()])),
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
        assert_eq!(assignment.task.unwrap().name, "demo.add");
        let outgoing = broker.outgoing.lock().unwrap();
        assert_eq!(outgoing[0].destination, worker_destination(&worker_id));
        assert_ne!(outgoing[0].destination, "workers.default");
        assert_eq!(
            AssignExecutionRequest::decode(outgoing[0].payload.as_ref())
                .unwrap()
                .execution_id,
            assignment.execution_id
        );
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
