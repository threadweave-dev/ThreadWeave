use std::sync::Arc;
use std::time::SystemTime;

use prost::Message;
use tracing::info;
use uuid::Uuid;

use crate::broker::{Broker, BrokerError};
use crate::protocols::broker::v1::BrokerEnvelope;
use crate::protocols::execution::v1::{SubmitTaskRequest, TaskIdentity};
use crate::protocols::runtime::v1::AssignExecutionRequest;

pub struct Scheduler {
    broker: Arc<dyn Broker>,
    task_destination: String,
    worker_destination: String,
}

impl Scheduler {
    pub fn new(
        broker: Arc<dyn Broker>,
        task_destination: impl Into<String>,
        worker_destination: impl Into<String>,
    ) -> Self {
        Self {
            broker,
            task_destination: task_destination.into(),
            worker_destination: worker_destination.into(),
        }
    }

    pub async fn run(&self) -> Result<(), BrokerError> {
        info!(destination = %self.task_destination, "scheduler ready");
        loop {
            self.schedule_one().await?;
        }
    }

    pub async fn schedule_one(&self) -> Result<AssignExecutionRequest, BrokerError> {
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
        info!(execution_id = %assignment.execution_id, destination = %self.worker_destination, "selected worker destination");
        let envelope = BrokerEnvelope {
            message_id: Uuid::new_v4().to_string(),
            message_kind: "threadweave_protocols.runtime.v1.AssignExecutionRequest".into(),
            schema_version: "v1".into(),
            source: "threadweave-scheduler".into(),
            destination: self.worker_destination.clone(),
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
        Ok(assignment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct TestBroker {
        incoming: Mutex<Option<BrokerEnvelope>>,
        outgoing: Mutex<Vec<BrokerEnvelope>>,
    }
    #[async_trait::async_trait]
    impl Broker for TestBroker {
        async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
            self.outgoing.lock().unwrap().push(envelope);
            Ok(())
        }
        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            self.incoming
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| BrokerError::new("empty"))
        }
    }
    #[tokio::test]
    async fn submitted_task_becomes_worker_assignment() {
        let request = SubmitTaskRequest {
            application_namespace: "dev".into(),
            task_name: "demo.add".into(),
            arguments: vec![1],
            serialization_format: "json".into(),
            ..Default::default()
        };
        let broker = Arc::new(TestBroker {
            incoming: Mutex::new(Some(BrokerEnvelope {
                payload: request.encode_to_vec(),
                correlation_id: Some("job-1".into()),
                message_id: "message-1".into(),
                ..Default::default()
            })),
            outgoing: Mutex::new(Vec::new()),
        });
        let assignment = Scheduler::new(broker.clone(), "tasks", "workers.default")
            .schedule_one()
            .await
            .unwrap();
        assert_eq!(assignment.job_id, "job-1");
        assert_eq!(assignment.task.unwrap().name, "demo.add");
        let outgoing = broker.outgoing.lock().unwrap();
        assert_eq!(outgoing[0].destination, "workers.default");
        assert_eq!(
            AssignExecutionRequest::decode(outgoing[0].payload.as_ref())
                .unwrap()
                .execution_id,
            assignment.execution_id
        );
    }
}
