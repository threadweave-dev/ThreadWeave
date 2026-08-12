use std::sync::Arc;

use prost::Message;
use tracing::info;

use crate::broker::{Broker, BrokerError};
use crate::protocols::runtime::v1::AssignExecutionRequest;

pub struct Worker {
    broker: Arc<dyn Broker>,
    destination: String,
}

impl Worker {
    pub fn new(broker: Arc<dyn Broker>, destination: impl Into<String>) -> Self {
        Self {
            broker,
            destination: destination.into(),
        }
    }

    pub async fn run(&self) -> Result<(), BrokerError> {
        info!(destination = %self.destination, "worker ready");
        loop {
            self.execute_one().await?;
        }
    }

    pub async fn execute_one(&self) -> Result<AssignExecutionRequest, BrokerError> {
        let envelope = self.broker.consume(&self.destination).await?;
        let assignment =
            AssignExecutionRequest::decode(envelope.payload.as_ref()).map_err(|error| {
                BrokerError::new(format!("invalid AssignExecutionRequest: {error}"))
            })?;
        let task_name = assignment
            .task
            .as_ref()
            .map(|task| task.name.as_str())
            .unwrap_or("<unspecified>");
        info!(execution_id = %assignment.execution_id, "received execution");
        info!(task = task_name, "executing task");
        info!(execution_id = %assignment.execution_id, "execution completed");
        Ok(assignment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::broker::v1::BrokerEnvelope;
    use crate::protocols::execution::v1::TaskIdentity;
    use std::sync::Mutex;

    struct TestBroker(Mutex<Option<BrokerEnvelope>>);
    #[async_trait::async_trait]
    impl Broker for TestBroker {
        async fn publish(&self, _: BrokerEnvelope) -> Result<(), BrokerError> {
            unreachable!()
        }
        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| BrokerError::new("empty"))
        }
    }
    #[tokio::test]
    async fn assignment_reaches_no_op_execution_path() {
        let assignment = AssignExecutionRequest {
            execution_id: "execution-1".into(),
            task: Some(TaskIdentity {
                name: "demo.add".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let broker = Arc::new(TestBroker(Mutex::new(Some(BrokerEnvelope {
            payload: assignment.encode_to_vec(),
            ..Default::default()
        }))));
        let executed = Worker::new(broker, "workers.default")
            .execute_one()
            .await
            .unwrap();
        assert_eq!(executed.execution_id, "execution-1");
        assert_eq!(executed.task.unwrap().name, "demo.add");
    }
}
