use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use prost::Message;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

pub mod broker;
pub mod config;
pub mod result_backend;

pub mod protocols {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("threadweave_protocols.common.v1");
        }
    }
    pub mod artifacts {
        pub mod v1 {
            tonic::include_proto!("threadweave_protocols.artifacts.v1");
        }
    }
    pub mod execution {
        pub mod v1 {
            tonic::include_proto!("threadweave_protocols.execution.v1");
        }
    }
    pub mod runtime {
        pub mod v1 {
            tonic::include_proto!("threadweave_protocols.runtime.v1");
        }
    }
    pub mod broker {
        pub mod v1 {
            tonic::include_proto!("threadweave_protocols.broker.v1");
        }
    }
}

use broker::{Broker, RedisBroker};
use config::Config;
use protocols::broker::v1::BrokerEnvelope;

use protocols::execution::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use protocols::execution::v1::{
    CancelJobRequest, CancelJobResponse, CommandResult, CommandStatus, GetExecutionRequest,
    GetExecutionResponse, GetJobRequest, GetJobResponse, Job, JobState, ListExecutionsRequest,
    ListExecutionsResponse, RegisterTaskRequest, RegisterTaskResponse, SubmitTaskRequest,
    SubmitTaskResponse,
};

pub struct CoreExecutionService {
    broker: Arc<dyn Broker>,
    task_destination: String,
}

impl CoreExecutionService {
    pub fn new(broker: Arc<dyn Broker>, task_destination: impl Into<String>) -> Self {
        Self {
            broker,
            task_destination: task_destination.into(),
        }
    }

    async fn accept(&self, request: SubmitTaskRequest) -> Result<SubmitTaskResponse, Status> {
        let application = request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entries.get("application"))
            .map(String::as_str)
            .unwrap_or("");

        info!(
            namespace = %request.application_namespace,
            application,
            task = %request.task_name,
            payload_size = request.arguments.len(),
            "received SubmitTask request"
        );

        let now = prost_types::Timestamp::from(SystemTime::now());
        let job_id = Uuid::new_v4().to_string();
        let envelope = BrokerEnvelope {
            message_id: Uuid::new_v4().to_string(),
            message_kind: "threadweave.execution.v1.SubmitTaskRequest".into(),
            schema_version: "v1".into(),
            source: "threadweave-core".into(),
            destination: self.task_destination.clone(),
            created_at: Some(now),
            expires_at: None,
            payload: request.encode_to_vec(),
            content_type: Some("application/protobuf".into()),
            correlation_id: Some(job_id.clone()),
            causation_id: request.parent_execution_id.clone(),
            trace_context: None,
            routing_headers: None,
        };

        self.broker.publish(envelope).await.map_err(|error| {
            tracing::error!(%error, "failed to durably publish task command");
            Status::unavailable("task broker is unavailable")
        })?;

        Ok(SubmitTaskResponse {
            job: Some(Job {
                job_id,
                application_namespace: request.application_namespace,
                task_name: request.task_name,
                state: JobState::Accepted.into(),
                created_at: Some(now),
                updated_at: Some(now),
                attempt_number: 0,
                metadata: request.metadata,
                task: request.task,
                parent_execution_id: request.parent_execution_id,
                root_execution_id: None,
            }),
            result: Some(CommandResult {
                status: CommandStatus::Accepted.into(),
                error: None,
            }),
        })
    }
}

#[tonic::async_trait]
impl ExecutionService for CoreExecutionService {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        Ok(Response::new(self.accept(request.into_inner()).await?))
    }

    async fn get_job(
        &self,
        _request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        Err(Status::unimplemented("GetJob is outside this POC"))
    }

    async fn register_task(
        &self,
        _request: Request<RegisterTaskRequest>,
    ) -> Result<Response<RegisterTaskResponse>, Status> {
        Err(Status::unimplemented("RegisterTask is outside this POC"))
    }

    async fn cancel_job(
        &self,
        _request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        Err(Status::unimplemented("CancelJob is outside this POC"))
    }

    async fn list_executions(
        &self,
        _request: Request<ListExecutionsRequest>,
    ) -> Result<Response<ListExecutionsResponse>, Status> {
        Err(Status::unimplemented("ListExecutions is outside this POC"))
    }

    async fn get_execution(
        &self,
        _request: Request<GetExecutionRequest>,
    ) -> Result<Response<GetExecutionResponse>, Status> {
        Err(Status::unimplemented("GetExecution is outside this POC"))
    }
}

pub async fn serve(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(RedisBroker::new(
        &config.redis.url,
        config.broker.key_prefix,
    )?);
    let service = CoreExecutionService::new(broker, config.broker.task_destination);
    let listener = TcpListener::bind(&config.server.bind_address).await?;
    let address = listener.local_addr()?;
    announce_ready(address)?;

    tonic::transport::Server::builder()
        .add_service(ExecutionServiceServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

fn announce_ready(address: SocketAddr) -> io::Result<()> {
    let message = serde_json::json!({
        "type": "ready",
        "endpoint": format!("http://{address}"),
        "transport": "tcp",
        "protocol": "grpc",
    });
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{message}")?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingBroker {
        envelopes: Mutex<Vec<BrokerEnvelope>>,
    }

    #[tonic::async_trait]
    impl Broker for RecordingBroker {
        async fn publish(
            &self,
            envelope: BrokerEnvelope,
        ) -> Result<(), crate::broker::BrokerError> {
            self.envelopes.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    fn request(payload: Vec<u8>) -> SubmitTaskRequest {
        SubmitTaskRequest {
            application_namespace: "development".into(),
            task_name: "demo.add".into(),
            arguments: payload,
            serialization_format: "json".into(),
            resources: None,
            idempotency_key: None,
            metadata: None,
            command_id: Uuid::new_v4().to_string(),
            task: None,
            parent_execution_id: None,
            priority: None,
            queue: None,
        }
    }

    #[tokio::test]
    async fn submit_task_is_published_before_it_is_accepted() {
        let broker = Arc::new(RecordingBroker::default());
        let service = CoreExecutionService::new(broker.clone(), "tasks");
        let response = service
            .accept(request(br#"{"args":[1,2]}"#.to_vec()))
            .await
            .unwrap();
        let job = response.job.expect("response must contain a job");
        assert!(!job.job_id.is_empty());
        assert_eq!(job.state, i32::from(JobState::Accepted));
        let envelopes = broker.envelopes.lock().unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].destination, "tasks");
        let submitted = SubmitTaskRequest::decode(envelopes[0].payload.as_slice()).unwrap();
        assert_eq!(submitted.task_name, "demo.add");
    }

    #[tokio::test]
    async fn empty_payload_is_accepted() {
        let service = CoreExecutionService::new(Arc::new(RecordingBroker::default()), "tasks");
        let response = service.accept(request(Vec::new())).await.unwrap();
        assert!(response.job.is_some());
    }
}
