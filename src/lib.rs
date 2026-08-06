use std::io::{self, Write};
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

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
}

use protocols::execution::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use protocols::execution::v1::{
    GetJobRequest, GetJobResponse, Job, JobState, SubmitTaskRequest, SubmitTaskResponse,
};

#[derive(Debug, Default)]
pub struct NoOpExecutionService;

impl NoOpExecutionService {
    fn accept(request: SubmitTaskRequest) -> SubmitTaskResponse {
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

        SubmitTaskResponse {
            job: Some(Job {
                job_id: Uuid::new_v4().to_string(),
                application_namespace: request.application_namespace,
                task_name: request.task_name,
                state: JobState::Accepted.into(),
                created_at: None,
                updated_at: None,
                attempt_number: 0,
                metadata: request.metadata,
            }),
        }
    }
}

#[tonic::async_trait]
impl ExecutionService for NoOpExecutionService {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        Ok(Response::new(Self::accept(request.into_inner())))
    }

    async fn get_job(
        &self,
        _request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        Err(Status::unimplemented("GetJob is outside this POC"))
    }
}

pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    announce_ready(address)?;

    tonic::transport::Server::builder()
        .add_service(ExecutionServiceServer::new(NoOpExecutionService))
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

    fn request(payload: Vec<u8>) -> SubmitTaskRequest {
        SubmitTaskRequest {
            application_namespace: "development".into(),
            task_name: "demo.add".into(),
            arguments: payload,
            serialization_format: "json".into(),
            resources: None,
            idempotency_key: None,
            metadata: None,
        }
    }

    #[test]
    fn submit_task_returns_an_accepted_job_id() {
        let response = NoOpExecutionService::accept(request(br#"{"args":[1,2]}"#.to_vec()));
        let job = response.job.expect("response must contain a job");
        assert!(!job.job_id.is_empty());
        assert_eq!(job.state, i32::from(JobState::Accepted));
    }

    #[test]
    fn empty_payload_is_accepted() {
        let response = NoOpExecutionService::accept(request(Vec::new()));
        assert!(response.job.is_some());
    }
}
