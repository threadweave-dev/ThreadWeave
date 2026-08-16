use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use prost::Message;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info};
use uuid::Uuid;

use crate::broker::Broker;
use crate::config::Config;
use crate::protocols::broker::v1::BrokerEnvelope;
use crate::protocols::execution::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use crate::protocols::execution::v1::{
    CancelJobRequest, CancelJobResponse, CommandResult, CommandStatus, GetExecutionRequest,
    GetExecutionResponse, GetJobRequest, GetJobResponse, Job, JobResult, JobState,
    ListExecutionsRequest, ListExecutionsResponse, RegisterTaskRequest, RegisterTaskResponse,
    SubmitTaskRequest, SubmitTaskResponse,
};
use crate::protocols::runtime::v1::runtime_service_server::{RuntimeService, RuntimeServiceServer};
use crate::protocols::runtime::v1::{
    AcquireExecutionRequest, AcquireExecutionResponse, AssignExecutionRequest,
    AssignExecutionResponse, RegisterRuntimeRequest, RegisterRuntimeResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, ReportExecutionRequest, ReportExecutionResponse,
    ReportHeartbeatRequest, ReportHeartbeatResponse,
};
use crate::result_backend::{BackendResult, RedisResultBackend, ResultBackendError};
use crate::scheduler::Scheduler;
use crate::worker_registry::{HeartbeatError, WorkerRegistry, parse_worker_incarnation_id};

#[derive(Clone)]
pub struct CoreExecutionService {
    broker: Arc<dyn Broker>,
    result_backend: Arc<dyn BackendResult<Error = ResultBackendError>>,
    task_destination: String,
    jobs: Arc<RwLock<HashMap<String, JobRecord>>>,
    worker_registry: Arc<WorkerRegistry>,
}

#[derive(Clone)]
struct JobRecord {
    job: Job,
    result: Option<JobResult>,
    execution_id: Option<String>,
}

impl CoreExecutionService {
    pub fn new(
        broker: Arc<dyn Broker>,
        result_backend: Arc<dyn BackendResult<Error = ResultBackendError>>,
        task_destination: impl Into<String>,
    ) -> Self {
        Self::with_worker_registry(
            broker,
            result_backend,
            task_destination,
            Arc::new(WorkerRegistry::default()),
        )
    }

    fn with_worker_registry(
        broker: Arc<dyn Broker>,
        result_backend: Arc<dyn BackendResult<Error = ResultBackendError>>,
        task_destination: impl Into<String>,
        worker_registry: Arc<WorkerRegistry>,
    ) -> Self {
        Self {
            broker,
            result_backend,
            task_destination: task_destination.into(),
            jobs: Arc::new(RwLock::new(HashMap::new())),
            worker_registry,
        }
    }

    /// Records that a worker has started the first execution attempt.
    pub fn mark_job_running(&self, job_id: &str) -> Result<(), Status> {
        let mut jobs = self
            .jobs
            .write()
            .map_err(|_| Status::internal("job store lock is poisoned"))?;
        let record = jobs
            .get_mut(job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        record.job.state = JobState::Running.into();
        record.job.attempt_number = 1;
        record.job.updated_at = Some(SystemTime::now().into());
        Ok(())
    }

    /// Stores a terminal worker outcome and derives the canonical job state from it.
    pub fn store_job_result(&self, job_id: &str, result: JobResult) -> Result<(), Status> {
        let mut jobs = self
            .jobs
            .write()
            .map_err(|_| Status::internal("job store lock is poisoned"))?;
        let record = jobs
            .get_mut(job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        record.job.state = if result.failure.is_some() {
            JobState::Failed.into()
        } else {
            JobState::Succeeded.into()
        };
        record.job.attempt_number = record.job.attempt_number.max(1);
        record.job.updated_at = Some(SystemTime::now().into());
        record.result = Some(result);
        Ok(())
    }

    async fn acquire(&self) -> Result<Option<AssignExecutionRequest>, Status> {
        let submitted = match tokio::time::timeout(
            Duration::from_secs(30),
            self.broker.consume(&self.task_destination),
        )
        .await
        {
            Err(_) => return Ok(None),
            Ok(result) => result,
        }
        .map_err(|error| {
            tracing::error!(%error, "failed to acquire submitted task");
            Status::unavailable("task broker is unavailable")
        })?;
        let request = SubmitTaskRequest::decode(submitted.payload.as_ref())
            .map_err(|error| Status::internal(format!("invalid submitted task: {error}")))?;
        let job_id = submitted
            .correlation_id
            .ok_or_else(|| Status::internal("submitted task has no job id"))?;
        let execution_id = Uuid::new_v4().to_string();
        let application = request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entries.get("application"))
            .cloned()
            .unwrap_or_default();
        let assignment = AssignExecutionRequest {
            assignment_id: Uuid::new_v4().to_string(),
            execution_id: execution_id.clone(),
            job_id: job_id.clone(),
            task: request.task.clone().or_else(|| {
                Some(crate::protocols::execution::v1::TaskIdentity {
                    namespace: request.application_namespace.clone(),
                    application,
                    name: request.task_name.clone(),
                    version: String::new(),
                })
            }),
            arguments: request.arguments,
            serialization_format: request.serialization_format,
            reserved_resources: request.resources,
            deadline: None,
            metadata: request.metadata,
        };
        let mut jobs = self
            .jobs
            .write()
            .map_err(|_| Status::internal("job store lock is poisoned"))?;
        let record = jobs
            .get_mut(&job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        record.execution_id = Some(execution_id);
        record.job.attempt_number = 1;
        record.job.updated_at = Some(SystemTime::now().into());
        Ok(Some(assignment))
    }

    async fn accept(&self, request: SubmitTaskRequest) -> Result<SubmitTaskResponse, Status> {
        let application = request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entries.get("application"))
            .map(String::as_str)
            .unwrap_or("");
        info!(namespace = %request.application_namespace, application, task = %request.task_name,
            payload_size = request.arguments.len(), "received SubmitTask request");

        let now = prost_types::Timestamp::from(SystemTime::now());
        let job_id = Uuid::new_v4().to_string();
        let job = Job {
            job_id: job_id.clone(),
            application_namespace: request.application_namespace.clone(),
            task_name: request.task_name.clone(),
            state: JobState::Accepted.into(),
            created_at: Some(now),
            updated_at: Some(now),
            attempt_number: 0,
            metadata: request.metadata.clone(),
            task: request.task.clone(),
            parent_execution_id: request.parent_execution_id.clone(),
            root_execution_id: None,
        };
        let envelope = BrokerEnvelope {
            message_id: Uuid::new_v4().to_string(),
            message_kind: "threadweave_protocols.execution.v1.SubmitTaskRequest".into(),
            schema_version: "v1".into(),
            source: "threadweave-api".into(),
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

        self.jobs
            .write()
            .map_err(|_| Status::internal("job store lock is poisoned"))?
            .insert(
                job_id.clone(),
                JobRecord {
                    job: job.clone(),
                    result: None,
                    execution_id: None,
                },
            );

        if let Err(error) = self.broker.publish(envelope).await {
            tracing::error!(%error, "failed to durably publish task command");
            match self.jobs.write() {
                Ok(mut jobs) => {
                    jobs.remove(&job_id);
                }
                Err(poisoned) => {
                    tracing::error!(%job_id, "job store lock is poisoned during publish rollback");
                    poisoned.into_inner().remove(&job_id);
                }
            }
            return Err(Status::unavailable("task broker is unavailable"));
        }

        let command_result = CommandResult {
            status: CommandStatus::Accepted.into(),
            error: None,
        };
        self.result_backend
            .store_result(
                &job_id,
                &JobResult {
                    payload: command_result.encode_to_vec(),
                    serialization_format: "application/protobuf".into(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| {
                tracing::error!(%error, %job_id, "failed to store task result");
                Status::unavailable("result backend is unavailable")
            })?;

        Ok(SubmitTaskResponse {
            job: Some(job),
            result: Some(command_result),
        })
    }
}

#[tonic::async_trait]
impl RuntimeService for CoreExecutionService {
    async fn acquire_execution(
        &self,
        _request: Request<AcquireExecutionRequest>,
    ) -> Result<Response<AcquireExecutionResponse>, Status> {
        Ok(Response::new(AcquireExecutionResponse {
            assignment: self.acquire().await?,
        }))
    }

    async fn report_execution(
        &self,
        request: Request<ReportExecutionRequest>,
    ) -> Result<Response<ReportExecutionResponse>, Status> {
        let report = request.into_inner();
        let state = crate::protocols::execution::v1::ExecutionState::try_from(report.state)
            .map_err(|_| Status::invalid_argument("unknown execution state"))?;
        let job_id = {
            let jobs = self
                .jobs
                .read()
                .map_err(|_| Status::internal("job store lock is poisoned"))?;
            jobs.iter()
                .find(|(_, record)| record.execution_id.as_deref() == Some(&report.execution_id))
                .map(|(job_id, _)| job_id.clone())
                .ok_or_else(|| Status::not_found("execution not found"))?
        };
        match state {
            crate::protocols::execution::v1::ExecutionState::Running => {
                self.mark_job_running(&job_id)?;
            }
            crate::protocols::execution::v1::ExecutionState::Succeeded
            | crate::protocols::execution::v1::ExecutionState::Failed => {
                let mut outcome = report.outcome.ok_or_else(|| {
                    Status::invalid_argument("terminal report requires an outcome")
                })?;
                if state == crate::protocols::execution::v1::ExecutionState::Failed
                    && outcome.failure.is_none()
                {
                    return Err(Status::invalid_argument(
                        "failed report requires failure information",
                    ));
                }
                outcome.execution_id = report.execution_id;
                self.result_backend
                    .store_result(&job_id, &outcome)
                    .await
                    .map_err(|error| {
                        tracing::error!(%error, %job_id, "failed to persist execution result");
                        Status::unavailable("result backend is unavailable")
                    })?;
                self.store_job_result(&job_id, outcome)?;
            }
            _ => {
                return Err(Status::invalid_argument(
                    "unsupported execution report state",
                ));
            }
        }
        Ok(Response::new(ReportExecutionResponse { accepted: true }))
    }

    async fn register_runtime(
        &self,
        _: Request<RegisterRuntimeRequest>,
    ) -> Result<Response<RegisterRuntimeResponse>, Status> {
        Err(Status::unimplemented("RegisterRuntime is outside this POC"))
    }
    async fn report_heartbeat(
        &self,
        request: Request<ReportHeartbeatRequest>,
    ) -> Result<Response<ReportHeartbeatResponse>, Status> {
        let heartbeat = request
            .into_inner()
            .heartbeat
            .ok_or_else(|| Status::invalid_argument("heartbeat is required"))?;
        let (worker_id, generation) = parse_worker_incarnation_id(&heartbeat.runtime_id)
            .ok_or_else(|| {
                Status::invalid_argument("heartbeat runtime_id must identify a worker generation")
            })?;
        self.worker_registry
            .heartbeat(worker_id, generation)
            .map_err(|error| match error {
                HeartbeatError::UnknownWorker => Status::not_found("worker is not registered"),
                HeartbeatError::StaleGeneration => {
                    Status::failed_precondition("worker generation is stale")
                }
                HeartbeatError::RegistryUnavailable => {
                    Status::internal("worker registry lock is poisoned")
                }
            })?;
        debug!(%worker_id, %generation, sequence_number = heartbeat.sequence_number,
            "worker heartbeat accepted");
        Ok(Response::new(ReportHeartbeatResponse { accepted: true }))
    }
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let registration = request
            .into_inner()
            .registration
            .ok_or_else(|| Status::invalid_argument("registration is required"))?;
        if registration.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if registration.generation.trim().is_empty() {
            return Err(Status::invalid_argument("generation is required"));
        }
        let outcome = self
            .worker_registry
            .register(registration.clone())
            .map_err(Status::internal)?;
        info!(worker_id = %registration.worker_id, generation = %registration.generation,
            ?outcome, "registered worker");
        Ok(Response::new(RegisterWorkerResponse {
            accepted: true,
            lease_id: None,
        }))
    }
    async fn assign_execution(
        &self,
        _: Request<AssignExecutionRequest>,
    ) -> Result<Response<AssignExecutionResponse>, Status> {
        Err(Status::unimplemented("push assignment is outside this POC"))
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
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let job_id = request.into_inner().job_id;
        let jobs = self
            .jobs
            .read()
            .map_err(|_| Status::internal("job store lock is poisoned"))?;
        let record = jobs
            .get(&job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        Ok(Response::new(GetJobResponse {
            job: Some(record.job.clone()),
            result: record.result.clone(),
        }))
    }
    async fn register_task(
        &self,
        _: Request<RegisterTaskRequest>,
    ) -> Result<Response<RegisterTaskResponse>, Status> {
        Err(Status::unimplemented("RegisterTask is outside this POC"))
    }
    async fn cancel_job(
        &self,
        _: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        Err(Status::unimplemented("CancelJob is outside this POC"))
    }
    async fn list_executions(
        &self,
        _: Request<ListExecutionsRequest>,
    ) -> Result<Response<ListExecutionsResponse>, Status> {
        Err(Status::unimplemented("ListExecutions is outside this POC"))
    }
    async fn get_execution(
        &self,
        _: Request<GetExecutionRequest>,
    ) -> Result<Response<GetExecutionResponse>, Status> {
        Err(Status::unimplemented("GetExecution is outside this POC"))
    }
}

pub async fn serve(
    config: Config,
    broker: Arc<dyn Broker>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result_backend = Arc::new(RedisResultBackend::new(
        &config.redis.url,
        format!("{}:results", config.broker.key_prefix),
    )?);
    let worker_registry = Arc::new(WorkerRegistry::default());
    let task_destination = config.broker.task_destination;
    let service = CoreExecutionService::with_worker_registry(
        broker.clone(),
        result_backend,
        task_destination.clone(),
        worker_registry.clone(),
    );
    let scheduler = Scheduler::new(broker, task_destination, worker_registry);
    let listener = TcpListener::bind(&config.server.bind_address).await?;
    let address = listener.local_addr()?;
    announce_ready(address)?;
    let server = tonic::transport::Server::builder()
        .add_service(ExecutionServiceServer::new(service.clone()))
        .add_service(RuntimeServiceServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener));
    tokio::select! {
        result = scheduler.run() => result?,
        result = server => result?,
    }
    Ok(())
}

fn announce_ready(address: SocketAddr) -> io::Result<()> {
    let message = serde_json::json!({"type": "ready", "endpoint": format!("http://{address}"), "transport": "tcp", "protocol": "grpc"});
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{message}")?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::BrokerError;
    use crate::protocols::common::v1::Error;
    use crate::protocols::execution::v1::ExecutionState;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn heartbeat(worker_id: &str, generation: &str) -> ReportHeartbeatRequest {
        ReportHeartbeatRequest {
            heartbeat: Some(crate::protocols::runtime::v1::RuntimeHeartbeat {
                runtime_id: crate::worker_registry::worker_incarnation_id(worker_id, generation),
                sequence_number: 1,
                ..Default::default()
            }),
        }
    }

    fn worker_registration(worker_id: &str, generation: &str) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            registration: Some(crate::protocols::runtime::v1::WorkerRegistration {
                worker_id: worker_id.to_owned(),
                generation: generation.to_owned(),
                implementation_version: "test".to_owned(),
                protocol_versions: vec!["runtime/v1".to_owned()],
                ..Default::default()
            }),
        }
    }

    #[derive(Default)]
    struct RecordingBroker {
        envelopes: Mutex<Vec<BrokerEnvelope>>,
    }
    #[derive(Default)]
    struct RecordingResultBackend {
        results: Mutex<Vec<(String, JobResult)>>,
    }
    #[derive(Default)]
    struct InspectingBroker {
        jobs: Mutex<Option<JobStore>>,
        job_was_visible: AtomicBool,
        fail_publish: bool,
    }
    type JobStore = Arc<RwLock<HashMap<String, JobRecord>>>;
    #[tonic::async_trait]
    impl BackendResult for RecordingResultBackend {
        type Error = ResultBackendError;

        async fn store_result(&self, job_id: &str, result: &JobResult) -> Result<(), Self::Error> {
            self.results
                .lock()
                .unwrap()
                .push((job_id.to_owned(), result.clone()));
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl Broker for RecordingBroker {
        async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
            self.envelopes.lock().unwrap().push(envelope);
            Ok(())
        }
        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            let mut envelopes = self.envelopes.lock().unwrap();
            if envelopes.is_empty() {
                return Err(BrokerError::new("empty"));
            }
            Ok(envelopes.remove(0))
        }
    }
    #[tonic::async_trait]
    impl Broker for InspectingBroker {
        async fn publish(&self, envelope: BrokerEnvelope) -> Result<(), BrokerError> {
            let job_id = envelope.correlation_id.unwrap();
            let jobs = self.jobs.lock().unwrap();
            self.job_was_visible.store(
                jobs.as_ref().unwrap().read().unwrap().contains_key(&job_id),
                Ordering::SeqCst,
            );
            if self.fail_publish {
                return Err(BrokerError::new("publish failed"));
            }
            Ok(())
        }
        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            Err(BrokerError::new("empty"))
        }
    }

    #[tokio::test]
    async fn valid_worker_registration_is_acknowledged_and_recorded() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );

        let response = service
            .register_worker(Request::new(worker_registration(
                "worker-1",
                "generation-1",
            )))
            .await
            .unwrap()
            .into_inner();

        assert!(response.accepted);
        assert!(response.lease_id.is_none());
        assert_eq!(service.worker_registry.len(), 1);
    }

    #[tokio::test]
    async fn malformed_worker_registration_is_rejected() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );

        let status = service
            .register_worker(Request::new(worker_registration(" ", "generation-1")))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(service.worker_registry.len(), 0);
    }

    #[tokio::test]
    async fn same_worker_generation_registration_is_idempotent() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let request = worker_registration("worker-1", "generation-1");

        service
            .register_worker(Request::new(request.clone()))
            .await
            .unwrap();
        let mut duplicate = request;
        duplicate
            .registration
            .as_mut()
            .unwrap()
            .implementation_version = "changed-but-same-incarnation".to_owned();
        service
            .register_worker(Request::new(duplicate))
            .await
            .unwrap();

        assert_eq!(service.worker_registry.len(), 1);
        let stored = service.worker_registry.get("worker-1").unwrap();
        assert_eq!(stored.registration.generation, "generation-1");
        assert_eq!(stored.registration.implementation_version, "test");
    }

    #[tokio::test]
    async fn new_generation_replaces_previous_worker_incarnation() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );

        service
            .register_worker(Request::new(worker_registration(
                "worker-1",
                "generation-1",
            )))
            .await
            .unwrap();
        service
            .register_worker(Request::new(worker_registration(
                "worker-1",
                "generation-2",
            )))
            .await
            .unwrap();

        assert_eq!(service.worker_registry.len(), 1);
        assert_eq!(
            service
                .worker_registry
                .get("worker-1")
                .unwrap()
                .registration
                .generation,
            "generation-2"
        );
    }

    #[tokio::test]
    async fn heartbeat_for_current_generation_is_accepted_and_updates_last_seen() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        service
            .register_worker(Request::new(worker_registration(
                "worker-1",
                "generation-1",
            )))
            .await
            .unwrap();
        let registered_at = service
            .worker_registry
            .get("worker-1")
            .unwrap()
            .registered_at;

        let response = service
            .report_heartbeat(Request::new(heartbeat("worker-1", "generation-1")))
            .await
            .unwrap()
            .into_inner();

        assert!(response.accepted);
        let last_seen = service
            .worker_registry
            .get("worker-1")
            .unwrap()
            .last_heartbeat_at
            .unwrap();
        assert!(last_seen >= registered_at);
    }

    #[tokio::test]
    async fn heartbeat_for_unknown_worker_is_rejected() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );

        let status = service
            .report_heartbeat(Request::new(heartbeat("unknown", "generation-1")))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn heartbeat_for_stale_generation_is_rejected() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        service
            .register_worker(Request::new(worker_registration(
                "worker-1",
                "generation-2",
            )))
            .await
            .unwrap();

        let status = service
            .report_heartbeat(Request::new(heartbeat("worker-1", "generation-1")))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            service
                .worker_registry
                .get("worker-1")
                .unwrap()
                .last_heartbeat_at
                .is_none()
        );
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
        let result_backend = Arc::new(RecordingResultBackend::default());
        let response = CoreExecutionService::new(broker.clone(), result_backend.clone(), "tasks")
            .accept(request(br#"{"args":[1,2]}"#.to_vec()))
            .await
            .unwrap();
        let job = response.job.unwrap();
        assert!(!job.job_id.is_empty());
        assert_eq!(job.state, i32::from(JobState::Accepted));
        let envelopes = broker.envelopes.lock().unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].destination, "tasks");
        assert_eq!(
            SubmitTaskRequest::decode(envelopes[0].payload.as_ref())
                .unwrap()
                .task_name,
            "demo.add"
        );
        let results = result_backend.results.lock().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, job.job_id);
    }

    #[tokio::test]
    async fn job_is_visible_before_task_is_published() {
        let broker = Arc::new(InspectingBroker::default());
        let service = CoreExecutionService::new(
            broker.clone(),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        *broker.jobs.lock().unwrap() = Some(service.jobs.clone());

        service.accept(request(Vec::new())).await.unwrap();

        assert!(broker.job_was_visible.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_publish_removes_inserted_job() {
        let broker = Arc::new(InspectingBroker {
            fail_publish: true,
            ..Default::default()
        });
        let service = CoreExecutionService::new(
            broker.clone(),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        *broker.jobs.lock().unwrap() = Some(service.jobs.clone());

        let status = service.accept(request(Vec::new())).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(broker.job_was_visible.load(Ordering::SeqCst));
        assert!(service.jobs.read().unwrap().is_empty());
    }
    #[tokio::test]
    async fn empty_payload_is_accepted() {
        let response = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        )
        .accept(request(Vec::new()))
        .await
        .unwrap();
        assert!(response.job.is_some());
    }

    #[tokio::test]
    async fn get_job_returns_an_accepted_job() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let submitted = service.accept(request(Vec::new())).await.unwrap();
        let job_id = submitted.job.unwrap().job_id;

        let response = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.job.unwrap().state, i32::from(JobState::Accepted));
        assert!(response.result.is_none());
    }

    #[tokio::test]
    async fn get_job_returns_not_found_for_an_unknown_job() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );

        let status = service
            .get_job(Request::new(GetJobRequest {
                job_id: "missing".into(),
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn successful_result_is_stored_and_returned() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let job_id = service
            .accept(request(Vec::new()))
            .await
            .unwrap()
            .job
            .unwrap()
            .job_id;
        let result = JobResult {
            payload: br#"{"value":3}"#.to_vec(),
            serialization_format: "json".into(),
            ..Default::default()
        };

        service.store_job_result(&job_id, result.clone()).unwrap();
        let response = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.job.unwrap().state, i32::from(JobState::Succeeded));
        assert_eq!(response.result, Some(result));
    }

    #[tokio::test]
    async fn failed_result_is_stored_and_returned() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let job_id = service
            .accept(request(Vec::new()))
            .await
            .unwrap()
            .job
            .unwrap()
            .job_id;
        let result = JobResult {
            failure: Some(Error {
                code: "TASK_FAILED".into(),
                message: "worker reported failure".into(),
                metadata: None,
            }),
            ..Default::default()
        };

        service.store_job_result(&job_id, result.clone()).unwrap();
        let response = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.job.unwrap().state, i32::from(JobState::Failed));
        assert_eq!(response.result, Some(result));
    }

    async fn acquire_submitted(service: &CoreExecutionService) -> (String, AssignExecutionRequest) {
        let job_id = service
            .accept(request(br#"{"args":[1,2]}"#.to_vec()))
            .await
            .unwrap()
            .job
            .unwrap()
            .job_id;
        let assignment = service
            .acquire_execution(Request::new(AcquireExecutionRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .assignment
            .unwrap();
        (job_id, assignment)
    }

    async fn report(
        service: &CoreExecutionService,
        assignment: &AssignExecutionRequest,
        state: ExecutionState,
        outcome: Option<JobResult>,
    ) {
        service
            .report_execution(Request::new(ReportExecutionRequest {
                report_id: Uuid::new_v4().to_string(),
                assignment_id: assignment.assignment_id.clone(),
                execution_id: assignment.execution_id.clone(),
                sequence_number: 1,
                state: state.into(),
                outcome,
                observed_at: Some(SystemTime::now().into()),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn worker_can_acquire_start_complete_and_observe_success() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let (job_id, assignment) = acquire_submitted(&service).await;
        assert_eq!(assignment.job_id, job_id);
        assert_eq!(assignment.task.as_ref().unwrap().name, "demo.add");
        assert_eq!(assignment.arguments, br#"{"args":[1,2]}"#);
        assert_eq!(assignment.serialization_format, "json");

        report(&service, &assignment, ExecutionState::Running, None).await;
        let running = service
            .get_job(Request::new(GetJobRequest {
                job_id: job_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(running.job.unwrap().state, i32::from(JobState::Running));

        let result = JobResult {
            payload: br#"{"value":3}"#.to_vec(),
            serialization_format: "json".into(),
            ..Default::default()
        };
        report(
            &service,
            &assignment,
            ExecutionState::Succeeded,
            Some(result.clone()),
        )
        .await;
        let completed = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(completed.job.unwrap().state, i32::from(JobState::Succeeded));
        assert_eq!(completed.result.as_ref().unwrap().payload, result.payload);
        assert_eq!(
            completed.result.unwrap().execution_id,
            assignment.execution_id
        );
    }

    #[tokio::test]
    async fn worker_can_acquire_start_fail_and_observe_failure() {
        let service = CoreExecutionService::new(
            Arc::new(RecordingBroker::default()),
            Arc::new(RecordingResultBackend::default()),
            "tasks",
        );
        let (job_id, assignment) = acquire_submitted(&service).await;
        report(&service, &assignment, ExecutionState::Running, None).await;
        let failure = Error {
            code: "PYTHON_EXCEPTION".into(),
            message: "ValueError: bad input".into(),
            metadata: None,
        };
        report(
            &service,
            &assignment,
            ExecutionState::Failed,
            Some(JobResult {
                failure: Some(failure.clone()),
                ..Default::default()
            }),
        )
        .await;

        let failed = service
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(failed.job.unwrap().state, i32::from(JobState::Failed));
        assert_eq!(failed.result.unwrap().failure, Some(failure));
    }
}
