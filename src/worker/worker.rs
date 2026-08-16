use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use prost::Message;
#[cfg(not(test))]
use tokio::net::TcpListener;
#[cfg(not(test))]
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(not(test))]
use tonic::transport::Server;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::PendingExecutions;
#[cfg(not(test))]
use super::WorkerRuntimeService;
use crate::broker::{Broker, BrokerError, worker_destination};
use crate::config::WorkerConfig;
use crate::protocols::common::v1::{Metadata, ResourceRequirements};
use crate::protocols::runtime::v1::runtime_service_client::RuntimeServiceClient;
#[cfg(not(test))]
use crate::protocols::runtime::v1::runtime_service_server::RuntimeServiceServer;
use crate::protocols::runtime::v1::{
    AssignExecutionRequest, RegisterWorkerRequest, RegisterWorkerResponse, ReportHeartbeatRequest,
    ReportHeartbeatResponse, RuntimeHeartbeat, RuntimeStatus, WorkerRegistration,
};
use crate::worker_registry::worker_incarnation_id;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

const SUPPORTED_PROTOCOL_VERSIONS: [&str; 5] = [
    "common/v1",
    "artifacts/v1",
    "broker/v1",
    "execution/v1",
    "runtime/v1",
];

#[derive(Debug)]
pub struct WorkerError(String);

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for WorkerError {}

impl From<BrokerError> for WorkerError {
    fn from(error: BrokerError) -> Self {
        Self(error.to_string())
    }
}

#[async_trait]
pub trait WorkerRegistrationClient: Send + Sync {
    async fn register_worker(
        &self,
        request: RegisterWorkerRequest,
    ) -> Result<RegisterWorkerResponse, WorkerError>;

    async fn report_heartbeat(
        &self,
        request: ReportHeartbeatRequest,
    ) -> Result<ReportHeartbeatResponse, WorkerError>;
}

struct GrpcWorkerRegistrationClient {
    endpoint: String,
}

#[async_trait]
impl WorkerRegistrationClient for GrpcWorkerRegistrationClient {
    async fn register_worker(
        &self,
        request: RegisterWorkerRequest,
    ) -> Result<RegisterWorkerResponse, WorkerError> {
        let mut client = RuntimeServiceClient::connect(self.endpoint.clone())
            .await
            .map_err(|error| WorkerError(format!("cannot connect to Core: {error}")))?;
        client
            .register_worker(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| WorkerError(format!("Core rejected registration: {error}")))
    }

    async fn report_heartbeat(
        &self,
        request: ReportHeartbeatRequest,
    ) -> Result<ReportHeartbeatResponse, WorkerError> {
        let mut client = RuntimeServiceClient::connect(self.endpoint.clone())
            .await
            .map_err(|error| WorkerError(format!("cannot connect to Core: {error}")))?;
        client
            .report_heartbeat(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| WorkerError(format!("Core rejected heartbeat: {error}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
    pub worker_id: String,
    pub generation: String,
    pub name: String,
}

impl WorkerIdentity {
    pub fn new(name: Option<String>) -> Self {
        Self {
            worker_id: Uuid::new_v4().to_string(),
            generation: Uuid::new_v4().to_string(),
            name: name.unwrap_or_else(default_worker_name),
        }
    }
}

fn default_worker_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty())
        })
        .unwrap_or_else(|| "localhost".to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResources {
    pub cpu_cores: u64,
    pub memory_bytes: u64,
}

impl From<&WorkerResources> for ResourceRequirements {
    fn from(resources: &WorkerResources) -> Self {
        Self {
            cpu_millicores: resources.cpu_cores * 1000,
            memory_bytes: resources.memory_bytes,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedCapacity {
    pub resources: WorkerResources,
    pub capabilities: Vec<String>,
}

pub struct Worker {
    broker: Arc<dyn Broker>,
    destination: String,
    identity: WorkerIdentity,
    advertised_capacity: AdvertisedCapacity,
    registration_client: Arc<dyn WorkerRegistrationClient>,
    heartbeat_interval: Duration,
    runtime_address: String,
    pending_executions: PendingExecutions,
}

impl Worker {
    pub fn new(broker: Arc<dyn Broker>, config: WorkerConfig) -> Self {
        let registration_client = Arc::new(GrpcWorkerRegistrationClient {
            endpoint: config.core_endpoint.clone(),
        });
        let identity = WorkerIdentity::new(config.name);
        Self {
            broker,
            destination: worker_destination(&identity.worker_id),
            identity,
            advertised_capacity: AdvertisedCapacity {
                resources: WorkerResources {
                    cpu_cores: config.resources.cpu,
                    memory_bytes: config.resources.memory,
                },
                capabilities: config.capabilities,
            },
            registration_client,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            runtime_address: config.runtime_address,
            pending_executions: PendingExecutions::default(),
        }
    }

    pub fn with_registration_client(
        broker: Arc<dyn Broker>,
        config: WorkerConfig,
        registration_client: Arc<dyn WorkerRegistrationClient>,
    ) -> Self {
        let identity = WorkerIdentity::new(config.name);
        Self {
            broker,
            destination: worker_destination(&identity.worker_id),
            identity,
            advertised_capacity: AdvertisedCapacity {
                resources: WorkerResources {
                    cpu_cores: config.resources.cpu,
                    memory_bytes: config.resources.memory,
                },
                capabilities: config.capabilities,
            },
            registration_client,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            runtime_address: config.runtime_address,
            pending_executions: PendingExecutions::default(),
        }
    }

    pub fn advertised_capacity(&self) -> &AdvertisedCapacity {
        &self.advertised_capacity
    }

    pub fn identity(&self) -> &WorkerIdentity {
        &self.identity
    }

    pub fn registration(&self) -> WorkerRegistration {
        WorkerRegistration {
            worker_id: self.identity.worker_id.clone(),
            node_id: None,
            generation: self.identity.generation.clone(),
            implementation_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_versions: SUPPORTED_PROTOCOL_VERSIONS
                .into_iter()
                .map(str::to_owned)
                .collect(),
            endpoint: self.runtime_address.clone(),
            executors: Vec::new(),
            capacity: Some(ResourceRequirements {
                required_capabilities: self.advertised_capacity.capabilities.clone(),
                ..ResourceRequirements::from(&self.advertised_capacity.resources)
            }),
            metadata: Some(Metadata {
                entries: HashMap::from([("worker_name".to_owned(), self.identity.name.clone())]),
            }),
        }
    }

    pub async fn run(&self) -> Result<(), WorkerError> {
        self.run_until_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                error!(%error, "failed to listen for worker shutdown signal");
            }
        })
        .await
    }

    async fn run_until_shutdown<F>(&self, shutdown: F) -> Result<(), WorkerError>
    where
        F: Future<Output = ()>,
    {
        let response = self
            .registration_client
            .register_worker(RegisterWorkerRequest {
                registration: Some(self.registration()),
            })
            .await;
        let response = match response {
            Ok(response) if response.accepted => response,
            Ok(_) => {
                let failure = WorkerError("Core did not accept worker registration".to_owned());
                error!(error = %failure, "worker registration failed");
                return Err(failure);
            }
            Err(failure) => {
                error!(error = %failure, "worker registration failed");
                return Err(failure);
            }
        };
        info!(
            worker_id = %self.identity.worker_id,
            generation = %self.identity.generation,
            name = %self.identity.name,
            cpu = self.advertised_capacity.resources.cpu_cores,
            memory = %format_memory(self.advertised_capacity.resources.memory_bytes),
            capabilities = ?self.advertised_capacity.capabilities,
            destination = %self.destination,
            lease_id = ?response.lease_id,
            "worker registered"
        );
        tokio::pin!(shutdown);
        let assignments = self.assignment_loop();
        let heartbeats = self.heartbeat_loop();
        tokio::pin!(assignments, heartbeats);
        #[cfg(not(test))]
        let runtime_server = async {
            let listener = TcpListener::bind(&self.runtime_address)
                .await
                .map_err(|error| WorkerError(format!("cannot bind worker runtime API: {error}")))?;
            let address = listener.local_addr().map_err(|error| {
                WorkerError(format!("cannot inspect worker runtime API: {error}"))
            })?;
            info!(%address, "worker runtime gRPC API listening");
            Server::builder()
                .add_service(RuntimeServiceServer::new(WorkerRuntimeService::new(
                    self.pending_executions.clone(),
                )))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .map_err(|error| WorkerError(format!("worker runtime API failed: {error}")))
        };
        #[cfg(not(test))]
        tokio::pin!(runtime_server);
        #[cfg(not(test))]
        let result = tokio::select! {
            result = &mut assignments => result,
            _ = &mut heartbeats => unreachable!("heartbeat loop does not complete"),
            result = &mut runtime_server => result,
            _ = &mut shutdown => Ok(()),
        };
        #[cfg(test)]
        let result = tokio::select! {
            result = &mut assignments => result,
            _ = &mut heartbeats => unreachable!("heartbeat loop does not complete"),
            _ = &mut shutdown => Ok(()),
        };
        info!(worker_id = %self.identity.worker_id, generation = %self.identity.generation,
            "worker shutting down");
        result
    }

    async fn assignment_loop(&self) -> Result<(), WorkerError> {
        loop {
            self.execute_one().await.map_err(WorkerError::from)?;
        }
    }

    async fn heartbeat_loop(&self) {
        let mut interval = tokio::time::interval(self.heartbeat_interval);
        let runtime_id = worker_incarnation_id(&self.identity.worker_id, &self.identity.generation);
        let mut sequence_number = 0;
        loop {
            interval.tick().await;
            sequence_number += 1;
            let response = self
                .registration_client
                .report_heartbeat(ReportHeartbeatRequest {
                    heartbeat: Some(RuntimeHeartbeat {
                        runtime_id: runtime_id.clone(),
                        sequence_number,
                        status: RuntimeStatus::Ready.into(),
                        available_resources: Some(ResourceRequirements::from(
                            &self.advertised_capacity.resources,
                        )),
                        observed_at: Some(SystemTime::now().into()),
                    }),
                })
                .await;
            match response {
                Ok(response) if response.accepted => {
                    debug!(worker_id = %self.identity.worker_id, %sequence_number,
                        "worker heartbeat accepted");
                }
                Ok(_) => warn!(worker_id = %self.identity.worker_id, %sequence_number,
                    "worker heartbeat was not accepted"),
                Err(error) => warn!(worker_id = %self.identity.worker_id, %sequence_number, %error,
                    "worker heartbeat failed"),
            }
        }
    }

    pub async fn execute_one(&self) -> Result<AssignExecutionRequest, BrokerError> {
        let envelope = self.broker.consume(&self.destination).await?;
        let assignment =
            AssignExecutionRequest::decode(envelope.payload.as_ref()).map_err(|error| {
                BrokerError::new(format!("invalid AssignExecutionRequest: {error}"))
            })?;
        self.pending_executions
            .enqueue(assignment.clone())
            .await
            .map_err(|error| BrokerError::new(format!("cannot queue assignment: {error}")))?;
        let task_name = assignment
            .task
            .as_ref()
            .map(|task| task.name.as_str())
            .unwrap_or("<unspecified>");
        info!(execution_id = %assignment.execution_id, "received execution");
        info!(task = task_name, "queued execution for attached runtime");
        Ok(assignment)
    }
}

fn format_memory(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ];
    for (suffix, divisor) in UNITS {
        if bytes >= divisor && bytes.is_multiple_of(divisor) {
            return format!("{}{suffix}", bytes / divisor);
        }
    }
    format!("{bytes}B")
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
            assignment_id: "assignment-1".into(),
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
        let executed = Worker::new(broker, WorkerConfig::default());
        let consumed = executed.execute_one().await.unwrap();
        let acquired = executed.pending_executions.acquire().await;
        assert_eq!(consumed, acquired);
        assert_eq!(acquired.execution_id, "execution-1");
        assert_eq!(acquired.task.unwrap().name, "demo.add");
    }

    #[derive(Default)]
    struct RecordingBroker(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl Broker for RecordingBroker {
        async fn publish(&self, _: BrokerEnvelope) -> Result<(), BrokerError> {
            unreachable!()
        }

        async fn consume(&self, destination: &str) -> Result<BrokerEnvelope, BrokerError> {
            self.0.lock().unwrap().push(destination.to_owned());
            Ok(BrokerEnvelope {
                payload: AssignExecutionRequest {
                    assignment_id: "assignment-1".into(),
                    execution_id: "execution-1".into(),
                    ..Default::default()
                }
                .encode_to_vec(),
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn workers_consume_only_from_their_own_worker_id_destinations() {
        let broker = Arc::new(RecordingBroker::default());
        let config = || WorkerConfig {
            name: Some("shared-human-name".to_owned()),
            ..WorkerConfig::default()
        };
        let worker_1 = Worker::new(broker.clone(), config());
        let worker_2 = Worker::new(broker.clone(), config());

        worker_1.execute_one().await.unwrap();
        worker_2.execute_one().await.unwrap();

        let destinations = broker.0.lock().unwrap();
        assert_eq!(
            destinations[0],
            worker_destination(&worker_1.identity.worker_id)
        );
        assert_eq!(
            destinations[1],
            worker_destination(&worker_2.identity.worker_id)
        );
        assert_ne!(destinations[0], destinations[1]);
        assert!(
            !destinations
                .iter()
                .any(|destination| destination.contains(&worker_1.identity.name))
        );
    }

    #[test]
    fn generated_identity_has_distinct_valid_uuids() {
        let identity = WorkerIdentity::new(None);

        assert!(!identity.worker_id.is_empty());
        assert!(!identity.generation.is_empty());
        assert!(Uuid::parse_str(&identity.worker_id).is_ok());
        assert!(Uuid::parse_str(&identity.generation).is_ok());
        assert_ne!(identity.worker_id, identity.generation);
    }

    #[test]
    fn configured_worker_name_is_respected() {
        let identity = WorkerIdentity::new(Some("gpu-worker-01".to_owned()));

        assert_eq!(identity.name, "gpu-worker-01");
    }

    #[test]
    fn default_worker_name_is_non_empty() {
        let identity = WorkerIdentity::new(None);

        assert!(!identity.name.trim().is_empty());
    }

    #[test]
    fn worker_retains_configured_identity_and_capacity() {
        let broker = Arc::new(TestBroker(Mutex::new(None)));
        let worker = Worker::new(
            broker,
            WorkerConfig {
                name: Some("big-bertha".to_owned()),
                core_endpoint: "http://core.invalid".to_owned(),
                runtime_address: "127.0.0.1:0".to_owned(),
                resources: crate::config::WorkerResourcesConfig {
                    cpu: 16,
                    memory: 32 * (1 << 30),
                },
                capabilities: vec!["python".to_owned(), "linux".to_owned()],
            },
        );

        assert_eq!(worker.identity().name, "big-bertha");
        assert_eq!(
            worker.destination,
            worker_destination(&worker.identity().worker_id)
        );
        assert_ne!(worker.destination, "workers.default");
        assert_eq!(worker.advertised_capacity().resources.cpu_cores, 16);
        assert_eq!(
            worker.advertised_capacity().resources.memory_bytes,
            32 * (1 << 30)
        );
        assert_eq!(
            worker.advertised_capacity().capabilities,
            ["python", "linux"]
        );
        let protocol_resources =
            ResourceRequirements::from(&worker.advertised_capacity().resources);
        assert_eq!(protocol_resources.cpu_millicores, 16_000);
        assert_eq!(protocol_resources.memory_bytes, 32 * (1 << 30));
    }

    struct RejectingRegistrationClient;

    #[async_trait]
    impl WorkerRegistrationClient for RejectingRegistrationClient {
        async fn register_worker(
            &self,
            _: RegisterWorkerRequest,
        ) -> Result<RegisterWorkerResponse, WorkerError> {
            Err(WorkerError("registration unavailable".to_owned()))
        }

        async fn report_heartbeat(
            &self,
            _: ReportHeartbeatRequest,
        ) -> Result<ReportHeartbeatResponse, WorkerError> {
            panic!("heartbeat must not be sent before registration succeeds")
        }
    }

    #[tokio::test]
    async fn failed_registration_prevents_assignment_consumption() {
        let broker = Arc::new(TestBroker(Mutex::new(Some(BrokerEnvelope::default()))));
        let worker = Worker::with_registration_client(
            broker.clone(),
            WorkerConfig::default(),
            Arc::new(RejectingRegistrationClient),
        );

        let error = worker.run().await.unwrap_err();

        assert!(error.to_string().contains("registration unavailable"));
        assert!(broker.0.lock().unwrap().is_some());
    }

    #[derive(Default)]
    struct AcceptingRegistrationClient {
        heartbeats: std::sync::atomic::AtomicUsize,
    }

    struct PendingBroker;

    #[async_trait]
    impl Broker for PendingBroker {
        async fn publish(&self, _: BrokerEnvelope) -> Result<(), BrokerError> {
            Ok(())
        }

        async fn consume(&self, _: &str) -> Result<BrokerEnvelope, BrokerError> {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl WorkerRegistrationClient for AcceptingRegistrationClient {
        async fn register_worker(
            &self,
            _: RegisterWorkerRequest,
        ) -> Result<RegisterWorkerResponse, WorkerError> {
            Ok(RegisterWorkerResponse {
                accepted: true,
                lease_id: None,
            })
        }

        async fn report_heartbeat(
            &self,
            request: ReportHeartbeatRequest,
        ) -> Result<ReportHeartbeatResponse, WorkerError> {
            let heartbeat = request.heartbeat.unwrap();
            assert!(heartbeat.sequence_number > 0);
            self.heartbeats
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ReportHeartbeatResponse { accepted: true })
        }
    }

    #[tokio::test]
    async fn worker_sends_heartbeats_after_successful_registration() {
        let client = Arc::new(AcceptingRegistrationClient::default());
        let mut worker = Worker::with_registration_client(
            Arc::new(PendingBroker),
            WorkerConfig::default(),
            client.clone(),
        );
        worker.heartbeat_interval = Duration::from_millis(1);

        worker
            .run_until_shutdown(tokio::time::sleep(Duration::from_millis(10)))
            .await
            .unwrap();

        assert!(client.heartbeats.load(std::sync::atomic::Ordering::SeqCst) > 0);
    }

    #[test]
    fn registration_contains_supported_worker_advertisement() {
        let worker = Worker::new(
            Arc::new(TestBroker(Mutex::new(None))),
            WorkerConfig {
                name: Some("gpu-01".to_owned()),
                core_endpoint: "http://core.invalid".to_owned(),
                runtime_address: "127.0.0.1:0".to_owned(),
                resources: crate::config::WorkerResourcesConfig {
                    cpu: 8,
                    memory: 16 * (1 << 30),
                },
                capabilities: vec!["cuda".to_owned()],
            },
        );

        let registration = worker.registration();

        assert_eq!(registration.worker_id, worker.identity().worker_id);
        assert_eq!(registration.generation, worker.identity().generation);
        assert_eq!(
            registration.implementation_version,
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(
            registration.protocol_versions,
            [
                "common/v1",
                "artifacts/v1",
                "broker/v1",
                "execution/v1",
                "runtime/v1"
            ]
        );
        assert_eq!(registration.endpoint, "127.0.0.1:0");
        assert_eq!(
            registration.capacity.unwrap().required_capabilities,
            ["cuda"]
        );
        assert_eq!(
            registration
                .metadata
                .unwrap()
                .entries
                .get("worker_name")
                .map(String::as_str),
            Some("gpu-01")
        );
        assert!(registration.executors.is_empty());
    }
}
