use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use super::CoreWorkerClient;
use crate::protocols::execution::v1::ExecutionState;
use crate::protocols::runtime::v1::runtime_service_server::RuntimeService;
use crate::protocols::runtime::v1::{
    AcquireExecutionRequest, AcquireExecutionResponse, AssignExecutionRequest,
    AssignExecutionResponse, RegisterRuntimeRequest, RegisterRuntimeResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, ReportExecutionRequest, ReportExecutionResponse,
    ReportHeartbeatRequest, ReportHeartbeatResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssignmentState {
    Pending,
    Acquired,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug)]
struct AssignmentRecord {
    assignment: AssignExecutionRequest,
    state: AssignmentState,
    last_sequence_number: Option<u64>,
}

#[derive(Debug, Default)]
struct PendingState {
    queue: VecDeque<String>,
    assignments: HashMap<String, AssignmentRecord>,
}

/// Worker-owned handoff between broker delivery and a language runtime.
#[derive(Debug, Clone, Default)]
pub struct PendingExecutions {
    state: Arc<Mutex<PendingState>>,
    available: Arc<Notify>,
}

impl PendingExecutions {
    pub async fn enqueue(&self, assignment: AssignExecutionRequest) -> Result<(), Status> {
        if assignment.assignment_id.trim().is_empty() || assignment.execution_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "assignment_id and execution_id are required",
            ));
        }
        let assignment_id = assignment.assignment_id.clone();
        let mut state = self.state.lock().await;
        if state.assignments.contains_key(&assignment_id) {
            return Err(Status::already_exists("assignment already exists"));
        }
        state.queue.push_back(assignment_id.clone());
        state.assignments.insert(
            assignment_id,
            AssignmentRecord {
                assignment,
                state: AssignmentState::Pending,
                last_sequence_number: None,
            },
        );
        drop(state);
        self.available.notify_one();
        Ok(())
    }

    /// Waits until an assignment is available. The loop avoids lost notifications.
    pub async fn acquire(&self) -> AssignExecutionRequest {
        loop {
            let notified = self.available.notified();
            {
                let mut state = self.state.lock().await;
                if let Some(assignment_id) = state.queue.pop_front() {
                    let record = state
                        .assignments
                        .get_mut(&assignment_id)
                        .expect("queued assignment must have a record");
                    record.state = AssignmentState::Acquired;
                    return record.assignment.clone();
                }
            }
            notified.await;
        }
    }

    pub async fn report(
        &self,
        report: ReportExecutionRequest,
    ) -> Result<AssignExecutionRequest, Status> {
        let reported_state = ExecutionState::try_from(report.state)
            .map_err(|_| Status::invalid_argument("unknown execution state"))?;
        let mut state = self.state.lock().await;
        let record = state
            .assignments
            .get_mut(&report.assignment_id)
            .ok_or_else(|| Status::not_found("assignment not found"))?;
        if record.assignment.execution_id != report.execution_id {
            return Err(Status::failed_precondition(
                "execution_id does not match assignment",
            ));
        }
        if record
            .last_sequence_number
            .is_some_and(|sequence| report.sequence_number <= sequence)
        {
            return Err(Status::failed_precondition("stale execution report"));
        }

        record.state = match (record.state, reported_state) {
            (AssignmentState::Acquired, ExecutionState::Running) => AssignmentState::Running,
            (AssignmentState::Running, ExecutionState::Succeeded) => {
                let outcome = report.outcome.as_ref().ok_or_else(|| {
                    Status::invalid_argument("succeeded report requires a result")
                })?;
                if outcome.failure.is_some() {
                    return Err(Status::invalid_argument(
                        "succeeded report cannot contain failure information",
                    ));
                }
                AssignmentState::Succeeded
            }
            (AssignmentState::Running, ExecutionState::Failed) => {
                if report
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.failure.as_ref())
                    .is_none()
                {
                    return Err(Status::invalid_argument(
                        "failed report requires failure information",
                    ));
                }
                AssignmentState::Failed
            }
            (AssignmentState::Succeeded | AssignmentState::Failed, _) => {
                return Err(Status::failed_precondition(
                    "assignment is already terminal",
                ));
            }
            _ => {
                return Err(Status::failed_precondition(
                    "invalid assignment lifecycle transition",
                ));
            }
        };
        record.last_sequence_number = Some(report.sequence_number);
        Ok(record.assignment.clone())
    }
}

#[derive(Clone)]
pub struct WorkerRuntimeService {
    pending: PendingExecutions,
    core_client: Arc<dyn CoreWorkerClient>,
}

impl WorkerRuntimeService {
    pub fn new(pending: PendingExecutions, core_client: Arc<dyn CoreWorkerClient>) -> Self {
        Self {
            pending,
            core_client,
        }
    }
}

#[tonic::async_trait]
impl RuntimeService for WorkerRuntimeService {
    async fn acquire_execution(
        &self,
        _request: Request<AcquireExecutionRequest>,
    ) -> Result<Response<AcquireExecutionResponse>, Status> {
        let assignment = self.pending.acquire().await;
        let task = assignment
            .task
            .as_ref()
            .map(|task| task.name.as_str())
            .unwrap_or("<unspecified>");
        info!(job_id = %assignment.job_id, execution_id = %assignment.execution_id,
            assignment_id = %assignment.assignment_id, task, "runtime acquired execution");
        Ok(Response::new(AcquireExecutionResponse {
            assignment: Some(assignment),
        }))
    }

    async fn report_execution(
        &self,
        request: Request<ReportExecutionRequest>,
    ) -> Result<Response<ReportExecutionResponse>, Status> {
        let report = request.into_inner();
        let assignment = self.pending.report(report.clone()).await?;
        let state = ExecutionState::try_from(report.state)
            .map_err(|_| Status::invalid_argument("unknown execution state"))?;
        let task = assignment
            .task
            .as_ref()
            .map(|task| task.name.as_str())
            .unwrap_or("<unspecified>");
        let failure_code = report
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.failure.as_ref())
            .map(|failure| failure.code.as_str());
        let serialization_format = report
            .outcome
            .as_ref()
            .map(|outcome| outcome.serialization_format.as_str());
        match state {
            ExecutionState::Running => info!(job_id = %assignment.job_id,
                execution_id = %report.execution_id, assignment_id = %report.assignment_id,
                task, "runtime reported execution running"),
            ExecutionState::Succeeded => info!(job_id = %assignment.job_id,
                execution_id = %report.execution_id, assignment_id = %report.assignment_id,
                task, serialization_format, "runtime reported execution succeeded"),
            ExecutionState::Failed => warn!(job_id = %assignment.job_id,
                execution_id = %report.execution_id, assignment_id = %report.assignment_id,
                task, failure_code, "runtime reported execution failed"),
            _ => unreachable!("pending execution accepted only supported states"),
        }

        // Local lifecycle state advances before Core acknowledgement. This POC intentionally has
        // no retry/outbox reconciliation, so a forwarding failure can leave Core behind the worker.
        info!(job_id = %assignment.job_id, execution_id = %report.execution_id,
            assignment_id = %report.assignment_id, task, state = ?state,
            "forwarding execution report to core");
        let response = match self.core_client.report_execution(report).await {
            Ok(response) => response,
            Err(status) => {
                error!(job_id = %assignment.job_id, execution_id = %assignment.execution_id,
                    assignment_id = %assignment.assignment_id, task, error = %status,
                    "failed to forward execution report to core");
                return Err(status);
            }
        };
        if !response.accepted {
            error!(job_id = %assignment.job_id, execution_id = %assignment.execution_id,
                assignment_id = %assignment.assignment_id, task,
                "failed to forward execution report to core");
            return Err(Status::internal("Core did not accept execution report"));
        }
        info!(job_id = %assignment.job_id, execution_id = %assignment.execution_id,
            assignment_id = %assignment.assignment_id, task,
            "core accepted execution report");
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
        _: Request<ReportHeartbeatRequest>,
    ) -> Result<Response<ReportHeartbeatResponse>, Status> {
        Err(Status::unimplemented(
            "runtime heartbeat is outside this POC",
        ))
    }
    async fn register_worker(
        &self,
        _: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        Err(Status::unimplemented("worker registration belongs to Core"))
    }
    async fn assign_execution(
        &self,
        _: Request<AssignExecutionRequest>,
    ) -> Result<Response<AssignExecutionResponse>, Status> {
        Err(Status::unimplemented(
            "assignments arrive through the worker broker consumer",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::common::v1::Error;
    use crate::protocols::execution::v1::JobResult;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Copy, Default)]
    enum CoreBehavior {
        #[default]
        Accept,
        Reject,
        Unavailable,
    }

    #[derive(Default)]
    struct RecordingCoreClient {
        reports: StdMutex<Vec<ReportExecutionRequest>>,
        behavior: CoreBehavior,
    }

    #[async_trait::async_trait]
    impl CoreWorkerClient for RecordingCoreClient {
        async fn register_worker(
            &self,
            _: RegisterWorkerRequest,
        ) -> Result<RegisterWorkerResponse, Status> {
            Ok(RegisterWorkerResponse::default())
        }

        async fn report_heartbeat(
            &self,
            _: ReportHeartbeatRequest,
        ) -> Result<ReportHeartbeatResponse, Status> {
            Ok(ReportHeartbeatResponse::default())
        }

        async fn report_execution(
            &self,
            report: ReportExecutionRequest,
        ) -> Result<ReportExecutionResponse, Status> {
            self.reports.lock().unwrap().push(report);
            match self.behavior {
                CoreBehavior::Accept => Ok(ReportExecutionResponse { accepted: true }),
                CoreBehavior::Reject => Ok(ReportExecutionResponse { accepted: false }),
                CoreBehavior::Unavailable => Err(Status::unavailable("Core unavailable")),
            }
        }
    }

    fn assignment() -> AssignExecutionRequest {
        AssignExecutionRequest {
            assignment_id: "assignment-1".into(),
            execution_id: "execution-1".into(),
            job_id: "job-1".into(),
            serialization_format: "json".into(),
            ..Default::default()
        }
    }

    fn report(state: ExecutionState, sequence_number: u64) -> ReportExecutionRequest {
        ReportExecutionRequest {
            report_id: format!("report-{sequence_number}"),
            assignment_id: "assignment-1".into(),
            execution_id: "execution-1".into(),
            sequence_number,
            state: state.into(),
            ..Default::default()
        }
    }

    async fn acquired_service(client: Arc<RecordingCoreClient>) -> WorkerRuntimeService {
        let pending = PendingExecutions::default();
        pending.enqueue(assignment()).await.unwrap();
        pending.acquire().await;
        WorkerRuntimeService::new(pending, client)
    }

    #[tokio::test]
    async fn running_report_is_forwarded_exactly_once_without_changes() {
        let client = Arc::new(RecordingCoreClient::default());
        let service = acquired_service(client.clone()).await;
        let mut running = report(ExecutionState::Running, 1);
        running.observed_at = Some(prost_types::Timestamp {
            seconds: 123,
            nanos: 456,
        });

        let response = service
            .report_execution(Request::new(running.clone()))
            .await
            .unwrap()
            .into_inner();

        assert!(response.accepted);
        assert_eq!(*client.reports.lock().unwrap(), [running]);
    }

    #[tokio::test]
    async fn terminal_outcomes_are_forwarded_without_changes() {
        let success_client = Arc::new(RecordingCoreClient::default());
        let success_service = acquired_service(success_client.clone()).await;
        success_service
            .report_execution(Request::new(report(ExecutionState::Running, 1)))
            .await
            .unwrap();
        let mut succeeded = report(ExecutionState::Succeeded, 2);
        succeeded.outcome = Some(JobResult {
            payload: br#"{"answer":42}"#.to_vec(),
            serialization_format: "application/json+threadweave".into(),
            ..Default::default()
        });
        success_service
            .report_execution(Request::new(succeeded.clone()))
            .await
            .unwrap();
        assert_eq!(success_client.reports.lock().unwrap()[1], succeeded);

        let failure_client = Arc::new(RecordingCoreClient::default());
        let failure_service = acquired_service(failure_client.clone()).await;
        failure_service
            .report_execution(Request::new(report(ExecutionState::Running, 1)))
            .await
            .unwrap();
        let mut failed = report(ExecutionState::Failed, 2);
        failed.outcome = Some(JobResult {
            serialization_format: "json".into(),
            failure: Some(Error {
                code: "USER_ERROR".into(),
                message: "boom".into(),
                metadata: None,
            }),
            ..Default::default()
        });
        failure_service
            .report_execution(Request::new(failed.clone()))
            .await
            .unwrap();
        assert_eq!(failure_client.reports.lock().unwrap()[1], failed);
    }

    #[tokio::test]
    async fn invalid_lifecycle_and_stale_reports_are_not_forwarded() {
        let client = Arc::new(RecordingCoreClient::default());
        let service = acquired_service(client.clone()).await;

        let invalid = service
            .report_execution(Request::new(report(ExecutionState::Succeeded, 1)))
            .await
            .unwrap_err();
        assert_eq!(invalid.code(), tonic::Code::FailedPrecondition);
        assert!(client.reports.lock().unwrap().is_empty());

        service
            .report_execution(Request::new(report(ExecutionState::Running, 2)))
            .await
            .unwrap();
        let stale = service
            .report_execution(Request::new(report(ExecutionState::Running, 2)))
            .await
            .unwrap_err();
        assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
        assert_eq!(client.reports.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn core_rejection_and_unavailability_are_returned_to_runtime() {
        let rejecting = Arc::new(RecordingCoreClient {
            behavior: CoreBehavior::Reject,
            ..Default::default()
        });
        let rejected = acquired_service(rejecting)
            .await
            .report_execution(Request::new(report(ExecutionState::Running, 1)))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::Internal);

        let unavailable = Arc::new(RecordingCoreClient {
            behavior: CoreBehavior::Unavailable,
            ..Default::default()
        });
        let unavailable = acquired_service(unavailable)
            .await
            .report_execution(Request::new(report(ExecutionState::Running, 1)))
            .await
            .unwrap_err();
        assert_eq!(unavailable.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn enqueue_acquire_running_and_succeeded_reach_core_mock() {
        let client = Arc::new(RecordingCoreClient::default());
        let pending = PendingExecutions::default();
        pending.enqueue(assignment()).await.unwrap();
        let service = WorkerRuntimeService::new(pending, client.clone());
        let acquired = service
            .acquire_execution(Request::new(AcquireExecutionRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .assignment
            .unwrap();
        assert_eq!(acquired, assignment());

        service
            .report_execution(Request::new(report(ExecutionState::Running, 1)))
            .await
            .unwrap();
        let mut succeeded = report(ExecutionState::Succeeded, 2);
        succeeded.outcome = Some(JobResult {
            payload: b"result".to_vec(),
            serialization_format: "bytes".into(),
            ..Default::default()
        });
        service
            .report_execution(Request::new(succeeded.clone()))
            .await
            .unwrap();

        let forwarded = client.reports.lock().unwrap();
        assert_eq!(forwarded.len(), 2);
        assert_eq!(forwarded[0].state, i32::from(ExecutionState::Running));
        assert_eq!(forwarded[1], succeeded);
    }

    #[tokio::test]
    async fn acquire_waits_and_preserves_assignment_identity() {
        let pending = PendingExecutions::default();
        let waiter = tokio::spawn({
            let pending = pending.clone();
            async move { pending.acquire().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        pending.enqueue(assignment()).await.unwrap();
        let acquired = waiter.await.unwrap();
        assert_eq!(acquired.assignment_id, "assignment-1");
        assert_eq!(acquired.execution_id, "execution-1");
        assert_eq!(acquired.job_id, "job-1");
    }

    #[tokio::test]
    async fn running_and_succeeded_reports_are_accepted() {
        let pending = PendingExecutions::default();
        pending.enqueue(assignment()).await.unwrap();
        pending.acquire().await;
        pending
            .report(report(ExecutionState::Running, 1))
            .await
            .unwrap();
        let mut succeeded = report(ExecutionState::Succeeded, 2);
        succeeded.outcome = Some(JobResult {
            payload: br#"{"answer":42}"#.to_vec(),
            serialization_format: "json".into(),
            ..Default::default()
        });
        pending.report(succeeded).await.unwrap();
    }

    #[tokio::test]
    async fn failed_report_is_accepted() {
        let pending = PendingExecutions::default();
        pending.enqueue(assignment()).await.unwrap();
        pending.acquire().await;
        pending
            .report(report(ExecutionState::Running, 1))
            .await
            .unwrap();
        let mut failed = report(ExecutionState::Failed, 2);
        failed.outcome = Some(JobResult {
            failure: Some(Error {
                code: "USER_ERROR".into(),
                message: "boom".into(),
                metadata: None,
            }),
            ..Default::default()
        });
        pending.report(failed).await.unwrap();
    }

    #[tokio::test]
    async fn unknown_mismatched_and_stale_reports_are_rejected() {
        let pending = PendingExecutions::default();
        let unknown = pending
            .report(report(ExecutionState::Running, 1))
            .await
            .unwrap_err();
        assert_eq!(unknown.code(), tonic::Code::NotFound);

        pending.enqueue(assignment()).await.unwrap();
        pending.acquire().await;
        let mut mismatched = report(ExecutionState::Running, 1);
        mismatched.execution_id = "another-execution".into();
        assert_eq!(
            pending.report(mismatched).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        pending
            .report(report(ExecutionState::Running, 2))
            .await
            .unwrap();
        assert_eq!(
            pending
                .report(report(ExecutionState::Running, 2))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
