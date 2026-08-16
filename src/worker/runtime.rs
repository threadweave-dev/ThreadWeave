use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};

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

    pub async fn report(&self, report: ReportExecutionRequest) -> Result<(), Status> {
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
        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkerRuntimeService {
    pending: PendingExecutions,
}

impl WorkerRuntimeService {
    pub fn new(pending: PendingExecutions) -> Self {
        Self { pending }
    }
}

#[tonic::async_trait]
impl RuntimeService for WorkerRuntimeService {
    async fn acquire_execution(
        &self,
        _request: Request<AcquireExecutionRequest>,
    ) -> Result<Response<AcquireExecutionResponse>, Status> {
        Ok(Response::new(AcquireExecutionResponse {
            assignment: Some(self.pending.acquire().await),
        }))
    }

    async fn report_execution(
        &self,
        request: Request<ReportExecutionRequest>,
    ) -> Result<Response<ReportExecutionResponse>, Status> {
        self.pending.report(request.into_inner()).await?;
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
