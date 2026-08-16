use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{Mutex, Notify, mpsc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::CoreWorkerClient;
use crate::protocols::execution::v1::{ExecutionState, JobResult};
use crate::protocols::runtime::v1::runtime_event::Payload as EventPayload;
use crate::protocols::runtime::v1::runtime_service_server::RuntimeService;
use crate::protocols::runtime::v1::worker_command::Payload as CommandPayload;
use crate::protocols::runtime::v1::{
    AssignExecutionRequest, AssignExecutionResponse, CancelExecution, ExecutionCompleted,
    ExecutionFailed, ExecutionMetrics, ExecutionProgress, ExecutionStarted, RegisterRuntimeRequest,
    RegisterRuntimeResponse, RegisterWorkerRequest, RegisterWorkerResponse, ReportExecutionRequest,
    ReportExecutionResponse, ReportHeartbeatRequest, ReportHeartbeatResponse, RuntimeEvent,
    WorkerCommand,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssignmentState {
    Pending,
    Assigned,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug)]
struct AssignmentRecord {
    assignment: AssignExecutionRequest,
    state: AssignmentState,
    last_sequence_number: Option<u64>,
    session_id: Option<String>,
    started_at: Option<Instant>,
    worker_duration: Option<Duration>,
    metrics: Vec<ExecutionMetrics>,
}

#[derive(Debug, Default)]
struct PendingState {
    queue: VecDeque<String>,
    assignments: HashMap<String, AssignmentRecord>,
    sessions: HashMap<String, mpsc::Sender<Result<WorkerCommand, Status>>>,
}

/// Worker-owned channel broker between assignment delivery and runtime sessions.
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
        let id = assignment.assignment_id.clone();
        let mut state = self.state.lock().await;
        if state.assignments.contains_key(&id) {
            return Err(Status::already_exists("assignment already exists"));
        }
        state.queue.push_back(id.clone());
        state.assignments.insert(
            id,
            AssignmentRecord {
                assignment,
                state: AssignmentState::Pending,
                last_sequence_number: None,
                session_id: None,
                started_at: None,
                worker_duration: None,
                metrics: Vec::new(),
            },
        );
        drop(state);
        self.available.notify_one();
        Ok(())
    }

    async fn register_session(
        &self,
        id: String,
        sender: mpsc::Sender<Result<WorkerCommand, Status>>,
    ) {
        self.state.lock().await.sessions.insert(id, sender);
    }

    async fn next_for(&self, session_id: &str) -> AssignExecutionRequest {
        loop {
            let notified = self.available.notified();
            {
                let mut state = self.state.lock().await;
                if let Some(id) = state.queue.pop_front() {
                    let record = state
                        .assignments
                        .get_mut(&id)
                        .expect("queued assignment exists");
                    record.state = AssignmentState::Assigned;
                    record.session_id = Some(session_id.to_owned());
                    return record.assignment.clone();
                }
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub async fn acquire(&self) -> AssignExecutionRequest {
        self.next_for("test-runtime-session").await
    }

    async fn disconnect(&self, session_id: &str) {
        let mut requeued = false;
        let mut state = self.state.lock().await;
        state.sessions.remove(session_id);
        let ids: Vec<String> = state
            .assignments
            .iter()
            .filter(|(_, record)| record.session_id.as_deref() == Some(session_id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let record = state
                .assignments
                .get_mut(&id)
                .expect("selected record exists");
            record.session_id = None;
            if record.state == AssignmentState::Assigned {
                record.state = AssignmentState::Pending;
                state.queue.push_back(id);
                requeued = true;
            } else if record.state == AssignmentState::Running {
                warn!(assignment_id = %record.assignment.assignment_id,
                    execution_id = %record.assignment.execution_id,
                    "runtime disconnected with execution running; execution retained for reconciliation");
            }
        }
        drop(state);
        if requeued {
            self.available.notify_waiters();
        }
    }

    pub async fn cancel(&self, assignment_id: &str, reason: Option<String>) -> Result<(), Status> {
        let (sender, command) = {
            let state = self.state.lock().await;
            let record = state
                .assignments
                .get(assignment_id)
                .ok_or_else(|| Status::not_found("assignment not found"))?;
            let session_id = record.session_id.as_ref().ok_or_else(|| {
                Status::failed_precondition("assignment has no connected runtime")
            })?;
            let sender = state
                .sessions
                .get(session_id)
                .cloned()
                .ok_or_else(|| Status::unavailable("runtime session disconnected"))?;
            let cancel = CancelExecution {
                assignment_id: assignment_id.to_owned(),
                execution_id: record.assignment.execution_id.clone(),
                reason,
            };
            (
                sender,
                WorkerCommand {
                    payload: Some(CommandPayload::CancelExecution(cancel)),
                },
            )
        };
        sender
            .send(Ok(command))
            .await
            .map_err(|_| Status::unavailable("runtime session disconnected"))
    }

    async fn validate<'a>(
        state: &'a mut PendingState,
        assignment_id: &str,
        execution_id: &str,
        sequence: u64,
    ) -> Result<&'a mut AssignmentRecord, Status> {
        let record = state
            .assignments
            .get_mut(assignment_id)
            .ok_or_else(|| Status::not_found("assignment not found"))?;
        if record.assignment.execution_id != execution_id {
            return Err(Status::failed_precondition(
                "execution_id does not match assignment",
            ));
        }
        if record
            .last_sequence_number
            .is_some_and(|last| sequence <= last)
        {
            return Err(Status::failed_precondition("stale execution event"));
        }
        Ok(record)
    }

    async fn started(&self, event: &ExecutionStarted) -> Result<AssignExecutionRequest, Status> {
        let mut state = self.state.lock().await;
        let record = Self::validate(
            &mut state,
            &event.assignment_id,
            &event.execution_id,
            event.sequence_number,
        )
        .await?;
        if record.state != AssignmentState::Assigned {
            return Err(Status::failed_precondition(
                "invalid assignment lifecycle transition",
            ));
        }
        record.state = AssignmentState::Running;
        record.last_sequence_number = Some(event.sequence_number);
        record.started_at = Some(Instant::now());
        Ok(record.assignment.clone())
    }

    async fn progress(&self, event: &ExecutionProgress) -> Result<(), Status> {
        if !(0.0..=1.0).contains(&event.progress) {
            return Err(Status::invalid_argument(
                "progress must be between zero and one",
            ));
        }
        let mut state = self.state.lock().await;
        let record = Self::validate(
            &mut state,
            &event.assignment_id,
            &event.execution_id,
            event.sequence_number,
        )
        .await?;
        if record.state != AssignmentState::Running {
            return Err(Status::failed_precondition(
                "progress requires a running execution",
            ));
        }
        record.last_sequence_number = Some(event.sequence_number);
        Ok(())
    }

    async fn metrics(&self, event: ExecutionMetrics) -> Result<(), Status> {
        let mut state = self.state.lock().await;
        let record = Self::validate(
            &mut state,
            &event.assignment_id,
            &event.execution_id,
            event.sequence_number,
        )
        .await?;
        if record.state != AssignmentState::Running {
            return Err(Status::failed_precondition(
                "metrics require a running execution",
            ));
        }
        record.last_sequence_number = Some(event.sequence_number);
        record.metrics.push(event);
        Ok(())
    }

    async fn terminal(
        &self,
        assignment_id: &str,
        execution_id: &str,
        sequence: u64,
        succeeded: bool,
    ) -> Result<(AssignExecutionRequest, Duration), Status> {
        let mut state = self.state.lock().await;
        let record = Self::validate(&mut state, assignment_id, execution_id, sequence).await?;
        if record.state != AssignmentState::Running {
            return Err(Status::failed_precondition(
                "invalid assignment lifecycle transition",
            ));
        }
        let duration = record
            .started_at
            .ok_or_else(|| Status::failed_precondition("execution was not started"))?
            .elapsed();
        record.state = if succeeded {
            AssignmentState::Succeeded
        } else {
            AssignmentState::Failed
        };
        record.last_sequence_number = Some(sequence);
        record.worker_duration = Some(duration);
        record.session_id = None;
        Ok((record.assignment.clone(), duration))
    }

    #[cfg(test)]
    async fn worker_duration(&self, id: &str) -> Option<Duration> {
        self.state
            .lock()
            .await
            .assignments
            .get(id)
            .and_then(|r| r.worker_duration)
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

    async fn forward(
        &self,
        report: ReportExecutionRequest,
        assignment: &AssignExecutionRequest,
    ) -> Result<(), Status> {
        let response = self.core_client.report_execution(report).await.map_err(|status| {
            error!(assignment_id = %assignment.assignment_id, execution_id = %assignment.execution_id,
                error = %status, "failed to forward execution event to core"); status
        })?;
        if !response.accepted {
            return Err(Status::internal("Core did not accept execution report"));
        }
        Ok(())
    }

    async fn handle_event(&self, event: RuntimeEvent) -> Result<(), Status> {
        match event
            .payload
            .ok_or_else(|| Status::invalid_argument("runtime event payload is required"))?
        {
            EventPayload::Ready(ready) => {
                info!(runtime_id = %ready.runtime_id, "runtime ready");
                Ok(())
            }
            EventPayload::Heartbeat(heartbeat) => {
                info!(runtime_id = %heartbeat.runtime_id, sequence_number = heartbeat.sequence_number, "runtime heartbeat");
                Ok(())
            }
            EventPayload::ExecutionStarted(started) => {
                let assignment = self.pending.started(&started).await?;
                info!(assignment_id = %started.assignment_id, execution_id = %started.execution_id, "execution started");
                self.forward(
                    report_for(
                        &started.assignment_id,
                        &started.execution_id,
                        started.sequence_number,
                        ExecutionState::Running,
                        None,
                        started.observed_at,
                    ),
                    &assignment,
                )
                .await
            }
            EventPayload::ExecutionProgress(progress) => {
                self.pending.progress(&progress).await?;
                info!(assignment_id = %progress.assignment_id, execution_id = %progress.execution_id,
                    progress = progress.progress, "execution progress");
                Ok(())
            }
            EventPayload::ExecutionMetrics(metrics) => self.pending.metrics(metrics).await,
            EventPayload::ExecutionCompleted(completed) => self.completed(completed).await,
            EventPayload::ExecutionFailed(failed) => self.failed(failed).await,
        }
    }

    async fn completed(&self, event: ExecutionCompleted) -> Result<(), Status> {
        let result = event
            .result
            .ok_or_else(|| Status::invalid_argument("completed event requires a result"))?;
        if result.failure.is_some() {
            return Err(Status::invalid_argument(
                "completed result cannot contain failure",
            ));
        }
        let (assignment, duration) = self
            .pending
            .terminal(
                &event.assignment_id,
                &event.execution_id,
                event.sequence_number,
                true,
            )
            .await?;
        info!(assignment_id = %event.assignment_id, execution_id = %event.execution_id,
            worker_elapsed_ms = duration.as_millis(), "execution completed");
        self.forward(
            report_for(
                &event.assignment_id,
                &event.execution_id,
                event.sequence_number,
                ExecutionState::Succeeded,
                Some(result),
                event.observed_at,
            ),
            &assignment,
        )
        .await
    }

    async fn failed(&self, event: ExecutionFailed) -> Result<(), Status> {
        let failure = event
            .failure
            .ok_or_else(|| Status::invalid_argument("failed event requires failure information"))?;
        let (assignment, duration) = self
            .pending
            .terminal(
                &event.assignment_id,
                &event.execution_id,
                event.sequence_number,
                false,
            )
            .await?;
        warn!(assignment_id = %event.assignment_id, execution_id = %event.execution_id,
            failure_code = %failure.code, worker_elapsed_ms = duration.as_millis(), "execution failed");
        self.forward(
            report_for(
                &event.assignment_id,
                &event.execution_id,
                event.sequence_number,
                ExecutionState::Failed,
                Some(JobResult {
                    failure: Some(failure),
                    ..Default::default()
                }),
                event.observed_at,
            ),
            &assignment,
        )
        .await
    }
}

fn report_for(
    assignment_id: &str,
    execution_id: &str,
    sequence_number: u64,
    state: ExecutionState,
    outcome: Option<JobResult>,
    observed_at: Option<prost_types::Timestamp>,
) -> ReportExecutionRequest {
    ReportExecutionRequest {
        report_id: Uuid::new_v4().to_string(),
        assignment_id: assignment_id.to_owned(),
        execution_id: execution_id.to_owned(),
        sequence_number,
        state: state.into(),
        outcome,
        observed_at: observed_at.or_else(|| Some(SystemTime::now().into())),
    }
}

#[tonic::async_trait]
impl RuntimeService for WorkerRuntimeService {
    type RuntimeSessionStream = Pin<Box<dyn Stream<Item = Result<WorkerCommand, Status>> + Send>>;

    async fn runtime_session(
        &self,
        request: Request<Streaming<RuntimeEvent>>,
    ) -> Result<Response<Self::RuntimeSessionStream>, Status> {
        let session_id = Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel(32);
        self.pending
            .register_session(session_id.clone(), sender.clone())
            .await;
        info!(%session_id, "runtime connected");
        let pending = self.pending.clone();
        let service = self.clone();
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = inbound.next() => match event {
                        Some(Ok(event)) => if let Err(status) = service.handle_event(event).await {
                            let _ = sender.send(Err(status)).await;
                            break;
                        },
                        Some(Err(status)) => { warn!(%session_id, error = %status, "runtime session read failed"); break; }
                        None => break,
                    },
                    assignment = pending.next_for(&session_id) => {
                        let command = WorkerCommand { payload: Some(CommandPayload::AssignExecution(assignment.clone())) };
                        if sender.send(Ok(command)).await.is_err() { break; }
                        info!(%session_id, assignment_id = %assignment.assignment_id,
                            execution_id = %assignment.execution_id, "assignment sent");
                    }
                }
            }
            pending.disconnect(&session_id).await;
            info!(%session_id, "runtime disconnected");
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn report_execution(
        &self,
        request: Request<ReportExecutionRequest>,
    ) -> Result<Response<ReportExecutionResponse>, Status> {
        let report = request.into_inner();
        let assignment = {
            let state = self.pending.state.lock().await;
            state
                .assignments
                .get(&report.assignment_id)
                .map(|r| r.assignment.clone())
                .ok_or_else(|| Status::not_found("assignment not found"))?
        };
        self.forward(report, &assignment).await?;
        Ok(Response::new(ReportExecutionResponse { accepted: true }))
    }
    async fn register_runtime(
        &self,
        _: Request<RegisterRuntimeRequest>,
    ) -> Result<Response<RegisterRuntimeResponse>, Status> {
        Err(Status::unimplemented(
            "registration is outside the runtime session",
        ))
    }
    async fn report_heartbeat(
        &self,
        _: Request<ReportHeartbeatRequest>,
    ) -> Result<Response<ReportHeartbeatResponse>, Status> {
        Err(Status::unimplemented(
            "runtime heartbeats use RuntimeSession",
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
            "assignments arrive through the broker",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::common::v1::Error;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct Core {
        reports: StdMutex<Vec<ReportExecutionRequest>>,
    }
    #[async_trait::async_trait]
    impl CoreWorkerClient for Core {
        async fn register_worker(
            &self,
            _: RegisterWorkerRequest,
        ) -> Result<RegisterWorkerResponse, Status> {
            unreachable!()
        }
        async fn report_heartbeat(
            &self,
            _: ReportHeartbeatRequest,
        ) -> Result<ReportHeartbeatResponse, Status> {
            unreachable!()
        }
        async fn report_execution(
            &self,
            r: ReportExecutionRequest,
        ) -> Result<ReportExecutionResponse, Status> {
            self.reports.lock().unwrap().push(r);
            Ok(ReportExecutionResponse { accepted: true })
        }
    }
    fn assignment() -> AssignExecutionRequest {
        AssignExecutionRequest {
            assignment_id: "a".into(),
            execution_id: "e".into(),
            job_id: "j".into(),
            ..Default::default()
        }
    }
    async fn assigned() -> (PendingExecutions, AssignExecutionRequest) {
        let pending = PendingExecutions::default();
        pending.enqueue(assignment()).await.unwrap();
        let item = pending.next_for("s").await;
        (pending, item)
    }

    #[tokio::test]
    async fn lifecycle_metrics_and_worker_duration_are_recorded() {
        let (pending, _) = assigned().await;
        pending
            .started(&ExecutionStarted {
                assignment_id: "a".into(),
                execution_id: "e".into(),
                sequence_number: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        pending
            .metrics(ExecutionMetrics {
                assignment_id: "a".into(),
                execution_id: "e".into(),
                sequence_number: 2,
                execution_ms: Some(4),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        pending.terminal("a", "e", 3, true).await.unwrap();
        assert!(pending.worker_duration("a").await.unwrap() >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn stale_and_invalid_transitions_are_rejected() {
        let (pending, _) = assigned().await;
        assert!(pending.terminal("a", "e", 1, true).await.is_err());
        let started = ExecutionStarted {
            assignment_id: "a".into(),
            execution_id: "e".into(),
            sequence_number: 2,
            ..Default::default()
        };
        pending.started(&started).await.unwrap();
        assert!(
            pending
                .progress(&ExecutionProgress {
                    assignment_id: "a".into(),
                    execution_id: "e".into(),
                    sequence_number: 2,
                    progress: 0.5,
                    ..Default::default()
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn disconnect_requeues_an_unstarted_assignment() {
        let (pending, _) = assigned().await;
        pending.disconnect("s").await;
        assert_eq!(pending.next_for("s2").await.assignment_id, "a");
    }

    #[tokio::test]
    async fn completion_and_failure_reach_core() {
        for success in [true, false] {
            let (pending, _) = assigned().await;
            pending
                .started(&ExecutionStarted {
                    assignment_id: "a".into(),
                    execution_id: "e".into(),
                    sequence_number: 1,
                    ..Default::default()
                })
                .await
                .unwrap();
            let core = Arc::new(Core::default());
            let service = WorkerRuntimeService::new(pending, core.clone());
            if success {
                service
                    .completed(ExecutionCompleted {
                        assignment_id: "a".into(),
                        execution_id: "e".into(),
                        sequence_number: 2,
                        result: Some(JobResult::default()),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            } else {
                service
                    .failed(ExecutionFailed {
                        assignment_id: "a".into(),
                        execution_id: "e".into(),
                        sequence_number: 2,
                        failure: Some(Error {
                            code: "boom".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(core.reports.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_is_sent_without_holding_state_lock() {
        let pending = PendingExecutions::default();
        let (tx, mut rx) = mpsc::channel(1);
        pending.register_session("s".into(), tx).await;
        pending.enqueue(assignment()).await.unwrap();
        pending.next_for("s").await;
        pending.cancel("a", Some("stop".into())).await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().payload,
            Some(CommandPayload::CancelExecution(_))
        ));
    }
}
