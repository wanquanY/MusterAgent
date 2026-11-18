use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use musteragent_runtime_contract::ErrorCode;

use crate::{
    AttemptTelemetryContext, CommandOperation, CommandTelemetryContext, CommandTerminalOutcome,
    DataEdgeOperation, DataEdgeTelemetryContext, DbCommitKind, DbTransactionPhase,
    DbTransactionResult, ErrorClass, InvocationOperation, InvocationTelemetryContext,
    TelemetryEventKind, TelemetryOperation, TelemetryOutcome, WaitReasonClass,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MachineEventIdentity {
    Runtime,
    Command(CommandTelemetryContext),
    Attempt(AttemptTelemetryContext),
    Invocation(InvocationTelemetryContext),
    DataEdge(DataEdgeTelemetryContext),
    DbCommit(DbCommitTelemetryContext),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DbCommitTelemetryContext {
    pub tenant_id: musteragent_identity::TenantId,
    pub conversation_id: musteragent_identity::ConversationId,
    pub correlation_id: musteragent_identity::CorrelationId,
    pub kind: DbCommitKind,
}

/// A sealed machine event. Callers choose a typed constructor, never a string
/// target, span name, event key, field name, payload, or message.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TypedMachineEvent {
    kind: TelemetryEventKind,
    operation: TelemetryOperation,
    outcome: TelemetryOutcome,
    error_code: Option<ErrorCode>,
    error_class: Option<ErrorClass>,
    identity: MachineEventIdentity,
    observation_id: Option<musteragent_identity::LedgerRecordId>,
}

impl TypedMachineEvent {
    #[must_use]
    fn runtime_started() -> Self {
        Self {
            kind: TelemetryEventKind::RuntimeStarted,
            operation: TelemetryOperation::RuntimeBootstrap,
            outcome: TelemetryOutcome::Success,
            error_code: None,
            error_class: None,
            identity: MachineEventIdentity::Runtime,
            observation_id: None,
        }
    }

    #[must_use]
    fn command(
        context: CommandTelemetryContext,
        terminal: CommandTerminalOutcome,
        error: Option<ErrorCode>,
    ) -> Self {
        let kind = match terminal {
            CommandTerminalOutcome::Completed => TelemetryEventKind::CommandCompleted,
            CommandTerminalOutcome::Rejected => TelemetryEventKind::CommandRejected,
            CommandTerminalOutcome::Failed => TelemetryEventKind::CommandFailed,
        };
        Self::new(
            kind,
            TelemetryOperation::CommandGeneral,
            terminal.telemetry_outcome(),
            error,
            MachineEventIdentity::Command(context),
        )
    }

    #[must_use]
    fn attempt(
        context: AttemptTelemetryContext,
        terminal: bool,
        outcome: TelemetryOutcome,
        error: Option<ErrorCode>,
    ) -> Self {
        Self::new(
            if terminal {
                TelemetryEventKind::AttemptCompleted
            } else {
                TelemetryEventKind::AttemptLifecycleObserved
            },
            TelemetryOperation::AttemptExecute,
            outcome,
            error,
            MachineEventIdentity::Attempt(context),
        )
    }

    #[must_use]
    fn invocation(
        context: InvocationTelemetryContext,
        operation: InvocationOperation,
        outcome: TelemetryOutcome,
        error: Option<ErrorCode>,
        observation_id: Option<musteragent_identity::LedgerRecordId>,
    ) -> Self {
        let (kind, operation_key) = match operation {
            InvocationOperation::Model => (
                TelemetryEventKind::ModelInvocationCompleted,
                TelemetryOperation::Model,
            ),
            InvocationOperation::Tool => (
                TelemetryEventKind::ToolInvocationCompleted,
                TelemetryOperation::Tool,
            ),
        };
        let mut event = Self::new(
            kind,
            operation_key,
            outcome,
            error,
            MachineEventIdentity::Invocation(context),
        );
        event.observation_id = observation_id;
        event
    }

    #[must_use]
    fn tool_materialization_failed(context: InvocationTelemetryContext, error: ErrorCode) -> Self {
        Self::new(
            TelemetryEventKind::ToolMaterializationFailed,
            TelemetryOperation::ToolMaterialization,
            TelemetryOutcome::Error,
            Some(error),
            MachineEventIdentity::Invocation(context),
        )
    }

    #[must_use]
    fn tool_observation_failed(context: InvocationTelemetryContext, error: ErrorCode) -> Self {
        Self::new(
            TelemetryEventKind::ToolObservationFailed,
            TelemetryOperation::ToolObservation,
            TelemetryOutcome::Error,
            Some(error),
            MachineEventIdentity::Invocation(context),
        )
    }

    #[must_use]
    fn data_edge(
        context: DataEdgeTelemetryContext,
        operation: DataEdgeOperation,
        outcome: TelemetryOutcome,
        error_class: Option<ErrorClass>,
    ) -> Self {
        Self {
            kind: TelemetryEventKind::DataEdgeCompleted,
            operation: data_edge_operation(operation),
            outcome,
            error_code: None,
            error_class,
            identity: MachineEventIdentity::DataEdge(context),
            observation_id: None,
        }
    }

    #[must_use]
    fn db_commit(context: DbCommitTelemetryContext, outcome: TelemetryOutcome) -> Self {
        Self {
            kind: TelemetryEventKind::DbCommitCompleted,
            operation: TelemetryOperation::DbCommit,
            outcome,
            error_code: None,
            error_class: None,
            identity: MachineEventIdentity::DbCommit(context),
            observation_id: None,
        }
    }

    fn new(
        kind: TelemetryEventKind,
        operation: TelemetryOperation,
        outcome: TelemetryOutcome,
        error_code: Option<ErrorCode>,
        identity: MachineEventIdentity,
    ) -> Self {
        Self {
            kind,
            operation,
            outcome,
            error_class: error_code.map(ErrorClass::from_error_code),
            error_code,
            identity,
            observation_id: None,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> TelemetryEventKind {
        self.kind
    }

    #[must_use]
    pub const fn operation(&self) -> TelemetryOperation {
        self.operation
    }

    #[must_use]
    pub const fn outcome(&self) -> TelemetryOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn error_code(&self) -> Option<ErrorCode> {
        self.error_code
    }

    #[must_use]
    pub const fn error_class(&self) -> Option<ErrorClass> {
        self.error_class
    }

    #[must_use]
    pub const fn identity(&self) -> &MachineEventIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn observation_id(&self) -> Option<musteragent_identity::LedgerRecordId> {
        self.observation_id
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RuntimeSpanDescriptor {
    RuntimeBootstrap,
    Command {
        operation: CommandOperation,
        context: CommandTelemetryContext,
    },
    Attempt(AttemptTelemetryContext),
    Invocation {
        operation: InvocationOperation,
        context: InvocationTelemetryContext,
    },
    DataEdge {
        operation: DataEdgeOperation,
        context: DataEdgeTelemetryContext,
    },
    DbCommit(DbCommitTelemetryContext),
}

pub trait RuntimeSpanBackend: Send + Sync {
    fn record_outcome(&self, outcome: TelemetryOutcome);
    fn record_error(&self, error: ErrorClass);
    fn record_wait(&self, wait: WaitReasonClass);
    fn record_baggage(&self, baggage: &crate::ControlledBaggage);
    fn record_observation_id(&self, observation_id: musteragent_identity::LedgerRecordId);
    fn in_scope(&self, callback: &mut dyn FnMut());
}

#[derive(Clone)]
pub struct RuntimeSpan {
    backend: Option<Arc<dyn RuntimeSpanBackend>>,
}

impl RuntimeSpan {
    #[must_use]
    pub const fn none() -> Self {
        Self { backend: None }
    }

    #[must_use]
    pub fn from_backend(backend: Arc<dyn RuntimeSpanBackend>) -> Self {
        Self {
            backend: Some(backend),
        }
    }

    pub fn record_outcome(&self, outcome: TelemetryOutcome) {
        if let Some(backend) = &self.backend {
            backend.record_outcome(outcome);
        }
    }

    pub fn record_error(&self, error: ErrorClass) {
        if let Some(backend) = &self.backend {
            backend.record_error(error);
        }
    }

    pub fn record_wait(&self, wait: WaitReasonClass) {
        if let Some(backend) = &self.backend {
            backend.record_wait(wait);
        }
    }

    pub fn record_baggage(&self, baggage: &crate::ControlledBaggage) {
        if let Some(backend) = &self.backend {
            backend.record_baggage(baggage);
        }
    }

    pub fn record_observation_id(&self, observation_id: musteragent_identity::LedgerRecordId) {
        if let Some(backend) = &self.backend {
            backend.record_observation_id(observation_id);
        }
    }

    pub fn in_scope<R>(&self, callback: impl FnOnce() -> R) -> R {
        let mut callback = Some(callback);
        let mut result = None;
        let mut invoke = || {
            result = Some(callback
                .take()
                .expect("Runtime span callback is invoked once")(
            ));
        };
        if let Some(backend) = &self.backend {
            backend.in_scope(&mut invoke);
        } else {
            invoke();
        }
        result.expect("Runtime span callback completed")
    }

    pub fn instrument<'a, F, T>(&self, future: F) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        Box::pin(RuntimeSpanFuture {
            span: self.clone(),
            future: Box::pin(future),
        })
    }
}

struct RuntimeSpanFuture<'a, T> {
    span: RuntimeSpan,
    future: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
}

impl<T> Future for RuntimeSpanFuture<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut();
        inner.span.in_scope(|| inner.future.as_mut().poll(context))
    }
}

pub trait MachineTelemetryBackend: Send + Sync {
    fn start_span(&self, descriptor: RuntimeSpanDescriptor) -> RuntimeSpan;
    fn emit(&self, event: TypedMachineEvent);
    fn record_data_edge(
        &self,
        operation: DataEdgeOperation,
        outcome: TelemetryOutcome,
        duration: Duration,
    );
    fn record_db_commit(&self, kind: DbCommitKind, outcome: TelemetryOutcome, duration: Duration);
    fn record_db_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    );
}

/// The full process capability is created by telemetry bootstrap and retained
/// by application composition. Runtime producers receive only the narrower
/// handle matching the fact they author; there is no process-global registry
/// or free emit/start function.
#[derive(Clone)]
#[doc(hidden)]
pub struct MachineTelemetry {
    backend: Arc<dyn MachineTelemetryBackend>,
}

impl MachineTelemetry {
    #[doc(hidden)]
    #[must_use]
    pub fn from_backend(backend: Arc<dyn MachineTelemetryBackend>) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn runtime(&self) -> RuntimeTelemetry {
        RuntimeTelemetry::new(&self.backend)
    }

    #[must_use]
    pub fn command(&self) -> CommandTelemetry {
        CommandTelemetry::new(&self.backend)
    }

    #[must_use]
    pub fn attempt(&self) -> AttemptTelemetry {
        AttemptTelemetry::new(&self.backend)
    }

    #[must_use]
    pub fn invocation(&self) -> InvocationTelemetry {
        InvocationTelemetry::new(&self.backend)
    }

    #[must_use]
    pub fn data_edge(&self) -> DataEdgeTelemetry {
        DataEdgeTelemetry::new(&self.backend)
    }

    #[must_use]
    pub fn db_commit(&self) -> DbCommitTelemetry {
        DbCommitTelemetry::new(&self.backend)
    }
}

macro_rules! producer_handle {
    ($name:ident) => {
        #[derive(Clone)]
        pub struct $name {
            backend: Arc<dyn MachineTelemetryBackend>,
        }

        impl $name {
            fn new(backend: &Arc<dyn MachineTelemetryBackend>) -> Self {
                Self {
                    backend: Arc::clone(backend),
                }
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!($name))
            }
        }
    };
}

producer_handle!(RuntimeTelemetry);
producer_handle!(CommandTelemetry);
producer_handle!(AttemptTelemetry);
producer_handle!(InvocationTelemetry);
producer_handle!(DataEdgeTelemetry);
producer_handle!(DbCommitTelemetry);

impl RuntimeTelemetry {
    #[must_use]
    pub fn span(&self) -> RuntimeSpan {
        self.backend
            .start_span(RuntimeSpanDescriptor::RuntimeBootstrap)
    }

    pub fn observe_started(&self) {
        self.backend.emit(TypedMachineEvent::runtime_started());
    }
}

impl CommandTelemetry {
    #[must_use]
    pub fn span(
        &self,
        operation: CommandOperation,
        context: &CommandTelemetryContext,
    ) -> RuntimeSpan {
        self.backend.start_span(RuntimeSpanDescriptor::Command {
            operation,
            context: context.clone(),
        })
    }

    pub fn observe(
        &self,
        context: &CommandTelemetryContext,
        terminal: CommandTerminalOutcome,
        error: Option<ErrorCode>,
    ) {
        self.backend
            .emit(TypedMachineEvent::command(context.clone(), terminal, error));
    }
}

impl AttemptTelemetry {
    #[must_use]
    pub fn span(&self, context: &AttemptTelemetryContext) -> RuntimeSpan {
        self.backend
            .start_span(RuntimeSpanDescriptor::Attempt(context.clone()))
    }

    pub fn observe(
        &self,
        context: &AttemptTelemetryContext,
        terminal: bool,
        outcome: TelemetryOutcome,
        error: Option<ErrorCode>,
    ) {
        self.backend.emit(TypedMachineEvent::attempt(
            context.clone(),
            terminal,
            outcome,
            error,
        ));
    }
}

impl InvocationTelemetry {
    #[must_use]
    pub fn span(
        &self,
        operation: InvocationOperation,
        context: &InvocationTelemetryContext,
    ) -> RuntimeSpan {
        self.backend.start_span(RuntimeSpanDescriptor::Invocation {
            operation,
            context: context.clone(),
        })
    }

    pub fn observe(
        &self,
        operation: InvocationOperation,
        context: &InvocationTelemetryContext,
        outcome: TelemetryOutcome,
        error: Option<ErrorCode>,
        observation_id: Option<musteragent_identity::LedgerRecordId>,
    ) {
        self.backend.emit(TypedMachineEvent::invocation(
            context.clone(),
            operation,
            outcome,
            error,
            observation_id,
        ));
    }

    pub fn observe_materialization_failure(
        &self,
        context: &InvocationTelemetryContext,
        error: ErrorCode,
    ) {
        self.backend
            .emit(TypedMachineEvent::tool_materialization_failed(
                context.clone(),
                error,
            ));
    }

    pub fn observe_tool_observation_failure(
        &self,
        context: &InvocationTelemetryContext,
        error: ErrorCode,
    ) {
        self.backend
            .emit(TypedMachineEvent::tool_observation_failed(
                context.clone(),
                error,
            ));
    }
}

impl DataEdgeTelemetry {
    #[must_use]
    pub fn span(
        &self,
        operation: DataEdgeOperation,
        context: &DataEdgeTelemetryContext,
    ) -> RuntimeSpan {
        self.backend.start_span(RuntimeSpanDescriptor::DataEdge {
            operation,
            context: context.clone(),
        })
    }

    pub fn observe(
        &self,
        context: &DataEdgeTelemetryContext,
        operation: DataEdgeOperation,
        outcome: TelemetryOutcome,
        error_class: Option<ErrorClass>,
        duration: Duration,
    ) {
        self.backend.record_data_edge(operation, outcome, duration);
        // Successful request volume and latency belong to low-cardinality
        // Metrics and sampled Traces. A per-request success log duplicates
        // both signals and overwhelms production Logs during Snapshot hydration.
        // Failures remain durable structured events for incident investigation.
        if outcome == TelemetryOutcome::Error {
            self.backend.emit(TypedMachineEvent::data_edge(
                context.clone(),
                operation,
                outcome,
                error_class,
            ));
        }
    }
}

impl DbCommitTelemetry {
    #[must_use]
    pub fn span(&self, context: &DbCommitTelemetryContext) -> RuntimeSpan {
        self.backend
            .start_span(RuntimeSpanDescriptor::DbCommit(context.clone()))
    }

    pub fn observe(
        &self,
        context: &DbCommitTelemetryContext,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) {
        self.backend
            .record_db_commit(context.kind, outcome, duration);
        // Successful commit volume and latency are already represented by the
        // low-cardinality DB commit metric and sampled spans. Logging every
        // successful Ledger + Outbox transaction floods operator consoles on
        // active conversations without adding incident evidence. Preserve all
        // abnormal outcomes as joinable structured events.
        if outcome != TelemetryOutcome::Success {
            self.backend
                .emit(TypedMachineEvent::db_commit(context.clone(), outcome));
        }
    }

    pub fn observe_phase(
        &self,
        context: &DbCommitTelemetryContext,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        self.backend
            .record_db_transaction(context.kind, phase, result, duration);
    }

    /// Records a governed transaction phase when the Unit of Work has no
    /// Conversation-scoped identity. Transaction kind, phase and result stay
    /// closed and low-cardinality; request identities remain in the caller's
    /// trace rather than being invented by infrastructure.
    pub fn observe_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        if phase == DbTransactionPhase::Commit {
            let outcome = match result {
                DbTransactionResult::Success => TelemetryOutcome::Success,
                DbTransactionResult::CommitUnknown => TelemetryOutcome::ExternalOutcomeUnknown,
                DbTransactionResult::Aborted
                | DbTransactionResult::Error
                | DbTransactionResult::Timeout
                | DbTransactionResult::Deadlock
                | DbTransactionResult::SerializationFailure
                | DbTransactionResult::Constraint
                | DbTransactionResult::Unavailable => TelemetryOutcome::Error,
            };
            self.backend.record_db_commit(kind, outcome, duration);
        }
        self.backend
            .record_db_transaction(kind, phase, result, duration);
    }
}

const fn data_edge_operation(operation: DataEdgeOperation) -> TelemetryOperation {
    match operation {
        DataEdgeOperation::ProjectionRead => TelemetryOperation::DataEdgeProjectionRead,
        DataEdgeOperation::ConversationBootstrapRead => {
            TelemetryOperation::DataEdgeConversationBootstrapRead
        }
        DataEdgeOperation::RecordList => TelemetryOperation::DataEdgeRecordList,
        DataEdgeOperation::RecordSubscribe => TelemetryOperation::DataEdgeRecordSubscribe,
        DataEdgeOperation::ContentRead => TelemetryOperation::DataEdgeContentRead,
        DataEdgeOperation::ContentBatchRead => TelemetryOperation::DataEdgeContentBatchRead,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingBackend {
        events: Mutex<Vec<TypedMachineEvent>>,
        data_edge: Mutex<Vec<(DataEdgeOperation, TelemetryOutcome, Duration)>>,
        db_commit: Mutex<Vec<(DbCommitKind, TelemetryOutcome, Duration)>>,
        db_transaction: Mutex<
            Vec<(
                DbCommitKind,
                DbTransactionPhase,
                DbTransactionResult,
                Duration,
            )>,
        >,
    }

    impl MachineTelemetryBackend for RecordingBackend {
        fn start_span(&self, _descriptor: RuntimeSpanDescriptor) -> RuntimeSpan {
            RuntimeSpan::none()
        }

        fn emit(&self, event: TypedMachineEvent) {
            self.events.lock().expect("event lock").push(event);
        }

        fn record_data_edge(
            &self,
            operation: DataEdgeOperation,
            outcome: TelemetryOutcome,
            duration: Duration,
        ) {
            self.data_edge
                .lock()
                .expect("data edge metric lock")
                .push((operation, outcome, duration));
        }

        fn record_db_commit(
            &self,
            kind: DbCommitKind,
            outcome: TelemetryOutcome,
            duration: Duration,
        ) {
            self.db_commit
                .lock()
                .expect("DB commit metric lock")
                .push((kind, outcome, duration));
        }

        fn record_db_transaction(
            &self,
            kind: DbCommitKind,
            phase: DbTransactionPhase,
            result: DbTransactionResult,
            duration: Duration,
        ) {
            self.db_transaction
                .lock()
                .expect("DB transaction metric lock")
                .push((kind, phase, result, duration));
        }
    }

    fn data_edge_context() -> DataEdgeTelemetryContext {
        DataEdgeTelemetryContext {
            tenant_id: musteragent_identity::TenantId::generate(),
            conversation_id: musteragent_identity::ConversationId::generate(),
            correlation_id: musteragent_identity::CorrelationId::generate(),
            trace_context: None,
        }
    }

    #[test]
    fn successful_data_edge_requests_are_metrics_and_traces_not_logs() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).data_edge();
        telemetry.observe(
            &data_edge_context(),
            DataEdgeOperation::ContentRead,
            TelemetryOutcome::Success,
            None,
            Duration::from_millis(12),
        );

        assert!(backend.events.lock().expect("event lock").is_empty());
        assert_eq!(
            backend.data_edge.lock().expect("metric lock").as_slice(),
            &[(
                DataEdgeOperation::ContentRead,
                TelemetryOutcome::Success,
                Duration::from_millis(12),
            )]
        );
    }

    #[test]
    fn failed_data_edge_requests_keep_one_joinable_structured_event() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).data_edge();
        telemetry.observe(
            &data_edge_context(),
            DataEdgeOperation::ContentBatchRead,
            TelemetryOutcome::Error,
            Some(ErrorClass::Dependency),
            Duration::from_millis(25),
        );

        let events = backend.events.lock().expect("event lock");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].operation(),
            TelemetryOperation::DataEdgeContentBatchRead
        );
        assert_eq!(events[0].outcome(), TelemetryOutcome::Error);
        assert_eq!(events[0].error_class(), Some(ErrorClass::Dependency));
    }

    fn db_commit_context() -> DbCommitTelemetryContext {
        DbCommitTelemetryContext {
            tenant_id: musteragent_identity::TenantId::generate(),
            conversation_id: musteragent_identity::ConversationId::generate(),
            correlation_id: musteragent_identity::CorrelationId::generate(),
            kind: DbCommitKind::LedgerOutbox,
        }
    }

    #[test]
    fn successful_db_commits_are_metrics_and_traces_not_logs() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).db_commit();
        telemetry.observe(
            &db_commit_context(),
            TelemetryOutcome::Success,
            Duration::from_millis(8),
        );

        assert!(backend.events.lock().expect("event lock").is_empty());
        assert_eq!(
            backend.db_commit.lock().expect("metric lock").as_slice(),
            &[(
                DbCommitKind::LedgerOutbox,
                TelemetryOutcome::Success,
                Duration::from_millis(8),
            )]
        );
    }

    #[test]
    fn abnormal_db_commits_keep_one_joinable_structured_event_and_metric() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).db_commit();
        telemetry.observe(
            &db_commit_context(),
            TelemetryOutcome::Error,
            Duration::from_millis(21),
        );

        let events = backend.events.lock().expect("event lock");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), TelemetryEventKind::DbCommitCompleted);
        assert_eq!(events[0].operation(), TelemetryOperation::DbCommit);
        assert_eq!(events[0].outcome(), TelemetryOutcome::Error);
        drop(events);
        assert_eq!(
            backend.db_commit.lock().expect("metric lock").as_slice(),
            &[(
                DbCommitKind::LedgerOutbox,
                TelemetryOutcome::Error,
                Duration::from_millis(21),
            )]
        );
    }

    #[test]
    fn governed_commit_phase_preserves_aggregate_and_phase_metrics() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).db_commit();
        telemetry.observe_transaction(
            DbCommitKind::LedgerOutbox,
            DbTransactionPhase::Commit,
            DbTransactionResult::Success,
            Duration::from_millis(5),
        );

        assert_eq!(
            backend.db_commit.lock().expect("metric lock").as_slice(),
            &[(
                DbCommitKind::LedgerOutbox,
                TelemetryOutcome::Success,
                Duration::from_millis(5),
            )]
        );
        assert_eq!(
            backend
                .db_transaction
                .lock()
                .expect("transaction metric lock")
                .as_slice(),
            &[(
                DbCommitKind::LedgerOutbox,
                DbTransactionPhase::Commit,
                DbTransactionResult::Success,
                Duration::from_millis(5),
            )]
        );
    }

    #[test]
    fn governed_non_commit_phase_does_not_duplicate_commit_metric() {
        let backend = Arc::new(RecordingBackend::default());
        let machine_backend: Arc<dyn MachineTelemetryBackend> = backend.clone();
        let telemetry = MachineTelemetry::from_backend(machine_backend).db_commit();
        telemetry.observe_transaction(
            DbCommitKind::LedgerOutbox,
            DbTransactionPhase::Total,
            DbTransactionResult::Success,
            Duration::from_millis(9),
        );

        assert!(backend.db_commit.lock().expect("metric lock").is_empty());
        assert_eq!(
            backend
                .db_transaction
                .lock()
                .expect("transaction metric lock")
                .len(),
            1
        );
    }
}
