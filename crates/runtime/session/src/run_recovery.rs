//! One guarded, idempotent release of a durable repair wait.
use musteragent_identity::{ActivityId, Actor, RunSeq};
use musteragent_ledger::AppendRequest;
use musteragent_run::ResumeRun;
use musteragent_runtime_contract::ErrorCode;

#[derive(Debug, Clone, PartialEq)]
pub struct RunResumeCommit {
    pub actor: Actor,
    pub request: ResumeRun,
    pub activity_id: ActivityId,
    pub expected_last_run_sequence: RunSeq,
    pub append: AppendRequest,
}

impl RunResumeCommit {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        self.append
            .validate()
            .map_err(|_| ErrorCode::InvalidCommand)?;
        if self.actor.tenant_id != self.append.tenant_id
            || self.actor.principal_kind != musteragent_identity::PrincipalKind::User
            || self.append.records.len() != 1
            || self.append.outbox.len() != 1
            || self.append.outbox[0].topic.as_str() != "run.dispatch"
            || self.append.outbox[0].payload != format!("run:{}", self.request.run_id).as_bytes()
        {
            return Err(ErrorCode::InvalidCommand);
        }
        let record = &self.append.records[0];
        if record.attempt_id.is_some()
            || record.run_id != Some(self.request.run_id)
            || record.activity_id != Some(self.activity_id)
            || record.participant_id != Some(self.request.participant_id)
            || record.run_seq.map(RunSeq::get)
                != self.expected_last_run_sequence.get().checked_add(1)
            || record.producer != musteragent_event_envelope::Producer::RuntimeApi
            || record.record_kind != musteragent_event_envelope::LedgerRecordKind::DomainEvent
        {
            return Err(ErrorCode::InvalidCommand);
        }
        Ok(())
    }
}
