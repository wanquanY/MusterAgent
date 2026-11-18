//! Content-free Task signing facts. The private facts service recomputes these
//! from the original durable reservation; callers cannot supply authority.
use super::*;
use musteragent_capability::{CapabilityEntitlement, CapabilityLocation};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_execution_context::DialogueCursor;
use musteragent_policy::{Decision, Effect};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDelegationCeiling {
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub source_run_id: RunId,
    pub source_invocation_id: InvocationId,
    pub source_snapshot_id: RuntimeSnapshotId,
    /// Exact accepted ceiling from source authority; target grants may only
    /// attenuate it. A tool key alone would lose effect/scopes/approval posture.
    pub capabilities: Vec<CapabilityEntitlement>,
    /// Authorized immutable inputs only, not arbitrary caller-supplied paths.
    pub input_refs: Vec<ContentRef>,
}
impl WorkItemDelegationCeiling {
    /// An executor receives no provider, effect, scope, or approval privilege
    /// that was absent from the authority under which this task was accepted.
    pub fn permits(&self, granted: &[CapabilityEntitlement]) -> bool {
        self.validate().is_ok()
            && granted.iter().all(|candidate| {
                candidate.validate().is_ok()
                    && self.capabilities.iter().any(|ceiling| {
                        candidate.key == ceiling.key
                            && candidate.provider_id == ceiling.provider_id
                            && candidate.location == ceiling.location
                            && candidate.effect == ceiling.effect
                            && candidate.allowed_scopes.is_subset(&ceiling.allowed_scopes)
                            && matches!(
                                (ceiling.policy_decision, candidate.policy_decision),
                                (_, Decision::Deny)
                                    | (Decision::Allow, _)
                                    | (Decision::RequireApproval, Decision::RequireApproval)
                            )
                    })
            })
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self
            .product_release
            .as_ref()
            .is_some_and(|r| r.digest == [0; 32])
            || self.capabilities.is_empty()
            || self.capabilities.len() > 512
            || self.input_refs.len() > 256
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut previous = None;
        for capability in &self.capabilities {
            capability
                .validate()
                .map_err(|_| DialogueStoreError::Corrupt)?;
            if previous.is_some_and(|key: &str| key >= capability.key.as_str()) {
                return Err(DialogueStoreError::Corrupt);
            }
            previous = Some(capability.key.as_str());
        }
        let mut refs = std::collections::BTreeSet::new();
        for reference in &self.input_refs {
            if !refs.insert(reference.content_id) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(())
    }
    pub fn sha256(&self) -> Result<Sha256, DialogueStoreError> {
        self.validate()?;
        let mut b = b"musteragent.work-item-delegation-ceiling.v1\0".to_vec();
        for id in [
            self.source_run_id.into_uuid(),
            self.source_invocation_id.into_uuid(),
            self.source_snapshot_id.into_uuid(),
        ] {
            frame(&mut b, id.as_bytes());
        }
        number(&mut b, self.capabilities.len() as u64);
        for c in &self.capabilities {
            frame(&mut b, c.key.as_str().as_bytes());
            frame(&mut b, c.provider_id.into_uuid().as_bytes());
            frame(
                &mut b,
                match c.location {
                    CapabilityLocation::Cloud => b"cloud",
                    CapabilityLocation::Backend => b"backend",
                    CapabilityLocation::Device => b"device",
                    CapabilityLocation::RemoteDevice => b"remote_device",
                    CapabilityLocation::Browser => b"browser",
                },
            );
            frame(
                &mut b,
                match c.effect {
                    Effect::Read => b"read",
                    Effect::WriteReversible => b"write_reversible",
                    Effect::WriteExternal => b"write_external",
                    Effect::Destructive => b"destructive",
                },
            );
            frame(
                &mut b,
                match c.policy_decision {
                    Decision::Allow => b"allow",
                    Decision::Deny => b"deny",
                    Decision::RequireApproval => b"require_approval",
                },
            );
            number(&mut b, c.allowed_scopes.len() as u64);
            for scope in &c.allowed_scopes {
                frame(&mut b, scope.as_str().as_bytes());
            }
        }
        number(&mut b, self.input_refs.len() as u64);
        for value in &self.input_refs {
            reference(&mut b, value);
        }
        if let Some(release) = &self.product_release {
            b = release.bind_canonical(b"musteragent.work-item-delegation-ceiling.v2\0", &b);
        }
        Ok(Sha256::digest(b))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemExecutionGrantFacts {
    pub device_workspace_id: Option<WorkspaceId>,
    pub key: WorkItemExecutionKey,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub executing_dialogue_id: AgentDialogueId,
    pub executing_agent_version_id: AgentVersionId,
    pub identities: WorkItemExecutionIdentities,
    pub run_id: RunId,
    pub context_watermark: DialogueCursor,
    pub context_source_sha256: Sha256,
    pub manifest_revision: u64,
    pub instructions_revision: u64,
    pub permission_revision: u64,
    /// Agent preference is not permission to execute outside Cloud.
    pub permission_mode: i32,
    pub default_agent_id: AgentId,
    pub delegation: WorkItemDelegationCeiling,
    pub target: WorkItemExecutionTarget,
    pub source_sha256: Sha256,
    pub report: Option<WorkItemExecutionReport>,
}
impl WorkItemExecutionPreparation {
    pub fn grant_fact_binding(&self) -> Result<WorkItemExecutionGrantFacts, DialogueStoreError> {
        self.validate()?;
        Ok(WorkItemExecutionGrantFacts {
            device_workspace_id: None,
            key: self.key,
            source_dialogue_id: self.work_item.source_dialogue_id,
            source_intent_id: self.work_item.source_intent_id,
            executing_dialogue_id: self.executing_agent.dialogue_id,
            executing_agent_version_id: self.executing_agent.published_version_id,
            identities: self.identities(),
            run_id: self.identities().run_id()?,
            context_watermark: self.context_cut.through,
            context_source_sha256: self.context_cut.source_chain_sha256,
            manifest_revision: self.configuration.manifest_revision.get(),
            instructions_revision: self.configuration.instructions_revision.get(),
            permission_revision: self.configuration.permission_revision.get(),
            permission_mode: self.configuration.permission_mode as i32,
            default_agent_id: self.default_agent_id,
            delegation: self.delegation.clone(),
            target: self.target.clone(),
            source_sha256: self.source_sha256()?,
            report: self.report.clone(),
        })
    }
    /// Length-prefixed canonical commitment of all source material; no user
    /// plaintext leaves the authority fact service. The snapshot pins this hash.
    pub fn source_sha256(&self) -> Result<Sha256, DialogueStoreError> {
        self.validate()?;
        let k = self.key;
        let mut b = if self.report.is_some() {
            b"musteragent.work-item-report-source.v1\0".to_vec()
        } else {
            b"musteragent.work-item-execution-source.v1\0".to_vec()
        };
        for id in [
            k.actor.tenant_id.into_uuid(),
            k.scope.owner_id.into_uuid(),
            k.scope.space_id.into_uuid(),
            k.scope.agent_id.into_uuid(),
            k.work_item_id.into_uuid(),
            k.executing_agent_id.into_uuid(),
            self.work_item.source_dialogue_id.into_uuid(),
            self.work_item.source_intent_id.into_uuid(),
            self.executing_agent.dialogue_id.into_uuid(),
            self.executing_agent.published_version_id.into_uuid(),
            self.default_agent_id.into_uuid(),
        ] {
            frame(&mut b, id.as_bytes());
        }
        for n in [
            k.goal_revision.get(),
            k.control_revision.get(),
            k.assignment_epoch.get(),
            self.configuration.manifest_revision.get(),
            self.configuration.instructions_revision.get(),
            self.configuration.permission_revision.get(),
            self.configuration.permission_mode as u64,
            self.context_cut.through.epoch,
            self.context_cut.through.seq,
        ] {
            number(&mut b, n);
        }
        reference(&mut b, &self.work_item.goal_content_ref);
        frame(&mut b, self.configuration.profile.display_name.as_bytes());
        frame(&mut b, self.configuration.profile.description.as_bytes());
        match &self.configuration.instructions_ref {
            Some(r) => {
                number(&mut b, 1);
                reference(&mut b, r);
            }
            None => number(&mut b, 0),
        }
        frame(&mut b, &self.context_cut.source_chain_sha256.into_bytes());
        frame(&mut b, &self.delegation.sha256()?.into_bytes());
        if let Some(r) = &self.report {
            for id in [
                r.producing_run_id.into_uuid(),
                r.producing_snapshot_id.into_uuid(),
                r.producing_agent_id.into_uuid(),
            ] {
                frame(&mut b, id.as_bytes());
            }
            reference(&mut b, &r.content_ref);
            frame(&mut b, r.outcome.as_str().as_bytes());
        }
        if let Some(feedback) = &self.feedback {
            frame(&mut b, b"musteragent.task-feedback.v1");
            reference(&mut b, &feedback.previous_result);
            reference(&mut b, &feedback.request);
            number(&mut b, feedback.files.len() as u64);
            for file in &feedback.files {
                reference(&mut b, file);
            }
        }
        // Cloud is the original source commitment, byte for byte. Device
        // reservations add an explicit domain-separated target, never a new Intent.
        if let WorkItemExecutionTarget::RemoteDevice(target) = &self.target {
            frame(&mut b, b"musteragent.work-item-remote-device-target.v1");
            for id in [
                target.device_id.into_uuid(),
                target.installation_id.into_uuid(),
                target.membership_id,
                target.consent_id,
            ] {
                frame(&mut b, id.as_bytes());
            }
            number(&mut b, target.membership_epoch);
            frame(&mut b, &target.consent_sha256.into_bytes());
            if let Some(anchor) = &target.consent_anchor {
                frame(&mut b, b"musteragent.work-item-device-consent-anchor.v1");
                frame(&mut b, &anchor.canonical_bytes()?);
            }
        }
        if !self.dependency_results.is_empty() {
            frame(&mut b, b"musteragent.work-item-dependencies.v1");
            number(&mut b, self.dependency_results.len() as u64);
            for dependency in &self.dependency_results {
                frame(&mut b, dependency.work_item_id.into_uuid().as_bytes());
                number(&mut b, dependency.goal_revision);
                number(&mut b, dependency.control_revision);
                number(&mut b, dependency.assignment_epoch);
                frame(&mut b, dependency.producing_run_id.into_uuid().as_bytes());
                reference(&mut b, &dependency.content_ref);
            }
        }
        Ok(Sha256::digest(b))
    }
}
fn frame(b: &mut Vec<u8>, v: &[u8]) {
    b.extend_from_slice(&(v.len() as u64).to_be_bytes());
    b.extend_from_slice(v);
}
fn number(b: &mut Vec<u8>, v: u64) {
    frame(b, &v.to_be_bytes());
}
fn reference(b: &mut Vec<u8>, r: &ContentRef) {
    frame(b, r.content_id.into_uuid().as_bytes());
    frame(b, r.media_type.as_str().as_bytes());
    number(b, r.byte_length);
    frame(b, &r.sha256.into_bytes());
}

#[cfg(test)]
mod product_release_tests {
    use super::*;
    #[test]
    fn host_release_commitment_binds_product_and_rejects_empty_digest() {
        let mut source = WorkItemDelegationCeiling {
            product_release: None,
            source_run_id: "019b2f80-0000-7000-8000-00000000000a"
                .parse()
                .expect("valid test fixture"),
            source_invocation_id: "019b2f80-0000-7000-8000-00000000000b"
                .parse()
                .expect("valid test fixture"),
            source_snapshot_id: "019b2f80-0000-7000-8000-00000000000c"
                .parse()
                .expect("valid test fixture"),
            capabilities: vec![CapabilityEntitlement {
                key: musteragent_capability::CapabilityKey::from_versioned_key(
                    musteragent_identity::VersionedKey::new("core.time.read@1")
                        .expect("valid test fixture"),
                ),
                provider_id: "01982f80-0000-7000-8000-000000000101"
                    .parse()
                    .expect("valid test fixture"),
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: std::collections::BTreeSet::from([
                    musteragent_identity::DottedKey::new("time.read").expect("valid test fixture"),
                ]),
                policy_decision: Decision::Allow,
            }],
            input_refs: vec![ContentRef {
                content_id: "019b2f80-0000-7000-8000-00000000000d"
                    .parse()
                    .expect("valid test fixture"),
                media_type: musteragent_content_ref::MediaType::new("text/plain")
                    .expect("valid test fixture"),
                byte_length: 0,
                sha256: Sha256::from_bytes([b'i'; 32]).expect("valid test fixture"),
            }],
        };
        assert_eq!(
            source.sha256().expect("valid test fixture").to_string(),
            "8688c92f98c1b4f50235b98764ea7bae125f82817158bdc101820b6baba97265"
        );
        // Host-defined release identity is part of the kernel commitment.
        let unbound = source.sha256().expect("valid test fixture");
        let published = musteragent_identity::ProductReleaseRef::new(
            musteragent_identity::ProductId::KERNEL,
            musteragent_identity::VersionedKey::new("fixture-release@1")
                .expect("valid test fixture"),
            [7; 32],
        )
        .expect("valid test fixture");
        source.product_release = Some(published.clone());
        let bound = source.sha256().expect("valid test fixture");
        assert_ne!(bound, unbound);
        assert_eq!(source.sha256().expect("valid test fixture"), bound);
        source.product_release = Some(musteragent_identity::ProductReleaseRef {
            product: musteragent_identity::ProductId::parse("other-host")
                .expect("valid test fixture"),
            ..published.clone()
        });
        assert_ne!(source.sha256().expect("valid test fixture"), bound);
        source.product_release = Some(musteragent_identity::ProductReleaseRef {
            digest: [0; 32],
            ..published
        });
        assert!(source.sha256().is_err());
    }
}
