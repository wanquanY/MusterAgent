use super::*;
use musteragent_execution_grant::*;
use musteragent_model::CredentialGrantRef;
use musteragent_runtime_contract::OpaqueExternalRef;

/// What a derived Run is. It decides which source capabilities stay
/// reachable; it is never inferred from other parameters.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DerivationKind {
    /// An unattended scheduled occurrence: nobody holds an open page, so no
    /// browser lease exists for it.
    Scheduled {
        retry: musteragent_runtime_automation::schedule_policy::RetrySpec,
    },
    /// A delegated child of a live parent Run: the parent's browser binding is
    /// still live and serves the child.
    DelegatedChild,
}

/// New cloud admission authority is bounded to the exact persisted source.
/// Its ten-minute lifetime limits admission, not the lifetime of an already
/// admitted Run. Scheduling authority is separately revoked by schedule state.
pub(super) fn derive(
    source: &Source,
    run: RunContext,
    now: UtcTimestamp,
    retry: musteragent_runtime_automation::schedule_policy::RetrySpec,
) -> Result<musteragent_ledger::RuntimeSnapshotBlob, Error> {
    derive_as(source, run, now, DerivationKind::Scheduled { retry })
}

pub(crate) fn derive_child(
    source: &Source,
    run: RunContext,
    now: UtcTimestamp,
) -> Result<musteragent_ledger::RuntimeSnapshotBlob, Error> {
    derive_as(source, run, now, DerivationKind::DelegatedChild)
}

fn derive_as(
    source: &Source,
    run: RunContext,
    now: UtcTimestamp,
    kind: DerivationKind,
) -> Result<musteragent_ledger::RuntimeSnapshotBlob, Error> {
    let mut content = source.snapshot.clone();
    if let DerivationKind::Scheduled { retry: spec } = kind {
        content.execution_policy.recovery_policy = RecoveryPolicySnapshot::BoundedBackoff { spec };
    }
    if !content.application_catalogs.is_empty() {
        return Err(Error::ApplicationCatalogUnsupported);
    }
    // Goal authority is a frozen subject ceiling, not a new pursuit. Preserve
    // it byte-for-byte; neither the schedule nor its occurrence advances the
    // standing-goal state machine or manufactures a second goal.
    let grant_ref = CredentialGrantRef::new(format!("grant_derived_{}", run.run_id))
        .map_err(|_| Error::Invalid)?;
    content.model.credential_grant_ref = grant_ref.clone();
    for route in &mut content.model.fallback_candidates {
        route.credential_grant_ref = grant_ref.clone();
    }
    for route in &mut content.auxiliary_models {
        route.model.credential_grant_ref = CredentialGrantRef::new(format!(
            "grant_derived_{}_{}",
            run.run_id,
            route.role.canonical_code()
        ))
        .map_err(|_| Error::Invalid)?;
    }
    // A derived execution never acquires physical device capabilities from
    // the originating foreground conversation. Browser-executed tools are
    // dropped only when nothing could ever run them (see
    // `browser_execution_unreachable`) instead of failing on every call.
    let browser_unreachable = browser_execution_unreachable(kind, source.sandbox_policy);
    content
        .capabilities
        .retain(|c| derived_capability_retained(c.location, c.key.as_str(), browser_unreachable));
    content.tools.retain(|tool| {
        content
            .capabilities
            .iter()
            .any(|cap| cap.key.as_str() == tool.manifest.capability_key.as_str())
    });
    if let Some(discovery) = &mut content.discovery {
        discovery
            .initial_exposed_tool_keys
            .retain(|key| content.tools.iter().any(|t| t.manifest.key == *key));
        discovery.authorized_tool_catalog_hash =
            authorized_tool_catalog(&content.tools, &content.capabilities)
                .map_err(|_| Error::Corrupt)?
                .catalog_hash();
    }
    let grant = ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: content.product_release.clone(),
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).map_err(|_| Error::Corrupt)?,
        grant_ref,
        issuer: DottedKey::new("musteragent.cloud.orchestration").map_err(|_| Error::Corrupt)?,
        audience: DottedKey::new("musteragent.runtime.cloud").map_err(|_| Error::Corrupt)?,
        tenant_id: source.actor.tenant_id,
        model_provider_id: content.model.route.model_provider_id,
        model_entry_id: content.model.route.model_entry_id,
        run_id: run.run_id,
        issued_at: now,
        expires_at: UtcTimestamp::from_unix_millis(
            now.unix_millis()
                .checked_add(600_000)
                .ok_or(Error::Invalid)?,
        ),
        nonce: OpaqueExternalRef::new(format!("grant_derived_{}", run.run_id))
            .map_err(|_| Error::Invalid)?,
        requested_model_class: content.model.requested_model_class.clone(),
        model_route: content.model.route.clone(),
        fallback_model_routes: content
            .model
            .fallback_candidates
            .iter()
            .map(|r| r.route.clone())
            .collect(),
        auxiliary_model_routes: content
            .auxiliary_models
            .iter()
            .map(|r| GrantedAuxiliaryModelRoute {
                role: r.role,
                route: r.model.route.clone(),
                credential_grant_ref: r.model.credential_grant_ref.clone(),
            })
            .collect(),
        agent_contract: Some(
            content
                .agent
                .resolved_v5
                .as_ref()
                .ok_or(Error::Invalid)?
                .supported_runtime_contract
                .clone(),
        ),
        surface: Some(
            DottedKey::new(
                if content.policy.permission_mode
                    == musteragent_policy::PermissionMode::ServiceManaged
                {
                    "web"
                } else {
                    "background"
                },
            )
            .map_err(|_| Error::Corrupt)?,
        ),
        capability_entitlements: content
            .capabilities
            .iter()
            .map(|c| musteragent_capability::CapabilityEntitlement {
                key: c.key.clone(),
                provider_id: c.provider_id,
                location: c.location,
                effect: c.effect,
                allowed_scopes: c.allowed_scopes.clone(),
                policy_decision: c.policy_decision,
            })
            .collect(),
        tool_contract_releases: Vec::new(),
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id: source.actor.principal_id,
            authority_scope: match source.workspace {
                Some(workspace_id) => ExecutionGrantAuthorityScope::Workspace { workspace_id },
                None => ExecutionGrantAuthorityScope::Tenant {
                    tenant_id: source.actor.tenant_id,
                },
            },
            device_id: None,
            device_workspace_id: None,
        }),
        permission_mode: Some(content.policy.permission_mode),
        device_direct_provider: None,
        vision_direct_provider: None,
    };
    grant.validate().map_err(|error| {
        tracing::warn!(error = ?error, "derived grant rejected");
        Error::Corrupt
    })?;
    let sealed = RuntimeSnapshot::seal(
        run.runtime_snapshot_id,
        content,
        now,
        &musteragent_persistence_codec::ProtobufSnapshotCodec,
    )
    .map_err(|error| {
        tracing::warn!(error = ?error, "derived snapshot rejected");
        Error::Corrupt
    })?;
    Ok(musteragent_ledger::RuntimeSnapshotBlob {
        runtime_snapshot_id: sealed.runtime_snapshot_id,
        schema_version: sealed.content.schema_version.clone(),
        encoded_snapshot: sealed.encoded_content,
        content_hash: sealed.content_hash,
        created_at: now,
        memory_selection: sealed.content.memory,
        instinct_selections: sealed.content.instincts,
        memory_agent_id: sealed.content.agent.agent_id,
        execution_grant: grant,
        execution_grant_proof: None,
    })
}

/// Whether no browser execution provider can ever serve the derived Run.
///
/// - A delegated child shares its live parent's browser binding: reachable.
/// - A scheduled occurrence has no open page. It can still execute browser
///   tools on the server sandbox unless the host bound the source Run to
///   `BrowserOnly` (it operates no server sandbox).
/// - A source Run without a routing context gives no evidence that the server
///   sandbox is absent; the scheduled occurrence keeps the tools and its own
///   routing decides (and records) where each call runs, failing explicitly
///   when no executor exists.
fn browser_execution_unreachable(
    kind: DerivationKind,
    source_policy: Option<musteragent_sandbox::SandboxRoutingPolicy>,
) -> bool {
    use musteragent_sandbox::SandboxRoutingPolicy;
    match (kind, source_policy) {
        (DerivationKind::DelegatedChild, _) => false,
        (DerivationKind::Scheduled { .. }, Some(SandboxRoutingPolicy::BrowserOnly)) => true,
        (
            DerivationKind::Scheduled { .. },
            Some(SandboxRoutingPolicy::PreferBrowser | SandboxRoutingPolicy::ServerOnly),
        ) => false,
        (DerivationKind::Scheduled { .. }, None) => false,
    }
}

/// Whether a derived Run keeps a capability of its source Run.
fn derived_capability_retained(
    location: musteragent_capability::CapabilityLocation,
    key: &str,
    browser_unreachable: bool,
) -> bool {
    location != musteragent_capability::CapabilityLocation::Device
        && !(browser_unreachable && musteragent_tool::requires_browser_execution(key))
}

#[cfg(test)]
mod tests {
    use super::{DerivationKind, browser_execution_unreachable, derived_capability_retained};
    use musteragent_capability::CapabilityLocation;
    use musteragent_sandbox::SandboxRoutingPolicy;

    #[test]
    fn only_scheduled_occurrences_of_browser_only_sources_lose_browser_execution() {
        let scheduled = DerivationKind::Scheduled {
            retry: musteragent_runtime_automation::schedule_policy::RetrySpec::default(),
        };
        assert!(browser_execution_unreachable(
            scheduled,
            Some(SandboxRoutingPolicy::BrowserOnly)
        ));
        for policy in [
            None,
            Some(SandboxRoutingPolicy::PreferBrowser),
            Some(SandboxRoutingPolicy::ServerOnly),
        ] {
            assert!(
                !browser_execution_unreachable(scheduled, policy),
                "{policy:?}"
            );
        }
        for policy in [
            None,
            Some(SandboxRoutingPolicy::PreferBrowser),
            Some(SandboxRoutingPolicy::BrowserOnly),
            Some(SandboxRoutingPolicy::ServerOnly),
        ] {
            assert!(
                !browser_execution_unreachable(DerivationKind::DelegatedChild, policy),
                "{policy:?}"
            );
        }
    }

    #[test]
    fn unreachable_browser_drops_browser_executed_capabilities_and_device_always_drops() {
        for key in [
            "data.python.run@11",
            "sandbox.environment.prepare@1",
            "sandbox.environment.inspect@2",
            "sandbox.operation.cancel@1",
            "application.web.build@1",
        ] {
            assert!(
                !derived_capability_retained(CapabilityLocation::Cloud, key, true),
                "{key}"
            );
            // A server sandbox (or a delegated child's live parent binding)
            // can still execute them.
            assert!(
                derived_capability_retained(CapabilityLocation::Cloud, key, false),
                "{key}"
            );
        }
        for browser_unreachable in [true, false] {
            assert!(!derived_capability_retained(
                CapabilityLocation::Device,
                "device.terminal.exec@3",
                browser_unreachable
            ));
            assert!(derived_capability_retained(
                CapabilityLocation::Cloud,
                "web.fetch@1",
                browser_unreachable
            ));
            assert!(derived_capability_retained(
                CapabilityLocation::Cloud,
                "workspace.file.read@1",
                browser_unreachable
            ));
        }
    }
}
