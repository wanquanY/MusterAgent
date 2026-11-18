use super::*;
use crate::EXECUTION_GRANT_SCHEMA_V10;

fn assigned() -> (ExecutionGrant, GrantAuthorizationRequest) {
    let (mut grant, mut request) = super::hosted_project::hosted_grant();
    let binding = grant.identity_binding.as_mut().expect("fixture identity");
    let ExecutionGrantAuthorityScope::HostedProject { scope } = binding.authority_scope else {
        panic!("fixture scope")
    };
    binding.authority_scope = ExecutionGrantAuthorityScope::HostedProjectExecution {
        scope,
        assignment_digest: [19; 32],
    };
    let executor = DeviceId::generate();
    binding.device_id = Some(executor);
    grant.schema_version = DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("schema");
    grant.audience =
        DottedKey::new(format!("musteragent.runtime.device.id-{executor}")).expect("audience");
    request.identity_binding = *binding;
    request.runtime_audience = grant.audience.clone();
    (grant, request)
}

#[test]
fn web_and_desktop_bind_actual_executor_with_independent_home_identity() {
    let (mut grant, request) = assigned();
    for surface in ["web", "desktop"] {
        grant.surface = Some(DottedKey::new(surface).expect("surface"));
        grant.authorize(&request).expect("assigned executor");
        assert!(
            canonical_claims(&grant)
                .expect("canonical grant")
                .starts_with(b"musteragent.execution-grant.v10\0")
        );
    }
    grant.permission_mode = Some(PermissionMode::ServiceManaged);
    assert_eq!(
        grant.validate(),
        Err(ExecutionGrantContractError::InvalidPermissionSurface)
    );
}

#[test]
fn retired_schemas_reject_assignments_and_authority_cannot_be_substituted() {
    let (grant, request) = assigned();
    for schema in [
        "musteragent.legacy.execution-grant.v1",
        "musteragent.execution-grant.v8",
        "musteragent.execution-grant.v9",
        EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1,
        STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1,
    ] {
        let mut wrong = grant.clone();
        wrong.schema_version = DottedKey::new(schema).expect("schema");
        assert_eq!(
            wrong.validate(),
            Err(ExecutionGrantContractError::BindingMismatch)
        );
    }
    let ExecutionGrantAuthorityScope::HostedProjectExecution { scope, .. } =
        grant.identity_binding.expect("binding").authority_scope
    else {
        panic!("scope")
    };
    for authority in [
        ExecutionGrantAuthorityScope::HostedProject { scope },
        ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        ExecutionGrantAuthorityScope::Workspace {
            workspace_id: scope.workspace_id,
        },
    ] {
        let mut wrong = grant.clone();
        wrong
            .identity_binding
            .as_mut()
            .expect("binding")
            .authority_scope = authority;
        let expected = if matches!(authority, ExecutionGrantAuthorityScope::Workspace { .. }) {
            ExecutionGrantContractError::InvalidDeviceWorkspaceBinding
        } else {
            ExecutionGrantContractError::BindingMismatch
        };
        assert_eq!(wrong.authorize(&request), Err(expected));
    }
    let mut wrong = grant;
    wrong.identity_binding = None;
    assert_eq!(
        wrong.validate(),
        Err(ExecutionGrantContractError::MissingIdentityBinding)
    );
}

#[test]
fn assignment_digest_and_exact_device_binding_are_checked_and_signed() {
    let (grant, request) = assigned();
    let original = canonical_claims(&grant).expect("canonical grant");
    let mut changed = grant.clone();
    let ExecutionGrantAuthorityScope::HostedProjectExecution {
        assignment_digest, ..
    } = &mut changed
        .identity_binding
        .as_mut()
        .expect("binding")
        .authority_scope
    else {
        panic!("scope")
    };
    *assignment_digest = [21; 32];
    assert_ne!(
        canonical_claims(&changed).expect("canonical grant"),
        original
    );
    assert_eq!(
        changed.authorize(&request),
        Err(ExecutionGrantContractError::BindingMismatch)
    );
    let mut variants = vec![grant.clone(); 5];
    variants[0]
        .identity_binding
        .as_mut()
        .expect("binding")
        .device_id = None;
    variants[1]
        .identity_binding
        .as_mut()
        .expect("binding")
        .device_workspace_id = None;
    variants[2]
        .identity_binding
        .as_mut()
        .expect("binding")
        .device_workspace_id = Some(WorkspaceId::generate());
    variants[3].tenant_id = TenantId::generate();
    if let ExecutionGrantAuthorityScope::HostedProjectExecution {
        assignment_digest, ..
    } = &mut variants[4]
        .identity_binding
        .as_mut()
        .expect("binding")
        .authority_scope
    {
        *assignment_digest = [0; 32];
    }
    for wrong in variants {
        assert_eq!(
            wrong.validate(),
            Err(ExecutionGrantContractError::BindingMismatch)
        );
    }
    for audience in [
        "musteragent.runtime.cloud".to_owned(),
        format!("musteragent.runtime.device.id-{}", DeviceId::generate()),
    ] {
        let mut wrong = grant.clone();
        wrong.audience = DottedKey::new(audience).expect("audience");
        assert_eq!(
            wrong.validate(),
            Err(ExecutionGrantContractError::AudienceMismatch)
        );
    }
}
