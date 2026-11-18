use super::*;
use crate::EXECUTION_GRANT_SCHEMA_V10;
use musteragent_event_envelope::{HostedHomeBinding, HostedProjectScope};
use musteragent_identity::{HomeBindingId, HomeEpoch, ProjectId};

pub(super) fn hosted_grant() -> (ExecutionGrant, GrantAuthorizationRequest) {
    let (mut grant, mut request) = device_grant();
    let scope = HostedProjectScope {
        project_id: ProjectId::generate(),
        workspace_id: WorkspaceId::generate(),
        resource_tenant_id: grant.tenant_id,
        owner_principal_id: PrincipalId::generate(),
        home: HostedHomeBinding {
            binding_id: HomeBindingId::generate(),
            device_id: DeviceId::generate(),
            home_epoch: HomeEpoch::new(7).expect("valid hosted scope test fixture"),
        },
    };
    grant.schema_version =
        DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("valid hosted scope test fixture");
    grant.issuer = DottedKey::new("musteragent.backend.control-plane")
        .expect("valid hosted scope test fixture");
    grant.audience = DottedKey::new(format!(
        "musteragent.runtime.device.id-{}",
        scope.home.device_id
    ))
    .expect("valid hosted scope test fixture");
    let binding = grant
        .identity_binding
        .as_mut()
        .expect("valid hosted scope test fixture");
    binding.authority_scope = ExecutionGrantAuthorityScope::HostedProject { scope };
    binding.device_id = Some(scope.home.device_id);
    binding.device_workspace_id = Some(scope.workspace_id);
    request.identity_binding = *binding;
    request.runtime_audience = grant.audience.clone();
    (grant, request)
}

#[test]
fn web_and_desktop_requester_can_authorize_same_hosted_home() {
    let (mut grant, request) = hosted_grant();
    for surface in ["desktop", "web"] {
        grant.surface = Some(DottedKey::new(surface).expect("valid hosted scope test fixture"));
        assert_eq!(grant.authorize(&request), Ok(()));
        assert!(
            canonical_claims(&grant)
                .expect("valid hosted scope test fixture")
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
fn every_project_and_home_field_is_bound_to_request_and_signing_bytes() {
    let (grant, request) = hosted_grant();
    let original = canonical_claims(&grant).expect("valid hosted scope test fixture");
    let ExecutionGrantAuthorityScope::HostedProject { scope } =
        request.identity_binding.authority_scope
    else {
        panic!()
    };
    let mut variants = vec![scope; 7];
    variants[0].project_id = ProjectId::generate();
    variants[1].workspace_id = WorkspaceId::generate();
    variants[2].resource_tenant_id = TenantId::generate();
    variants[3].owner_principal_id = PrincipalId::generate();
    variants[4].home.binding_id = HomeBindingId::generate();
    variants[5].home.device_id = DeviceId::generate();
    variants[6].home.home_epoch = HomeEpoch::new(8).expect("valid hosted scope test fixture");
    for scope in variants {
        let mut wrong = request.clone();
        wrong.identity_binding.authority_scope =
            ExecutionGrantAuthorityScope::HostedProject { scope };
        assert_eq!(
            grant.authorize(&wrong),
            Err(ExecutionGrantContractError::BindingMismatch)
        );
        let mut changed = grant.clone();
        changed.tenant_id = scope.resource_tenant_id;
        let binding = changed
            .identity_binding
            .as_mut()
            .expect("valid hosted scope test fixture");
        binding.authority_scope = ExecutionGrantAuthorityScope::HostedProject { scope };
        binding.device_id = Some(scope.home.device_id);
        binding.device_workspace_id = Some(scope.workspace_id);
        changed.audience = DottedKey::new(format!(
            "musteragent.runtime.device.id-{}",
            scope.home.device_id
        ))
        .expect("valid hosted scope test fixture");
        assert_ne!(
            canonical_claims(&changed).expect("valid hosted scope test fixture"),
            original
        );
    }
    let mut wrong_requester = request.clone();
    wrong_requester.identity_binding.principal_id = scope.owner_principal_id;
    assert_eq!(
        grant.authorize(&wrong_requester),
        Err(ExecutionGrantContractError::BindingMismatch)
    );
}

#[test]
fn rejects_legacy_schema_scope_downgrade_and_cloud_or_different_home() {
    let (grant, request) = hosted_grant();
    for schema in [
        "musteragent.legacy.execution-grant.v1",
        "musteragent.execution-grant.v8",
        "musteragent.execution-grant.v9",
    ] {
        let mut legacy = grant.clone();
        legacy.schema_version = DottedKey::new(schema).expect("valid hosted scope test fixture");
        assert_eq!(
            legacy.validate(),
            Err(ExecutionGrantContractError::BindingMismatch)
        );
    }
    let mut downgraded = grant.clone();
    downgraded
        .identity_binding
        .as_mut()
        .expect("valid hosted scope test fixture")
        .authority_scope = ExecutionGrantAuthorityScope::Tenant {
        tenant_id: grant.tenant_id,
    };
    assert_eq!(
        downgraded.authorize(&request),
        Err(ExecutionGrantContractError::BindingMismatch)
    );
    for audience in [
        "musteragent.runtime.cloud".to_owned(),
        format!("musteragent.runtime.device.id-{}", DeviceId::generate()),
    ] {
        let mut wrong = grant.clone();
        wrong.audience = DottedKey::new(audience).expect("valid hosted scope test fixture");
        assert_eq!(
            wrong.validate(),
            Err(ExecutionGrantContractError::AudienceMismatch)
        );
    }
    for missing in [true, false] {
        let mut wrong = grant.clone();
        let binding = wrong
            .identity_binding
            .as_mut()
            .expect("valid hosted scope test fixture");
        if missing {
            binding.device_workspace_id = None;
        } else {
            binding.device_id = None;
        }
        assert_eq!(
            wrong.validate(),
            Err(ExecutionGrantContractError::BindingMismatch)
        );
    }
}
