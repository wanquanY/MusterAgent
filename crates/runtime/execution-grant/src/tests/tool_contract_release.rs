use super::*;

#[test]
fn v8_seals_model_invisible_tool_execution_binding() {
    let (mut grant, _) = device_grant();
    grant.schema_version = DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("V8 schema");
    grant.issuer = DottedKey::new("musteragent.backend.control-plane").expect("issuer");
    grant.audience = DottedKey::new("musteragent.runtime.cloud").expect("audience");
    let identity = grant.identity_binding.as_mut().expect("identity binding");
    identity.device_id = None;
    identity.device_workspace_id = None;
    let schema_version = DottedKey::new(TOOL_CONTRACT_RELEASE_SCHEMA_V1).expect("release schema");
    let tool_key = VersionedKey::new("artifact.image.create@1").expect("image Tool key");
    let description = "Create images with t2i and i2i.";
    let schema = br#"{"type":"object","properties":{"generation_type":{"enum":["t2i","i2i"]}}}"#;
    let execution_binding = "musteragent.image-execution-binding.v1:44444444-4444-4444-8444-444444444444:11111111-1111-4111-8111-111111111111:22222222-2222-4222-8222-222222222222:-";
    let content_sha256 = GrantedToolContractRelease::digest(
        &schema_version,
        &tool_key,
        true,
        Some(description),
        Some(schema),
        Some(execution_binding),
    );
    assert_eq!(
        content_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "6bc7609c964ba236a8b7b00f641f4013e46e0aa2a998f25c7225f75b5922153e"
    );
    grant.tool_contract_releases = vec![GrantedToolContractRelease {
        release_ref: OpaqueExternalRef::new(format!(
            "toolrel_{}",
            content_sha256
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ))
        .expect("release ref"),
        schema_version,
        tool_key,
        available: true,
        description: Some(description.to_owned()),
        input_schema_json: Some(schema.to_vec()),
        content_sha256,
        execution_binding_ref: Some(execution_binding.to_owned()),
    }];

    assert_eq!(grant.validate(), Ok(()));
    assert!(!canonical_claims(&grant).expect("V8 claims").is_empty());

    let mut legacy_namespace = grant.clone();
    legacy_namespace.audience =
        DottedKey::new("musteragent.legacy.runtime.cloud").expect("legacy audience");
    assert_eq!(
        legacy_namespace.validate(),
        Err(ExecutionGrantContractError::InvalidCurrentNamespace)
    );

    let mut binding_tampered = grant.clone();
    binding_tampered.tool_contract_releases[0].execution_binding_ref =
        Some("musteragent.image-execution-binding.v1:changed".to_owned());
    assert_eq!(
        binding_tampered.validate(),
        Err(ExecutionGrantContractError::InvalidToolContractRelease)
    );

    let mut retired_release = grant.clone();
    retired_release.tool_contract_releases[0].schema_version =
        DottedKey::new("musteragent.legacy.tool-contract-release.v1").expect("retired schema");
    assert_eq!(
        retired_release.validate(),
        Err(ExecutionGrantContractError::InvalidToolContractRelease)
    );
    let mut retired_grant = grant;
    retired_grant.schema_version =
        DottedKey::new("musteragent.legacy.execution-grant.v6").expect("retired grant");
    assert_eq!(
        retired_grant.validate(),
        Err(ExecutionGrantContractError::UnsupportedSchema)
    );
}
