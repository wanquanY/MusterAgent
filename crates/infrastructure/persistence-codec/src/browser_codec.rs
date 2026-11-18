use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};

use musteragent_capability::{
    BROWSER_JOB_RESULT_SCHEMA_VERSION, BROWSER_JOB_SCHEMA_VERSION, BrowserApplicationBuildContract,
    BrowserApplicationPreviewProfile, BrowserApplicationRoutingMode, BrowserDeclaredOutput,
    BrowserDiagnostic, BrowserDiagnosticSeverity, BrowserInput, BrowserJobContractError,
    BrowserJobEngine, BrowserJobMetrics, BrowserJobProgram, BrowserJobRequest, BrowserJobResult,
    BrowserJobStatus, BrowserLeaseProof, BrowserProviderLease, BrowserPythonEnvironmentBinding,
    BrowserResourceLimits, BrowserRuntimeFeatureProbe, BrowserSecurityProfile, CapabilityKey,
    StagedBrowserOutput,
};
use musteragent_content_ref::MediaType;
use musteragent_identity::{BrowserLeaseEpoch, FencingToken, VersionedKey};
use thiserror::Error;

use crate::wire;
use crate::wire_common::{
    WireContractError, content_ref, decode_content_ref, decode_sha256, decode_timestamp, parse_id,
    sha256, timestamp,
};

pub fn encode_browser_provider_lease(
    value: &BrowserProviderLease,
) -> Result<wire::BrowserProviderLease, BrowserProtocolCodecError> {
    value.validate()?;
    Ok(wire::BrowserProviderLease {
        lease_id: Some(wire::BrowserLeaseId {
            value: value.lease_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        principal_id: Some(wire::PrincipalId {
            value: value.principal_id.to_string(),
        }),
        browser_session_id: Some(wire::BrowserSessionId {
            value: value.browser_session_id.to_string(),
        }),
        page_instance_id: Some(wire::PageInstanceId {
            value: value.page_instance_id.to_string(),
        }),
        provider_id: Some(wire::CapabilityProviderId {
            value: value.provider_id.to_string(),
        }),
        runtime_release_ref: Some(versioned_ref(&value.runtime_release_ref)),
        capability_keys: value
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        resource_profile_ref: Some(versioned_ref(&value.resource_profile_ref)),
        epoch: value.epoch.get(),
        fence: value.fence.get(),
        issued_at: Some(timestamp(value.issued_at)),
        expires_at: Some(timestamp(value.expires_at)),
    })
}

pub fn decode_browser_provider_lease(
    value: wire::BrowserProviderLease,
) -> Result<BrowserProviderLease, BrowserProtocolCodecError> {
    let capability_count = value.capability_keys.len();
    let capabilities = value
        .capability_keys
        .into_iter()
        .map(|key| {
            VersionedKey::new(key)
                .map(CapabilityKey::from_versioned_key)
                .map_err(|_| BrowserProtocolCodecError::InvalidContract)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if capabilities.len() != capability_count {
        return Err(BrowserProtocolCodecError::InvalidContract);
    }
    let lease = BrowserProviderLease {
        lease_id: parse_id(value.lease_id.map(|value| value.value))?,
        tenant_id: parse_id(value.tenant_id.map(|value| value.value))?,
        principal_id: parse_id(value.principal_id.map(|value| value.value))?,
        browser_session_id: parse_id(value.browser_session_id.map(|value| value.value))?,
        page_instance_id: parse_id(value.page_instance_id.map(|value| value.value))?,
        provider_id: parse_id(value.provider_id.map(|value| value.value))?,
        runtime_release_ref: decode_versioned_ref(value.runtime_release_ref)?,
        capabilities,
        resource_profile_ref: decode_versioned_ref(value.resource_profile_ref)?,
        epoch: BrowserLeaseEpoch::new(value.epoch)
            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
        fence: FencingToken::new(value.fence)
            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
        issued_at: decode_timestamp(value.issued_at)?,
        expires_at: decode_timestamp(value.expires_at)?,
    };
    lease.validate()?;
    Ok(lease)
}

#[must_use]
pub fn encode_browser_lease_proof(value: BrowserLeaseProof) -> wire::BrowserLeaseProof {
    wire::BrowserLeaseProof {
        lease_id: Some(wire::BrowserLeaseId {
            value: value.lease_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        principal_id: Some(wire::PrincipalId {
            value: value.principal_id.to_string(),
        }),
        browser_session_id: Some(wire::BrowserSessionId {
            value: value.browser_session_id.to_string(),
        }),
        page_instance_id: Some(wire::PageInstanceId {
            value: value.page_instance_id.to_string(),
        }),
        provider_id: Some(wire::CapabilityProviderId {
            value: value.provider_id.to_string(),
        }),
        epoch: value.epoch.get(),
        fence: value.fence.get(),
    }
}

pub fn decode_browser_lease_proof(
    value: wire::BrowserLeaseProof,
) -> Result<BrowserLeaseProof, BrowserProtocolCodecError> {
    Ok(BrowserLeaseProof {
        lease_id: parse_id(value.lease_id.map(|value| value.value))?,
        tenant_id: parse_id(value.tenant_id.map(|value| value.value))?,
        principal_id: parse_id(value.principal_id.map(|value| value.value))?,
        browser_session_id: parse_id(value.browser_session_id.map(|value| value.value))?,
        page_instance_id: parse_id(value.page_instance_id.map(|value| value.value))?,
        provider_id: parse_id(value.provider_id.map(|value| value.value))?,
        epoch: BrowserLeaseEpoch::new(value.epoch)
            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
        fence: FencingToken::new(value.fence)
            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
    })
}

pub fn encode_browser_feature_probe(
    value: &BrowserRuntimeFeatureProbe,
) -> Result<wire::BrowserRuntimeFeatureProbe, BrowserProtocolCodecError> {
    value.validate()?;
    Ok(wire::BrowserRuntimeFeatureProbe {
        worker: value.worker,
        web_assembly: value.web_assembly,
        crypto_subtle: value.crypto_subtle,
        transferable_array_buffer: value.transferable_array_buffer,
        service_worker: value.service_worker,
        indexed_db: value.indexed_db,
        secure_context: value.secure_context,
        engines: value
            .engines
            .iter()
            .copied()
            .map(|engine| encode_engine(engine) as i32)
            .collect(),
    })
}

pub fn decode_browser_feature_probe(
    value: wire::BrowserRuntimeFeatureProbe,
) -> Result<BrowserRuntimeFeatureProbe, BrowserProtocolCodecError> {
    let engine_count = value.engines.len();
    let engines = value
        .engines
        .into_iter()
        .map(decode_engine)
        .collect::<Result<BTreeSet<_>, _>>()?;
    if engines.len() != engine_count {
        return Err(BrowserProtocolCodecError::InvalidContract);
    }
    let probe = BrowserRuntimeFeatureProbe {
        worker: value.worker,
        web_assembly: value.web_assembly,
        crypto_subtle: value.crypto_subtle,
        transferable_array_buffer: value.transferable_array_buffer,
        service_worker: value.service_worker,
        indexed_db: value.indexed_db,
        secure_context: value.secure_context,
        engines,
    };
    probe.validate()?;
    Ok(probe)
}

pub fn encode_browser_job_request(
    value: &BrowserJobRequest,
) -> Result<wire::BrowserJobRequest, BrowserProtocolCodecError> {
    value.validate()?;
    Ok(wire::BrowserJobRequest {
        execution_liveness_v1: value
            .execution_liveness
            .as_deref()
            .map(crate::execution_liveness_codec::execution_liveness_policy_message),
        schema_version: BROWSER_JOB_SCHEMA_VERSION.to_owned(),
        job_id: Some(wire::BrowserJobId {
            value: value.job_id.to_string(),
        }),
        invocation_id: Some(wire::InvocationId {
            value: value.invocation_id.to_string(),
        }),
        conversation_id: Some(wire::ConversationId {
            value: value.conversation_id.to_string(),
        }),
        engine: encode_engine(value.engine) as i32,
        runtime_release_ref: Some(versioned_ref(&value.runtime_release_ref)),
        program: Some(encode_program(&value.program)),
        inputs: value
            .inputs
            .iter()
            .map(|input| wire::BrowserInputDescriptor {
                path: input.path.clone(),
                content_ref: Some(content_ref(&input.content_ref)),
            })
            .collect(),
        declared_outputs: value
            .declared_outputs
            .iter()
            .map(|output| wire::BrowserDeclaredOutput {
                path: output.path.clone(),
                media_type: output.media_type.as_str().to_owned(),
                maximum_byte_length: output.maximum_byte_length.get(),
                required: output.required,
            })
            .collect(),
        limits: Some(encode_limits(value.limits)),
        security_profile: match value.security_profile {
            BrowserSecurityProfile::SafeOffline => 1,
            BrowserSecurityProfile::ProxyEgress => 2,
        },
    })
}

pub fn decode_browser_job_request(
    value: wire::BrowserJobRequest,
) -> Result<BrowserJobRequest, BrowserProtocolCodecError> {
    if value.schema_version != BROWSER_JOB_SCHEMA_VERSION {
        return Err(BrowserProtocolCodecError::InvalidContract);
    }
    let request = BrowserJobRequest {
        execution_liveness: value
            .execution_liveness_v1
            .map(crate::execution_liveness_codec::execution_liveness_policy)
            .transpose()
            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?
            .map(std::sync::Arc::new),
        job_id: parse_id(value.job_id.map(|value| value.value))?,
        invocation_id: parse_id(value.invocation_id.map(|value| value.value))?,
        conversation_id: parse_id(value.conversation_id.map(|value| value.value))?,
        engine: decode_engine(value.engine)?,
        runtime_release_ref: decode_versioned_ref(value.runtime_release_ref)?,
        program: decode_program(
            value
                .program
                .ok_or(BrowserProtocolCodecError::InvalidContract)?,
        )?,
        inputs: value
            .inputs
            .into_iter()
            .map(|input| {
                Ok(BrowserInput {
                    path: input.path,
                    content_ref: decode_content_ref(
                        input
                            .content_ref
                            .ok_or(BrowserProtocolCodecError::InvalidContract)?,
                    )?,
                })
            })
            .collect::<Result<_, BrowserProtocolCodecError>>()?,
        declared_outputs: value
            .declared_outputs
            .into_iter()
            .map(|output| {
                Ok(BrowserDeclaredOutput {
                    path: output.path,
                    media_type: MediaType::new(output.media_type)
                        .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                    maximum_byte_length: NonZeroU64::new(output.maximum_byte_length)
                        .ok_or(BrowserProtocolCodecError::InvalidContract)?,
                    required: output.required,
                })
            })
            .collect::<Result<_, BrowserProtocolCodecError>>()?,
        limits: decode_limits(
            value
                .limits
                .ok_or(BrowserProtocolCodecError::InvalidContract)?,
        )?,
        security_profile: match wire::BrowserSecurityProfile::try_from(value.security_profile) {
            Ok(wire::BrowserSecurityProfile::SafeOffline) => BrowserSecurityProfile::SafeOffline,
            Ok(wire::BrowserSecurityProfile::ProxyEgress) => BrowserSecurityProfile::ProxyEgress,
            _ => return Err(BrowserProtocolCodecError::InvalidContract),
        },
    };
    request.validate()?;
    Ok(request)
}

pub fn encode_browser_job_result(
    value: &BrowserJobResult,
) -> Result<wire::BrowserJobResult, BrowserProtocolCodecError> {
    value.validate()?;
    Ok(wire::BrowserJobResult {
        schema_version: BROWSER_JOB_RESULT_SCHEMA_VERSION.to_owned(),
        job_id: Some(wire::BrowserJobId {
            value: value.job_id.to_string(),
        }),
        status: encode_status(value.status) as i32,
        outputs: value
            .outputs
            .iter()
            .map(|output| wire::StagedBrowserOutput {
                path: output.path.clone(),
                content_ref: Some(content_ref(&output.content_ref)),
                staging_receipt_ref: output.staging_receipt_ref.clone(),
            })
            .collect(),
        diagnostics: value
            .diagnostics
            .iter()
            .map(|diagnostic| wire::BrowserDiagnostic {
                code: diagnostic.code.clone(),
                severity: encode_severity(diagnostic.severity) as i32,
                message: diagnostic.message.clone(),
                path: diagnostic.path.clone(),
                line: diagnostic.line.map(NonZeroU32::get),
                column: diagnostic.column.map(NonZeroU32::get),
            })
            .collect(),
        stdout_tail: value.stdout_tail.clone(),
        stderr_tail: value.stderr_tail.clone(),
        metrics: Some(wire::BrowserJobMetrics {
            wall_time_millis: value.metrics.wall_time_millis,
            input_bytes: value.metrics.input_bytes,
            output_bytes: value.metrics.output_bytes,
            output_files: value.metrics.output_files,
        }),
        result_hash: Some(sha256(value.result_hash)),
    })
}

pub fn decode_browser_job_result(
    value: wire::BrowserJobResult,
) -> Result<BrowserJobResult, BrowserProtocolCodecError> {
    if value.schema_version != BROWSER_JOB_RESULT_SCHEMA_VERSION {
        return Err(BrowserProtocolCodecError::InvalidContract);
    }
    let metrics = value
        .metrics
        .ok_or(BrowserProtocolCodecError::InvalidContract)?;
    let result = BrowserJobResult {
        job_id: parse_id(value.job_id.map(|value| value.value))?,
        status: decode_status(value.status)?,
        outputs: value
            .outputs
            .into_iter()
            .map(|output| {
                Ok(StagedBrowserOutput {
                    path: output.path,
                    content_ref: decode_content_ref(
                        output
                            .content_ref
                            .ok_or(BrowserProtocolCodecError::InvalidContract)?,
                    )?,
                    staging_receipt_ref: output.staging_receipt_ref,
                })
            })
            .collect::<Result<_, BrowserProtocolCodecError>>()?,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(|diagnostic| {
                Ok(BrowserDiagnostic {
                    code: diagnostic.code,
                    severity: decode_severity(diagnostic.severity)?,
                    message: diagnostic.message,
                    path: diagnostic.path,
                    line: diagnostic
                        .line
                        .map(|value| {
                            NonZeroU32::new(value).ok_or(BrowserProtocolCodecError::InvalidContract)
                        })
                        .transpose()?,
                    column: diagnostic
                        .column
                        .map(|value| {
                            NonZeroU32::new(value).ok_or(BrowserProtocolCodecError::InvalidContract)
                        })
                        .transpose()?,
                })
            })
            .collect::<Result<_, BrowserProtocolCodecError>>()?,
        stdout_tail: value.stdout_tail,
        stderr_tail: value.stderr_tail,
        metrics: BrowserJobMetrics {
            wall_time_millis: metrics.wall_time_millis,
            input_bytes: metrics.input_bytes,
            output_bytes: metrics.output_bytes,
            output_files: metrics.output_files,
        },
        result_hash: decode_sha256(value.result_hash)?,
    };
    result.validate()?;
    Ok(result)
}

fn encode_program(value: &BrowserJobProgram) -> wire::BrowserJobProgram {
    use wire::browser_job_program::Program;
    let program = match value {
        BrowserJobProgram::PythonWorkspace {
            execution_workspace_id,
            source_ref,
            workspace_ref,
            entrypoint,
            environment,
            environment_manifest_ref,
        } => Program::PythonWorkspace(wire::BrowserPythonWorkspaceProgram {
            execution_workspace_id: Some(wire::ExecutionWorkspaceId {
                value: execution_workspace_id.to_string(),
            }),
            source_ref: Some(content_ref(source_ref)),
            workspace_ref: Some(content_ref(workspace_ref)),
            environment_manifest_ref: environment_manifest_ref.as_ref().map(content_ref),
            entrypoint: entrypoint.clone(),
            environment: environment
                .as_ref()
                .map(|value| wire::BrowserPythonEnvironmentBinding {
                    installation_id: value.installation_id.to_string(),
                    revision_hash: value.revision_hash.to_string(),
                    scope_key: value.scope_key.to_string(),
                }),
        }),
        BrowserJobProgram::Presentation {
            bundle_ref,
            component_generation,
        } => Program::Presentation(wire::BrowserPresentationProgram {
            bundle_ref: Some(content_ref(bundle_ref)),
            component_generation: component_generation
                .map(|hash| hash.to_string())
                .unwrap_or_default(),
        }),
        BrowserJobProgram::Python {
            source_ref,
            entrypoint,
            environment,
            require_prepared_environment,
        } => Program::Python(wire::BrowserPythonProgram {
            source_ref: Some(content_ref(source_ref)),
            entrypoint: entrypoint.clone(),
            environment: environment
                .as_ref()
                .map(|value| wire::BrowserPythonEnvironmentBinding {
                    installation_id: value.installation_id.to_string(),
                    revision_hash: value.revision_hash.to_string(),
                    scope_key: value.scope_key.to_string(),
                }),
            require_prepared_environment: *require_prepared_environment,
        }),
        BrowserJobProgram::PythonEnvironment {
            requirements,
            requires_python,
            scope_key,
        } => Program::PythonEnvironment(wire::BrowserPythonEnvironmentProgram {
            requirements: requirements.clone(),
            requires_python: requires_python.clone(),
            scope_key: scope_key.to_string(),
        }),
        BrowserJobProgram::StaticValidate {
            source_tree_ref,
            application,
        } => Program::StaticValidate(wire::BrowserStaticValidateProgram {
            source_tree_ref: Some(content_ref(source_tree_ref)),
            application: Some(encode_application_build_contract(application)),
        }),
        BrowserJobProgram::Esbuild {
            source_tree_ref,
            dependency_lock_ref,
            application,
        } => Program::Esbuild(wire::BrowserEsbuildProgram {
            source_tree_ref: Some(content_ref(source_tree_ref)),
            dependency_lock_ref: Some(content_ref(dependency_lock_ref)),
            application: Some(encode_application_build_contract(application)),
        }),
    };
    wire::BrowserJobProgram {
        program: Some(program),
    }
}

fn decode_program(
    value: wire::BrowserJobProgram,
) -> Result<BrowserJobProgram, BrowserProtocolCodecError> {
    use wire::browser_job_program::Program;
    match value
        .program
        .ok_or(BrowserProtocolCodecError::InvalidContract)?
    {
        Program::Presentation(value) => Ok(BrowserJobProgram::Presentation {
            bundle_ref: decode_content_ref(
                value
                    .bundle_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            component_generation: if value.component_generation.is_empty() {
                None
            } else {
                Some(
                    musteragent_content_ref::Sha256::from_hex(&value.component_generation)
                        .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                )
            },
        }),
        Program::PythonWorkspace(value) => Ok(BrowserJobProgram::PythonWorkspace {
            execution_workspace_id: parse_id(value.execution_workspace_id.map(|id| id.value))?,
            environment_manifest_ref: value
                .environment_manifest_ref
                .map(decode_content_ref)
                .transpose()?,
            source_ref: decode_content_ref(
                value
                    .source_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            workspace_ref: decode_content_ref(
                value
                    .workspace_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            entrypoint: value.entrypoint,
            environment: value
                .environment
                .map(|item| -> Result<_, BrowserProtocolCodecError> {
                    Ok(BrowserPythonEnvironmentBinding {
                        installation_id: parse_id(Some(item.installation_id))?,
                        revision_hash: musteragent_content_ref::Sha256::from_hex(
                            &item.revision_hash,
                        )
                        .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                        scope_key: musteragent_content_ref::Sha256::from_hex(&item.scope_key)
                            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                    })
                })
                .transpose()?,
        }),
        Program::Python(value) => Ok(BrowserJobProgram::Python {
            source_ref: decode_content_ref(
                value
                    .source_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            entrypoint: value.entrypoint,
            environment: value
                .environment
                .map(|item| -> Result<_, BrowserProtocolCodecError> {
                    Ok(BrowserPythonEnvironmentBinding {
                        installation_id: parse_id(Some(item.installation_id))?,
                        revision_hash: musteragent_content_ref::Sha256::from_hex(
                            &item.revision_hash,
                        )
                        .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                        scope_key: musteragent_content_ref::Sha256::from_hex(&item.scope_key)
                            .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
                    })
                })
                .transpose()?,
            require_prepared_environment: value.require_prepared_environment,
        }),
        Program::PythonEnvironment(value) => Ok(BrowserJobProgram::PythonEnvironment {
            requirements: value.requirements,
            requires_python: value.requires_python,
            scope_key: musteragent_content_ref::Sha256::from_hex(&value.scope_key)
                .map_err(|_| BrowserProtocolCodecError::InvalidContract)?,
        }),
        Program::StaticValidate(value) => Ok(BrowserJobProgram::StaticValidate {
            source_tree_ref: decode_content_ref(
                value
                    .source_tree_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            application: decode_application_build_contract(
                value
                    .application
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
        }),
        Program::Esbuild(value) => Ok(BrowserJobProgram::Esbuild {
            source_tree_ref: decode_content_ref(
                value
                    .source_tree_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            dependency_lock_ref: decode_content_ref(
                value
                    .dependency_lock_ref
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
            application: decode_application_build_contract(
                value
                    .application
                    .ok_or(BrowserProtocolCodecError::InvalidContract)?,
            )?,
        }),
    }
}

fn encode_application_build_contract(
    value: &BrowserApplicationBuildContract,
) -> wire::BrowserApplicationBuildContract {
    wire::BrowserApplicationBuildContract {
        routing_mode: match value.routing_mode {
            BrowserApplicationRoutingMode::MultiPage => {
                wire::BrowserApplicationRoutingMode::MultiPage as i32
            }
            BrowserApplicationRoutingMode::Spa => wire::BrowserApplicationRoutingMode::Spa as i32,
        },
        routing_fallback: value.routing_fallback.clone(),
        preview_profile: match value.preview_profile {
            BrowserApplicationPreviewProfile::StaticSafe => {
                wire::BrowserApplicationPreviewProfile::StaticSafe as i32
            }
            BrowserApplicationPreviewProfile::InteractiveIsolated => {
                wire::BrowserApplicationPreviewProfile::InteractiveIsolated as i32
            }
        },
        source_revision_hash: Some(sha256(value.source_revision_hash)),
    }
}

fn decode_application_build_contract(
    value: wire::BrowserApplicationBuildContract,
) -> Result<BrowserApplicationBuildContract, BrowserProtocolCodecError> {
    Ok(BrowserApplicationBuildContract {
        routing_mode: match wire::BrowserApplicationRoutingMode::try_from(value.routing_mode) {
            Ok(wire::BrowserApplicationRoutingMode::MultiPage) => {
                BrowserApplicationRoutingMode::MultiPage
            }
            Ok(wire::BrowserApplicationRoutingMode::Spa) => BrowserApplicationRoutingMode::Spa,
            _ => return Err(BrowserProtocolCodecError::InvalidContract),
        },
        routing_fallback: value.routing_fallback,
        preview_profile: match wire::BrowserApplicationPreviewProfile::try_from(
            value.preview_profile,
        ) {
            Ok(wire::BrowserApplicationPreviewProfile::StaticSafe) => {
                BrowserApplicationPreviewProfile::StaticSafe
            }
            Ok(wire::BrowserApplicationPreviewProfile::InteractiveIsolated) => {
                BrowserApplicationPreviewProfile::InteractiveIsolated
            }
            _ => return Err(BrowserProtocolCodecError::InvalidContract),
        },
        source_revision_hash: decode_sha256(value.source_revision_hash)?,
    })
}

const fn encode_engine(value: BrowserJobEngine) -> wire::BrowserJobEngine {
    match value {
        BrowserJobEngine::Python => wire::BrowserJobEngine::Python,
        BrowserJobEngine::PythonEnvironment => wire::BrowserJobEngine::PythonEnvironment,
        BrowserJobEngine::StaticValidate => wire::BrowserJobEngine::StaticValidate,
        BrowserJobEngine::Esbuild => wire::BrowserJobEngine::Esbuild,
        BrowserJobEngine::Presentation => wire::BrowserJobEngine::Presentation,
    }
}

fn decode_engine(value: i32) -> Result<BrowserJobEngine, BrowserProtocolCodecError> {
    match wire::BrowserJobEngine::try_from(value) {
        Ok(wire::BrowserJobEngine::Python) => Ok(BrowserJobEngine::Python),
        Ok(wire::BrowserJobEngine::PythonEnvironment) => Ok(BrowserJobEngine::PythonEnvironment),
        Ok(wire::BrowserJobEngine::StaticValidate) => Ok(BrowserJobEngine::StaticValidate),
        Ok(wire::BrowserJobEngine::Esbuild) => Ok(BrowserJobEngine::Esbuild),
        Ok(wire::BrowserJobEngine::Presentation) => Ok(BrowserJobEngine::Presentation),
        _ => Err(BrowserProtocolCodecError::InvalidContract),
    }
}

const fn encode_status(value: BrowserJobStatus) -> wire::BrowserJobStatus {
    match value {
        BrowserJobStatus::Completed => wire::BrowserJobStatus::Completed,
        BrowserJobStatus::Failed => wire::BrowserJobStatus::Failed,
        BrowserJobStatus::Indeterminate => wire::BrowserJobStatus::Indeterminate,
        BrowserJobStatus::Cancelled => wire::BrowserJobStatus::Cancelled,
        BrowserJobStatus::ResourceExceeded => wire::BrowserJobStatus::ResourceExceeded,
    }
}

fn decode_status(value: i32) -> Result<BrowserJobStatus, BrowserProtocolCodecError> {
    match wire::BrowserJobStatus::try_from(value) {
        Ok(wire::BrowserJobStatus::Completed) => Ok(BrowserJobStatus::Completed),
        Ok(wire::BrowserJobStatus::Failed) => Ok(BrowserJobStatus::Failed),
        Ok(wire::BrowserJobStatus::Indeterminate) => Ok(BrowserJobStatus::Indeterminate),
        Ok(wire::BrowserJobStatus::Cancelled) => Ok(BrowserJobStatus::Cancelled),
        Ok(wire::BrowserJobStatus::ResourceExceeded) => Ok(BrowserJobStatus::ResourceExceeded),
        _ => Err(BrowserProtocolCodecError::InvalidContract),
    }
}

const fn encode_severity(value: BrowserDiagnosticSeverity) -> wire::BrowserDiagnosticSeverity {
    match value {
        BrowserDiagnosticSeverity::Info => wire::BrowserDiagnosticSeverity::Info,
        BrowserDiagnosticSeverity::Warning => wire::BrowserDiagnosticSeverity::Warning,
        BrowserDiagnosticSeverity::Error => wire::BrowserDiagnosticSeverity::Error,
    }
}

fn decode_severity(value: i32) -> Result<BrowserDiagnosticSeverity, BrowserProtocolCodecError> {
    match wire::BrowserDiagnosticSeverity::try_from(value) {
        Ok(wire::BrowserDiagnosticSeverity::Info) => Ok(BrowserDiagnosticSeverity::Info),
        Ok(wire::BrowserDiagnosticSeverity::Warning) => Ok(BrowserDiagnosticSeverity::Warning),
        Ok(wire::BrowserDiagnosticSeverity::Error) => Ok(BrowserDiagnosticSeverity::Error),
        _ => Err(BrowserProtocolCodecError::InvalidContract),
    }
}

fn encode_limits(value: BrowserResourceLimits) -> wire::BrowserResourceLimits {
    wire::BrowserResourceLimits {
        wall_time_millis: value.wall_time_millis.get(),
        maximum_input_bytes: value.maximum_input_bytes.get(),
        maximum_output_bytes: value.maximum_output_bytes.get(),
        maximum_files: value.maximum_files.get(),
        maximum_stdout_bytes: value.maximum_stdout_bytes.get(),
        maximum_stderr_bytes: value.maximum_stderr_bytes.get(),
    }
}

fn decode_limits(
    value: wire::BrowserResourceLimits,
) -> Result<BrowserResourceLimits, BrowserProtocolCodecError> {
    Ok(BrowserResourceLimits {
        wall_time_millis: non_zero_u64(value.wall_time_millis)?,
        maximum_input_bytes: non_zero_u64(value.maximum_input_bytes)?,
        maximum_output_bytes: non_zero_u64(value.maximum_output_bytes)?,
        maximum_files: NonZeroU32::new(value.maximum_files)
            .ok_or(BrowserProtocolCodecError::InvalidContract)?,
        maximum_stdout_bytes: non_zero_u64(value.maximum_stdout_bytes)?,
        maximum_stderr_bytes: non_zero_u64(value.maximum_stderr_bytes)?,
    })
}

fn non_zero_u64(value: u64) -> Result<NonZeroU64, BrowserProtocolCodecError> {
    NonZeroU64::new(value).ok_or(BrowserProtocolCodecError::InvalidContract)
}

fn versioned_ref(value: &VersionedKey) -> wire::VersionedRef {
    wire::VersionedRef {
        key: value.as_str().to_owned(),
    }
}

fn decode_versioned_ref(
    value: Option<wire::VersionedRef>,
) -> Result<VersionedKey, BrowserProtocolCodecError> {
    VersionedKey::new(value.ok_or(BrowserProtocolCodecError::InvalidContract)?.key)
        .map_err(|_| BrowserProtocolCodecError::InvalidContract)
}

#[derive(Debug, Error)]
pub enum BrowserProtocolCodecError {
    #[error("Browser protocol value violates the closed wire contract")]
    InvalidContract,
    #[error(transparent)]
    Domain(#[from] BrowserJobContractError),
    #[error("Browser protocol contains an invalid shared wire value")]
    SharedWire,
}

impl From<WireContractError> for BrowserProtocolCodecError {
    fn from(_: WireContractError) -> Self {
        Self::SharedWire
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::{NonZeroU32, NonZeroU64};

    use musteragent_capability::{BrowserInput, BrowserSecurityProfile};
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        BrowserJobId, ContentId, ConversationId, InvocationId, VersionedKey,
    };

    use super::*;

    fn content(name: &str, media: &str) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media).expect("media"),
            byte_length: u64::try_from(name.len()).expect("length"),
            sha256: Sha256::digest(name),
        }
    }

    fn request() -> BrowserJobRequest {
        let source = content("print(1)", "text/x-python");
        BrowserJobRequest {
            execution_liveness: None,
            job_id: BrowserJobId::generate(),
            invocation_id: InvocationId::generate(),
            conversation_id: ConversationId::generate(),
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
            program: BrowserJobProgram::Python {
                source_ref: source.clone(),
                entrypoint: "main.py".to_owned(),
                environment: None,
                require_prepared_environment: false,
            },
            inputs: vec![BrowserInput {
                path: "main.py".to_owned(),
                content_ref: source,
            }],
            declared_outputs: Vec::new(),
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(1_000).expect("limit"),
                maximum_input_bytes: NonZeroU64::new(1_000).expect("limit"),
                maximum_output_bytes: NonZeroU64::new(1_000).expect("limit"),
                maximum_files: NonZeroU32::new(10).expect("limit"),
                maximum_stdout_bytes: NonZeroU64::new(1_000).expect("limit"),
                maximum_stderr_bytes: NonZeroU64::new(1_000).expect("limit"),
            },
            security_profile: BrowserSecurityProfile::SafeOffline,
        }
    }

    #[test]
    fn browser_job_request_round_trips_and_rejects_unknown_schema() {
        let source = request();
        let wire = encode_browser_job_request(&source).expect("encode");
        assert_eq!(
            decode_browser_job_request(wire.clone()).expect("decode"),
            source
        );

        let mut invalid = wire;
        invalid.schema_version = "musteragent.browser-job/2".to_owned();
        assert!(matches!(
            decode_browser_job_request(invalid),
            Err(BrowserProtocolCodecError::InvalidContract)
        ));
    }

    #[test]
    fn proxy_egress_profile_round_trips_without_changing_offline_default() {
        assert_eq!(
            BrowserSecurityProfile::default(),
            BrowserSecurityProfile::SafeOffline
        );
        let mut source = request();
        source.security_profile = BrowserSecurityProfile::ProxyEgress;
        let wire = encode_browser_job_request(&source).expect("encode proxy profile");
        assert_eq!(
            decode_browser_job_request(wire).expect("decode proxy profile"),
            source
        );
    }

    #[test]
    fn browser_job_carries_exact_activity_policy_and_rejects_tampered_digest() {
        use musteragent_runtime_contract::execution_liveness::{
            ActivitySource, ExecutionLivenessPolicy, ExecutionLivenessWindows,
        };
        let mut source = request();
        assert!(
            encode_browser_job_request(&source)
                .unwrap()
                .execution_liveness_v1
                .is_none()
        );
        source.execution_liveness = Some(std::sync::Arc::new(
            ExecutionLivenessPolicy::new(
                VersionedKey::new("data.python.run@9").unwrap(),
                ExecutionLivenessWindows::for_idle_window(std::time::Duration::from_secs(60)),
                ActivitySource::ALL,
            )
            .unwrap(),
        ));
        let mut wire = encode_browser_job_request(&source).unwrap();
        assert_eq!(decode_browser_job_request(wire.clone()).unwrap(), source);
        wire.execution_liveness_v1.as_mut().unwrap().idle_window_ms += 1;
        assert!(decode_browser_job_request(wire).is_err());
    }

    #[test]
    fn browser_lease_round_trips_with_fence_and_capabilities() {
        use musteragent_identity::{
            BrowserLeaseId, BrowserSessionId, CapabilityProviderId, PageInstanceId, PrincipalId,
            TenantId, UtcTimestamp,
        };

        let lease = BrowserProviderLease {
            lease_id: BrowserLeaseId::generate(),
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            browser_session_id: BrowserSessionId::generate(),
            page_instance_id: PageInstanceId::generate(),
            provider_id: CapabilityProviderId::generate(),
            runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
            capabilities: BTreeSet::from([CapabilityKey::from_versioned_key(
                VersionedKey::new("browser.python.execute@1").expect("capability"),
            )]),
            resource_profile_ref: VersionedKey::new("browser.resource.standard@1")
                .expect("resource profile"),
            epoch: BrowserLeaseEpoch::new(1).expect("epoch"),
            fence: FencingToken::new(1).expect("fence"),
            issued_at: UtcTimestamp::from_unix_millis(1),
            expires_at: UtcTimestamp::from_unix_millis(2),
        };
        let wire = encode_browser_provider_lease(&lease).expect("encode");
        assert_eq!(
            decode_browser_provider_lease(wire.clone()).expect("decode"),
            lease
        );

        let mut invalid = wire;
        invalid.capability_keys = vec!["browser.shell.execute@1".to_owned()];
        assert!(matches!(
            decode_browser_provider_lease(invalid),
            Err(BrowserProtocolCodecError::Domain(
                BrowserJobContractError::InvalidLease
            ))
        ));

        let proof = BrowserLeaseProof::from(&lease);
        let wire = encode_browser_lease_proof(proof);
        assert_eq!(decode_browser_lease_proof(wire).expect("decode"), proof);
    }

    #[test]
    fn browser_feature_probe_round_trips_and_rejects_duplicate_engines() {
        let probe = BrowserRuntimeFeatureProbe {
            worker: true,
            web_assembly: true,
            crypto_subtle: true,
            transferable_array_buffer: true,
            service_worker: true,
            indexed_db: true,
            secure_context: true,
            engines: BTreeSet::from([BrowserJobEngine::Python, BrowserJobEngine::StaticValidate]),
        };
        let wire = encode_browser_feature_probe(&probe).expect("encode");
        assert_eq!(
            decode_browser_feature_probe(wire.clone()).expect("decode"),
            probe
        );

        let mut invalid = wire;
        invalid.engines.push(wire::BrowserJobEngine::Python as i32);
        assert!(matches!(
            decode_browser_feature_probe(invalid),
            Err(BrowserProtocolCodecError::InvalidContract)
        ));
    }
}
