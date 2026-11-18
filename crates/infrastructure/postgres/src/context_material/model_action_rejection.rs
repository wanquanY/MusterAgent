use musteragent_context::{
    MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS, MaterializedModelActionRejection,
};
use musteragent_identity::{RunId, TenantId};
use musteragent_runtime_contract::{
    MAX_TOOL_ARGUMENT_VALIDATION_ISSUES, ToolArgumentValidationIssue,
};
use musteragent_tool::WireName;
use sqlx::{PgPool, Row as _};

use super::PostgresContextMaterialError;

#[derive(PartialEq)]
enum Fingerprint {
    Tool(String, String, Vec<ToolArgumentValidationIssue>),
    Public(
        musteragent_identity::VersionedKey,
        musteragent_execution_context::PublicDeliveryIssue,
    ),
}
impl Fingerprint {
    fn same_budget(&self, other: &Self) -> bool {
        match (self, other) {
            // Switching invalid shape/size/reference must not reset the budget.
            (Self::Public(a, _), Self::Public(b, _)) => a == b,
            _ => self == other,
        }
    }
}
pub(super) async fn load_latest_model_action_rejection(
    pool: &PgPool,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<Option<MaterializedModelActionRejection>, PostgresContextMaterialError> {
    let dialogue = super::public_delivery_recovery::load(pool, tenant_id, run_id).await?;
    if matches!(&dialogue, Some(MaterializedModelActionRejection::InvalidPublicDelivery { consecutive_count, .. })
        if consecutive_count.get() >= MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS)
    {
        return Ok(dialogue);
    }
    let latest = load_latest_consecutive_rejection(pool, tenant_id, run_id).await?;
    Ok(match latest {
        Some(MaterializedModelActionRejection::InvalidPublicDelivery { .. }) | None => {
            dialogue.or(latest)
        }
        other => other,
    })
}

async fn load_latest_consecutive_rejection(
    pool: &PgPool,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<Option<MaterializedModelActionRejection>, PostgresContextMaterialError> {
    let rows=sqlx::query(
        "SELECT invocation.status,invocation.terminal_error,rejection.error_code,
                rejection.attempted_wire_name,rejection.validation_issues,
                rejection.rejection_kind,rejection.delivery_contract_key,rejection.delivery_issue
         FROM runtime.model_invocations invocation
         JOIN runtime.context_plans plan ON plan.tenant_id=invocation.tenant_id
           AND plan.run_id=invocation.run_id AND plan.invocation_id=invocation.invocation_id
         LEFT JOIN runtime.model_action_rejections rejection ON rejection.invocation_id=invocation.invocation_id
         WHERE invocation.tenant_id=$1 AND invocation.run_id=$2 AND invocation.purpose=1
         ORDER BY plan.iteration DESC,invocation.started_at_ms DESC,invocation.invocation_id DESC LIMIT $3")
        .bind(tenant_id.into_uuid()).bind(run_id.into_uuid())
        .bind(i64::from(MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS)).fetch_all(pool).await?;
    let mut latest: Option<Fingerprint> = None;
    let mut count = 0u32;
    for row in rows {
        let code: Option<String> = row.try_get("error_code")?;
        let terminal: Option<Vec<u8>> = row.try_get("terminal_error")?;
        if row.try_get::<i16, _>("status")? != 3
            || code.is_none()
            || terminal.as_deref() != code.as_deref().map(str::as_bytes)
        {
            break;
        }
        let kind: Option<String> = row.try_get("rejection_kind")?;
        let name: Option<String> = row.try_get("attempted_wire_name")?;
        let issues = row
            .try_get::<Option<serde_json::Value>, _>("validation_issues")?
            .map(decode_validation_issues)
            .transpose()?
            .unwrap_or_default();
        let fingerprint = match (kind.as_deref(), code.as_deref()) {
            (Some("tool"), Some("tool_not_exposed" | "invalid_command")) => Fingerprint::Tool(
                code.expect("code checked"),
                name.ok_or(PostgresContextMaterialError::CorruptState)?,
                issues,
            ),
            (Some("public_delivery"), Some("model_response_invalid"))
                if name.is_none() && issues.is_empty() =>
            {
                let key: String = row.try_get("delivery_contract_key")?;
                if musteragent_execution_context::PublicDeliveryContract::from_key(&key).is_none() {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                Fingerprint::Public(
                    musteragent_identity::VersionedKey::new(key)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                    musteragent_execution_context::PublicDeliveryIssue::parse(
                        row.try_get::<&str, _>("delivery_issue")?,
                    )
                    .ok_or(PostgresContextMaterialError::CorruptState)?,
                )
            }
            _ => return Err(PostgresContextMaterialError::CorruptState),
        };
        if let Some(previous) = &latest {
            if !previous.same_budget(&fingerprint) {
                break;
            }
        } else {
            latest = Some(fingerprint);
        }
        count += 1;
    }
    let Some(latest) = latest else {
        return Ok(None);
    };
    let consecutive_count =
        std::num::NonZeroU32::new(count).ok_or(PostgresContextMaterialError::CorruptState)?;
    Ok(Some(match latest {
        Fingerprint::Public(contract_ref, issue) => {
            MaterializedModelActionRejection::InvalidPublicDelivery {
                contract_ref,
                issue,
                consecutive_count,
            }
        }
        Fingerprint::Tool(code, name, issues) => {
            let attempted_name =
                WireName::new(name).map_err(|_| PostgresContextMaterialError::CorruptState)?;
            match code.as_str() {
                "tool_not_exposed" => MaterializedModelActionRejection::ToolNotExposed {
                    attempted_name,
                    consecutive_count,
                },
                "invalid_command" => MaterializedModelActionRejection::InvalidToolArguments {
                    attempted_name,
                    issues,
                    consecutive_count,
                },
                _ => return Err(PostgresContextMaterialError::CorruptState),
            }
        }
    }))
}

fn decode_validation_issues(
    value: serde_json::Value,
) -> Result<Vec<ToolArgumentValidationIssue>, PostgresContextMaterialError> {
    let values = value
        .as_array()
        .filter(|values| values.len() <= MAX_TOOL_ARGUMENT_VALIDATION_ISSUES)
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    let mut issues = Vec::with_capacity(values.len());
    for value in values {
        let object = value
            .as_object()
            .filter(|object| object.len() == 2)
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        let kind = object
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok())
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        let path = object
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        issues.push(
            ToolArgumentValidationIssue::new(kind, path)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        );
    }
    if issues.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    Ok(issues)
}
