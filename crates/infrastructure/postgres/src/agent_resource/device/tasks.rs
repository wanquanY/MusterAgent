//! Native transport composition for the original WorkItem owner.
//! This module does not choose a Task, create a Cloud Run, or infer authority.
use super::*;
use musteragent_content_ref::SchemaRef;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_tool::{
    BuiltinToolCatalog, BuiltinToolDefinition, ProductExecutionTarget, ToolKey,
};

pub(super) async fn prepare(
    store: &PostgresDeviceResourceStore,
    auth: Arc<authority::Authority>,
    content: Arc<RecordingContentStore>,
) -> Result<prepare::Prepared, Error> {
    let command = auth.request.command();
    let message = crate::work_item_execution::native::stage_result_message(
        auth.actor,
        command,
        content.as_ref(),
    )
    .await?;
    let (mut tx, context, custody) = auth.current_snapshot().await?;
    let plan = crate::work_item_execution::native::prepare(
        &mut tx,
        crate::work_item_execution::native::NativeWorkItemAuthority {
            actor: auth.actor,
            custody: &custody,
            authenticated_origin: &auth.target,
        },
        &context,
        command,
        content.clone(),
        store.task_targets.as_deref(),
        message.as_ref(),
    )
    .await?;
    tx.commit().await.map_err(|_| Error::Infrastructure)?;
    let reference = match plan.stage_submitted_result(content.as_ref()).await? {
        Some(private) => private,
        None => {
            store
                .stage(auth.actor, command, b"native-task-result@1", plan.bytes())
                .await?
        }
    };
    Ok(prepare::Prepared::Task(
        Box::new(plan),
        CapabilityResult {
            result_hash: reference.sha256,
            result_ref: reference,
            result_schema: command.invocation().result_schema.clone(),
            provider_request_ref: None,
        },
    ))
}

pub(super) fn resolve_contract(
    release: Option<&ProductReleaseRef>,
    command: &DeviceResourceCommand,
) -> Result<BuiltinToolDefinition, Error> {
    let inv = command.invocation();
    resolve(
        release,
        &inv.tool_key,
        &inv.request_schema,
        &inv.result_schema,
    )
}

fn resolve(
    release: Option<&ProductReleaseRef>,
    key: &ToolKey,
    input: &SchemaRef,
    output: &SchemaRef,
) -> Result<BuiltinToolDefinition, Error> {
    let operation = DeviceResourceOperation::from_tool_key(key.as_str())?;
    let target = if operation.is_task() {
        // A historical Cloud ceiling is not evidence that this release shipped
        // Native Task control. Do not search later releases for legacy None.
        release.ok_or(Error::Forbidden)?;
        ProductExecutionTarget::Device
    } else {
        // Preserve the existing canonical Resource historical contract lookup.
        ProductExecutionTarget::Cloud
    };
    BuiltinToolCatalog::resolve_historical_contract(
        release,
        ProductId::KERNEL,
        target,
        key,
        input,
        output,
    )
    .map_err(|_| Error::Forbidden)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_task_contract_requires_its_exact_device_publication() {
        let current_release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let new = &current_release;
        let new_device =
            BuiltinToolCatalog::for_product_release(new, ProductExecutionTarget::Device).unwrap();
        let mut unknown = current_release.clone();
        unknown.digest = Sha256::digest(b"unpublished release").into_bytes();
        for key in [
            "tasks.start@3",
            "tasks.list@1",
            "tasks.inspect@1",
            "tasks.control@1",
            "tasks.amend@2",
            "tasks.verify@3",
            "tasks.submit_result@1",
            "tasks.handoff@1",
            "tasks.followup@1",
        ] {
            let key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
            let historical = new_device.by_key(&key).unwrap();
            assert!(
                resolve(
                    Some(&unknown),
                    &key,
                    &historical.input_schema,
                    &historical.output_schema
                )
                .is_err(),
                "an unrecognized publication cannot authorize a Native operation"
            );
            assert!(
                resolve(
                    None,
                    &key,
                    &historical.input_schema,
                    &historical.output_schema
                )
                .is_err(),
                "missing original release must not adopt a later publication"
            );
            let current = new_device.by_key(&key).unwrap();
            assert!(
                resolve(
                    Some(new),
                    &key,
                    &current.input_schema,
                    &current.output_schema
                )
                .is_ok()
            );
            let mut changed = current.input_schema.clone();
            changed.schema_hash = Sha256::digest(b"not the published Task schema");
            assert!(resolve(Some(new), &key, &changed, &current.output_schema).is_err());
        }
        // Existing Native Goal controls remain supported in the sealed release.
        for key in ["goal.create@3", "goal.draft.save@1"] {
            let key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
            let tool = new_device.by_key(&key).unwrap();
            assert!(resolve(Some(new), &key, &tool.input_schema, &tool.output_schema).is_ok());
        }
    }
}
