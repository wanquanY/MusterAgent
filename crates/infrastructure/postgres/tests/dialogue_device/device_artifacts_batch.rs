//! A staged multi-file delivery must have one complete, recoverable receipt.
use super::*;

#[tokio::test]
async fn native_staged_batch_publishes_every_file_and_replays_the_complete_receipt() {
    let f = Fixture::admitted().await;
    let mut exports = (0..3).map(|_| f.export(None, false)).collect::<Vec<_>>();
    for (index, export) in exports.iter_mut().enumerate() {
        let entry = &mut export.manifest.entries[0];
        entry.title = format!("native-result-{index}.txt");
        entry.producer.tool_call_id = ToolCallId::new(format!("file_{index}")).unwrap();
        for content in export.contents.entries() {
            let stored = f
                .content
                .put_at(
                    content.content_ref.content_id,
                    PutContent {
                        tenant_id: f.actor.tenant_id,
                        media_type: content.content_ref.media_type.clone(),
                        bytes: content.bytes.clone(),
                    },
                )
                .await
                .unwrap();
            assert_eq!(stored, content.content_ref);
        }
    }
    let arguments = serde_json::json!({"purpose":"reply","caption":"All three files.",
        "artifacts": exports.iter().map(|e| {
            let entry = &e.manifest.entries[0];
            serde_json::json!({"artifact_ref":{"kind":"artifact","id":entry.artifact_id.to_string()},
                "artifact_tool_call_id":entry.producer.tool_call_id.as_str()})
        }).collect::<Vec<_>>()});
    let request = serde_json::to_vec(&arguments).unwrap();
    let delivery = DeviceToolDelivery::new(
        f.binding.clone(),
        InvocationId::generate(),
        ARTIFACTS_DELIVER_TOOL.into(),
        reference("application/json", &request),
        request,
    )
    .unwrap();
    let manifest = DeviceArtifactExportManifest {
        request_fingerprint: delivery.fingerprint(),
        scope: exports[0].manifest.scope.clone(),
        entries: exports
            .iter()
            .map(|e| e.manifest.entries[0].clone())
            .collect(),
    };
    let contents = DeviceArtifactContents::staged(&delivery, &manifest).unwrap();
    let first = f
        .publisher
        .publish_device_artifacts(f.actor, &f.target, &delivery, &manifest, &contents)
        .await
        .unwrap();
    first.validate_for(&delivery, &manifest).unwrap();
    assert_eq!(first.delivery_receipt.entry_ids.len(), 4);
    assert_eq!(
        f.publisher
            .lookup_device_artifact_delivery(f.actor, &f.target, &delivery, &manifest)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        f.publisher
            .publish_device_artifacts(f.actor, &f.target, &delivery, &manifest, &contents)
            .await
            .unwrap(),
        first
    );
    let reader = f.artifacts();
    for (entry_id, expected) in first
        .delivery_receipt
        .entry_ids
        .iter()
        .skip(1)
        .zip(&manifest.entries)
    {
        let view = reader
            .describe(
                f.actor,
                f.custody.source.scope.dialogue_id,
                *entry_id,
                CorrelationId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(view.title, expected.title);
        let reference = reader
            .authorize_content(
                f.actor,
                f.custody.source.scope.dialogue_id,
                *entry_id,
                view.current_version.artifact_version_id,
                expected.version.content_ref.clone(),
                CorrelationId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(reference, expected.version.content_ref);
    }
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_publications WHERE source_tool_invocation_id=$1",
    )
    .bind(delivery.invocation_id().into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        total, 4,
        "exact replay must not add partial or duplicate public entries"
    );
}
