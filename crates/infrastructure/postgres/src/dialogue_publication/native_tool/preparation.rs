//! Read original invocation facts and stage immutable content before acquiring
//! the short publication transaction. Staged bytes alone are never public.
use super::*;
use musteragent_context::RuntimeSnapshotDecoder;

pub(super) struct Prepared {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub executor: ParticipantId,
    pub tool_key: String,
    pub request: ContentRef,
    pub publication: NativePublication,
    pub files: Vec<musteragent_runtime_file_query::FileReference>,
    pub message: Option<ContentRef>,
    pub result: CapabilityResult,
}
impl Prepared {
    pub fn entry_count(&self) -> usize {
        self.files.len()
            + self.publication.artifacts.len()
            + usize::from(self.publication.text.is_some())
    }
}

impl PostgresFencedDialogueToolPublication {
    pub(super) async fn prepare(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> Result<Prepared, Failure> {
        let row = sqlx::query("SELECT i.tenant_id,i.tool_ref,i.capability_provider_id,i.request_hash,
                r.executor_participant_id,s.encoded_snapshot,s.content_hash AS snapshot_hash,
                c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
            FROM tool.tool_invocations i
            JOIN runtime.runs r ON r.run_id=i.run_id AND r.tenant_id=i.tenant_id
            JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id
                AND s.tenant_id=r.tenant_id AND s.run_id=r.run_id AND s.conversation_id=r.conversation_id
            JOIN artifact.content_refs c ON c.content_id=i.request_content_id AND c.tenant_id=i.tenant_id AND c.state=2
            WHERE i.invocation_id=$1 AND i.run_id=$2 AND i.effect=2 AND i.approval_policy=1")
            .bind(invocation_id.into_uuid()).bind(attempt.run_id.into_uuid())
            .fetch_optional(self.ledger.database().pool()).await.map_err(database_error)?
            .ok_or(Failure::Forbidden)?;
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id").map_err(database_error)?)
            .map_err(|_| Failure::Corrupt)?;
        let executor = ParticipantId::from_uuid(
            row.try_get("executor_participant_id")
                .map_err(database_error)?,
        )
        .map_err(|_| Failure::Corrupt)?;
        let tool_key: String = row.try_get("tool_ref").map_err(database_error)?;
        if !matches!(
            tool_key.as_str(),
            DIALOGUE_PUBLISH_TOOL | ARTIFACTS_DELIVER_TOOL | ARTIFACTS_DELIVER_V2_TOOL
        ) {
            return Err(Failure::Forbidden);
        }
        let encoded: Vec<u8> = row.try_get("encoded_snapshot").map_err(database_error)?;
        if row
            .try_get::<Vec<u8>, _>("snapshot_hash")
            .map_err(database_error)?
            != Sha256::digest(&encoded).into_bytes()
        {
            return Err(Failure::Corrupt);
        }
        let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
            .decode_runtime_snapshot(&encoded)
            .map_err(|_| Failure::Corrupt)?;
        snapshot.validate_sealed().map_err(|_| Failure::Corrupt)?;
        let mut pins = snapshot
            .prompt
            .sections
            .iter()
            .filter(|p| p.section_key.as_str() == "dialogue.output-contract");
        let pin = pins.next().ok_or(Failure::Forbidden)?;
        if pins.next().is_some()
            || !DialogueOutputContract::from_prompt_ref(&pin.content_ref)
                .is_some_and(DialogueOutputContract::is_native)
        {
            return Err(Failure::Forbidden);
        }
        let definition = snapshot
            .tools
            .iter()
            .find(|tool| tool.manifest.key.as_str() == tool_key)
            .ok_or(Failure::Forbidden)?;
        if row
            .try_get::<uuid::Uuid, _>("capability_provider_id")
            .map_err(database_error)?
            != definition.manifest.capability_provider_id.into_uuid()
            || !snapshot.capabilities.iter().any(|grant| {
                grant.key.as_str() == definition.manifest.capability_key.as_str()
                    && grant.provider_id == definition.manifest.capability_provider_id
            })
        {
            return Err(Failure::Forbidden);
        }
        // Source resolution denies workspace runs, wrong participants, wrong
        // accounts and detached temporary subagents before staging any bytes.
        let mut connection = self
            .ledger
            .database()
            .pool()
            .acquire()
            .await
            .map_err(database_error)?;
        let source = load_source(&mut connection, attempt.run_id)
            .await
            .map_err(publication_error)?;
        if source.actor.tenant_id != tenant_id || source.agent_id != snapshot.agent.agent_id {
            return Err(Failure::Forbidden);
        }
        drop(connection);
        let request = ContentRef {
            content_id: ContentId::from_uuid(row.try_get("content_id").map_err(database_error)?)
                .map_err(|_| Failure::Corrupt)?,
            media_type: MediaType::new(
                row.try_get::<String, _>("media_type")
                    .map_err(database_error)?,
            )
            .map_err(|_| Failure::Corrupt)?,
            byte_length: row
                .try_get::<String, _>("byte_length")
                .map_err(database_error)?
                .parse()
                .map_err(|_| Failure::Corrupt)?,
            sha256: Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("content_hash")
                    .map_err(database_error)?,
            )
            .map_err(|_| Failure::Corrupt)?,
        };
        if row
            .try_get::<Vec<u8>, _>("request_hash")
            .map_err(database_error)?
            != request.sha256.into_bytes()
        {
            return Err(Failure::Corrupt);
        }
        if request.byte_length > MAX_PUBLICATION_TOOL_BYTES as u64 {
            return Err(Failure::InvalidArguments);
        }
        let bytes = self
            .content
            .read(tenant_id, &request)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        verify_content(&request, &bytes).map_err(|_| Failure::Corrupt)?;
        let decoded = decode_dialogue_tool_publication(&tool_key, &bytes)?;
        let publication = decoded.publication;
        let files = decoded.files;
        let message = match &publication.text {
            Some(text) => Some(
                self.stage(
                    tenant_id,
                    invocation_id,
                    b"public-tool-message@1",
                    "text/markdown;charset=utf-8",
                    text.as_bytes().to_vec(),
                )
                .await?,
            ),
            None => None,
        };
        let entry_count =
            files.len() + publication.artifacts.len() + usize::from(message.is_some());
        let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
            &invocation_id.into_uuid(),
            b"public-tool-delivery@1",
        ))
        .map_err(|_| Failure::Corrupt)?;
        let receipt = serde_json::to_vec(&serde_json::json!({
            "delivery_id": delivery_id.to_string(), "committed": true, "entry_count": entry_count,
        }))
        .map_err(|_| Failure::Corrupt)?;
        let result_ref = self
            .stage(
                tenant_id,
                invocation_id,
                b"public-tool-receipt@1",
                "application/json",
                receipt,
            )
            .await?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: definition.manifest.output_schema.clone(),
            provider_request_ref: None,
        };
        Ok(Prepared {
            tenant_id,
            run_id: attempt.run_id,
            executor,
            tool_key,
            request,
            publication,
            files,
            message,
            result,
        })
    }

    async fn stage(
        &self,
        tenant_id: TenantId,
        invocation: InvocationId,
        key: &[u8],
        media_type: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, Failure> {
        self.content
            .put_at(
                ContentId::from_uuid(uuid::Uuid::new_v5(&invocation.into_uuid(), key))
                    .map_err(|_| Failure::Corrupt)?,
                PutContent {
                    tenant_id,
                    media_type: MediaType::new(media_type).map_err(|_| Failure::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| Failure::Infrastructure)
    }
}
