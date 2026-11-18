use super::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemResultHome {
    Cloud,
    Device,
}
impl WorkItemResultHome {
    pub fn key(self) -> &'static str {
        match self {
            Self::Cloud => "cloud",
            Self::Device => "device",
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultScope {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub work_item_id: WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
    pub producing_run_id: RunId,
    pub producing_snapshot_id: RuntimeSnapshotId,
    pub producing_agent_id: AgentId,
    pub requesting_agent_id: AgentId,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub producer_home: WorkItemResultHome,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultAsset {
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    /// Original asset owner proves both the private version and the authorized return.
    pub authority_sha256: Sha256,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultItem {
    pub item_id: uuid::Uuid,
    pub content_ref: ContentRef,
    pub asset: Option<WorkItemResultAsset>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultManifest {
    pub scope: WorkItemResultScope,
    pub submission_invocation_id: InvocationId,
    pub request_sha256: Sha256,
    pub items: Vec<WorkItemResultItem>,
}
pub fn work_item_result_id(invocation: InvocationId) -> uuid::Uuid {
    uuid::Uuid::new_v5(&invocation.into_uuid(), b"work-item-result@1")
}
pub fn work_item_result_item_id(result_id: uuid::Uuid, ordinal: usize) -> uuid::Uuid {
    uuid::Uuid::new_v5(&result_id, format!("item:{ordinal}").as_bytes())
}
pub fn work_item_result_delivery_id(result_id: uuid::Uuid, dialogue: AgentDialogueId) -> CommandId {
    CommandId::from_uuid(uuid::Uuid::new_v5(
        &result_id,
        format!("requester-delivery@1:{dialogue}").as_bytes(),
    ))
    .expect("derived id")
}
impl WorkItemResultManifest {
    pub fn result_id(&self) -> uuid::Uuid {
        work_item_result_id(self.submission_invocation_id)
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        let s = &self.scope;
        if [s.goal_revision, s.control_revision, s.assignment_epoch]
            .iter()
            .any(|n| *n == 0 || *n > i64::MAX as u64)
            || self.items.is_empty()
            || self.items.len() > musteragent_execution_context::MAX_PUBLIC_DELIVERIES
            || self.request_sha256.into_bytes() == [0; 32]
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut message = false;
        let mut assets = std::collections::BTreeSet::new();
        for (index, item) in self.items.iter().enumerate() {
            if item.item_id != work_item_result_item_id(self.result_id(), index) {
                return Err(DialogueStoreError::Corrupt);
            }
            match &item.asset {
                Some(a)
                    if a.authority_sha256.into_bytes() != [0; 32]
                        && assets.insert(a.artifact_id) => {}
                None if !message
                    && index == 0
                    && item.content_ref.media_type.as_str() == "text/markdown;charset=utf-8"
                    && item.content_ref.content_id
                        == work_item_result_message_id(self.submission_invocation_id)
                    && item.content_ref.byte_length > 0
                    && item.content_ref.byte_length
                        <= musteragent_execution_context::MAX_PUBLICATION_TOOL_BYTES as u64 =>
                {
                    message = true;
                }
                _ => return Err(DialogueStoreError::Corrupt),
            }
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let s = &self.scope;
        let items=self.items.iter().map(|item| {
            let c=&item.content_ref;
            serde_json::json!({"item_id":item.item_id,"content":{"content_id":c.content_id.to_string(),"media_type":c.media_type.as_str(),"byte_length":c.byte_length,"sha256":c.sha256.to_string()},
                "asset":item.asset.as_ref().map(|a|serde_json::json!({"artifact_id":a.artifact_id.to_string(),"artifact_version_id":a.artifact_version_id.to_string(),"authority_sha256":a.authority_sha256.to_string()}))})
        }).collect::<Vec<_>>();
        serde_json::to_vec(&serde_json::json!({"schema":"musteragent.task-result-manifest.v1","result_id":self.result_id(),
            "tenant_id":s.tenant_id.to_string(),"owner_id":s.owner_id.to_string(),"task_id":s.work_item_id.to_string(),"goal_revision":s.goal_revision,
            "control_revision":s.control_revision,"assignment_epoch":s.assignment_epoch,"producing_run_id":s.producing_run_id.to_string(),
            "producing_snapshot_id":s.producing_snapshot_id.to_string(),"producing_agent_id":s.producing_agent_id.to_string(),
            "requesting_agent_id":s.requesting_agent_id.to_string(),"source_dialogue_id":s.source_dialogue_id.to_string(),"source_intent_id":s.source_intent_id.to_string(),
            "producer_home":s.producer_home.key(),"submission_invocation_id":self.submission_invocation_id.to_string(),"request_sha256":self.request_sha256.to_string(),"items":items}))
            .map_err(|_|DialogueStoreError::Corrupt)
    }
    pub fn receipt(&self) -> Result<WorkItemResultSubmissionReceipt, DialogueStoreError> {
        Ok(WorkItemResultSubmissionReceipt {
            result_id: self.result_id(),
            work_item_id: self.scope.work_item_id,
            producing_run_id: self.scope.producing_run_id,
            manifest_sha256: Sha256::digest(&self.canonical_bytes()?),
            item_ids: self.items.iter().map(|i| i.item_id).collect(),
        })
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultSubmissionReceipt {
    pub result_id: uuid::Uuid,
    pub work_item_id: WorkItemId,
    pub producing_run_id: RunId,
    pub manifest_sha256: Sha256,
    pub item_ids: Vec<uuid::Uuid>,
}
impl WorkItemResultSubmissionReceipt {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::json!({"status":"submitted_for_verification","task_id":self.work_item_id.to_string(),"producing_run_id":self.producing_run_id.to_string(),
            "result_id":self.result_id,"manifest_sha256":self.manifest_sha256.to_string(),"item_ids":self.item_ids}).to_string().into_bytes()
    }
}

impl WorkItemResultSubmissionReceipt {
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            status: String,
            task_id: uuid::Uuid,
            producing_run_id: uuid::Uuid,
            result_id: uuid::Uuid,
            manifest_sha256: String,
            item_ids: Vec<uuid::Uuid>,
        }
        if bytes.len() > 65536 {
            return Err(DialogueStoreError::Corrupt);
        }
        let w: Wire = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let receipt = Self {
            result_id: w.result_id,
            work_item_id: WorkItemId::from_uuid(w.task_id)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            producing_run_id: RunId::from_uuid(w.producing_run_id)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            manifest_sha256: Sha256::from_hex(&w.manifest_sha256)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            item_ids: w.item_ids,
        };
        if w.status != "submitted_for_verification"
            || receipt.result_id.is_nil()
            || receipt.manifest_sha256.into_bytes() == [0; 32]
            || receipt.item_ids.is_empty()
            || receipt.item_ids.len() > musteragent_execution_context::MAX_PUBLIC_DELIVERIES
            || receipt
                .item_ids
                .iter()
                .enumerate()
                .any(|(n, id)| *id != work_item_result_item_id(receipt.result_id, n))
            || receipt.canonical_bytes() != bytes
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(receipt)
    }
}
