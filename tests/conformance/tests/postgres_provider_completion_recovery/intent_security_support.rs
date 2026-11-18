use super::*;

pub(super) struct DeleteGateBlobStore {
    inner: MemoryBlobs,
    pub(super) put_unavailable: AtomicBool,
    pub(super) read_unavailable: AtomicBool,
    pub(super) unavailable: AtomicBool,
}

impl DeleteGateBlobStore {
    pub(super) fn new() -> Self {
        Self {
            inner: MemoryBlobs::in_memory(),
            put_unavailable: AtomicBool::new(false),
            read_unavailable: AtomicBool::new(false),
            unavailable: AtomicBool::new(true),
        }
    }

    pub(super) fn staging_fault() -> Self {
        Self {
            inner: MemoryBlobs::in_memory(),
            put_unavailable: AtomicBool::new(false),
            read_unavailable: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
        }
    }
}

impl BlobStore for DeleteGateBlobStore {
    fn put_stream<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<
            musteragent_runtime_content::PutBlobReceipt,
            musteragent_runtime_content::ContentStoreError,
        >,
    > {
        if self.put_unavailable.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(musteragent_runtime_content::ContentStoreError::Unavailable)
            });
        }
        self.inner.put_stream(identity, length, hash, source)
    }
    fn copy_to<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        if self.read_unavailable.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(musteragent_runtime_content::ContentStoreError::Unavailable)
            });
        }
        self.inner.copy_to(identity, locator, length, hash, sink)
    }

    fn put(
        &self,
        request: musteragent_runtime_content::PutBlob,
    ) -> ApplicationFuture<
        '_,
        Result<
            musteragent_runtime_content::PutBlobReceipt,
            musteragent_runtime_content::BlobStoreError,
        >,
    > {
        if self.put_unavailable.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(musteragent_runtime_content::BlobStoreError::Unavailable)
            });
        }
        self.inner.put(request)
    }

    fn read<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, musteragent_runtime_content::BlobStoreError>> {
        if self.read_unavailable.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(musteragent_runtime_content::BlobStoreError::Unavailable)
            });
        }
        self.inner.read(identity, locator)
    }

    fn delete<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), musteragent_runtime_content::BlobStoreError>> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(musteragent_runtime_content::BlobStoreError::Unavailable)
            });
        }
        self.inner.delete(identity, locator)
    }

    fn derive_locator(
        &self,
        identity: musteragent_runtime_content::BlobIdentity,
    ) -> Result<
        musteragent_runtime_content::StorageLocator,
        musteragent_runtime_content::BlobStoreError,
    > {
        self.inner.derive_locator(identity)
    }
}

pub(super) struct LargeToolThenFinalProvider {
    pub(super) calls: AtomicUsize,
}

impl LargeToolThenFinalProvider {
    pub(super) fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ModelProvider for LargeToolThenFinalProvider {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => large_tool_completion_events(),
            1 => final_no_usage_events(),
            _ => panic!("unexpected Provider redispatch"),
        };
        Box::pin(tokio_stream::iter(events.into_iter().map(Ok)))
    }
}

fn large_tool_completion_events() -> Vec<ModelStreamEvent> {
    const MEBIBYTE: usize = 1_048_576;
    let call_id = ToolCallId::new("large-recovery-tool").expect("tool call id");
    let argument = "a".repeat(MEBIBYTE - 1_024);
    let mut events = vec![ModelStreamEvent::ResponseStarted {
        response_ref: provider_ref("large-tool-fault-confirmation"),
        response_model_key: response_model_key(),
    }];
    for ordinal in 0..8 {
        let marker = char::from(b'0' + ordinal);
        events.push(ModelStreamEvent::ReasoningDelta {
            output_index: 0,
            delta: ModelText::new(marker.to_string().repeat(MEBIBYTE)).expect("reasoning chunk"),
        });
    }
    events.extend([
        ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: call_id.clone(),
            name: WireName::new("large_recovery_tool").expect("wire name"),
            provider_item_ref: None,
            provider_replay: None,
        },
        ModelStreamEvent::ToolCallArgumentsDelta {
            output_index: 0,
            call_id: call_id.clone(),
            delta: format!("{{\"payload\":\"{argument}\"}}"),
        },
        ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id,
            arguments: JsonValue::Object(BTreeMap::from([(
                "payload".into(),
                JsonValue::String(argument),
            )])),
        },
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        },
    ]);
    events
}
