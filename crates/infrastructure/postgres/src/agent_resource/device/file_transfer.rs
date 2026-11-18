//! Canonical FileQuery resolves both the association and its original bytes.
//! No Cloud Run is manufactured for a native import. Each bounded read repeats
//! the existing custody, frozen Tool, membership and erasure checks.
use super::*;
use musteragent_runtime_file_query::*;

pub(super) fn service(
    auth: Arc<authority::Authority>,
    content: Arc<dyn ContentStore>,
) -> Result<(FileQueryService, FileQueryAccess), Error> {
    let inv = prepare::invocation(&auth)?;
    let catalog = Arc::new(ManagedResourceFileCatalog::for_owned_agent(
        auth.clone(),
        Arc::new(crate::PostgresResourceStore::new(
            auth.store.database.clone(),
        )),
    ));
    let sources = Arc::new(native_input::NativeInputSource(auth.clone()));
    let dialogue =
        Arc::new(AgentFileCatalog::new(catalog.clone(), sources.clone()).with_outputs(sources));
    let app = Arc::new(
        FileQueryApplication::new(
            catalog.clone(),
            catalog,
            Arc::new(ContentFileReader::new(content.clone())),
        )
        .with_dialogue_catalog(dialogue)
        .with_content_store(content),
    );
    let access = FileQueryAccess {
        actor: auth.actor,
        conversation_id: Some(inv.conversation_id),
        workspace_id: None,
        run: Some(RunFileQueryAccess {
            invocation_id: Some(inv.invocation_id),
            context: *auth.request.command().run_context(),
            attempt: auth.request.attempt().clone(),
            authorized_content: inv.authorized_content,
            timing: inv.timing,
        }),
    };
    Ok((FileQueryService::new(auth, app), access))
}

impl PostgresDeviceResourceStore {
    pub(super) async fn read_source_file(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceResourceRequest,
        offset: u64,
    ) -> Result<DeviceFileChunk, Error> {
        if request.command().operation() != DeviceResourceOperation::WorkspaceFileImport {
            return Err(Error::Forbidden);
        }
        let input = DeviceFileImportInput::parse(request.command().request_bytes())?;
        let file_ref = input.file_ref.as_ref().ok_or(Error::Forbidden)?;
        if input.artifact_id.is_some() {
            return Err(Error::Forbidden);
        }
        let auth = Arc::new(self.admitted(actor, target, request).await?);
        let (service, access) = service(auth.clone(), self.content.clone())?;
        let descriptor = service
            .open(&access, file_ref)
            .await
            .map_err(failure::query)?;
        let reference = match input.content_id {
            Some(id) => std::iter::once(&descriptor.primary)
                .chain(&descriptor.dependencies)
                .find(|r| r.content_id == id)
                .ok_or(Error::ReferenceUnavailable)?
                .clone(),
            None => descriptor.primary.clone(),
        };
        if offset > reference.byte_length {
            return Err(Error::InvalidArguments);
        }
        // open_verified checks complete immutable Content identity, not just a
        // caller-selected range. FileQuery rechecks association after the IO.
        self.content
            .validate_current(actor.tenant_id, &reference)
            .await
            .map_err(|_| Error::ReferenceUnavailable)?;
        let key = request.command().commitment().to_string();
        let retained = self
            .file_material
            .lock()
            .map_err(|_| Error::Infrastructure)?
            .get(&key, &reference);
        let reader = match retained {
            Some(reader) => reader,
            None => {
                let reader = Arc::new(
                    self.content
                        .open_verified(actor.tenant_id, &reference)
                        .await
                        .map_err(|_| Error::ReferenceUnavailable)?,
                );
                self.content
                    .validate_current(actor.tenant_id, &reference)
                    .await
                    .map_err(|_| Error::ReferenceUnavailable)?;
                self.file_material
                    .lock()
                    .map_err(|_| Error::Infrastructure)?
                    .put(key, reference.clone(), reader.clone());
                reader
            }
        };
        let mut data =
            vec![0; (reference.byte_length - offset).min(DEVICE_FILE_CHUNK_BYTES as u64) as usize];
        reader
            .read_exact_at(&mut data, offset)
            .map_err(|_| Error::Infrastructure)?;
        let current = service
            .open(&access, file_ref)
            .await
            .map_err(failure::query)?;
        if current != descriptor {
            return Err(Error::Conflict);
        }
        auth.current().await?;
        self.content
            .validate_current(actor.tenant_id, &reference)
            .await
            .map_err(|_| Error::ReferenceUnavailable)?;
        let chunk = DeviceFileChunk {
            command_commitment: request.command().commitment(),
            content_ref: reference,
            offset,
            eof: data.is_empty(),
            data,
        };
        chunk.validate(request, offset)?;
        Ok(chunk)
    }
}
