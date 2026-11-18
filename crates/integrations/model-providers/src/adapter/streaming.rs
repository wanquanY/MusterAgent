//! Shared native transport/normalization; policy supervision stays at Home.
use super::*;
mod keepalive;

impl DirectModelProvider {
    pub(super) fn stream_prepared(
        &self,
        request: CanonicalModelRequest,
        prepared: PreparedDirectRequest,
        control: Option<Arc<execution::ModelControl>>,
    ) -> ModelEventStream<'_> {
        Box::pin(stream! {
            let stream_budget = request.stream_budget;
            let PreparedDirectRequest { provider_revision, client, endpoint, mut outbound, credential_lease, timing, wire_credit, mut recovery_headers } = prepared;
            if let Some(lease) = credential_lease
                && let Err(error) = lease.refresh(self, &mut outbound, &mut recovery_headers).await {
                yield Err(error);
                return;
            }
            let mut shared = match self.acquire_dependency(&request, &endpoint, provider_revision.as_deref()).await {
                Ok(permit) => permit,
                Err(error) => { yield Err(error); return; }
            };
            // `total_timeout` is retained as the endpoint registry's stable
            // wire field, but it bounds only connection plus response-header
            // startup. An active response stream has no wall-clock ceiling.
            if let Some(control) = &control
                && let Err(error) = control.will_dispatch() {
                yield Err(error);
                return;
            }
            let mut google_continuation = match google_continuation::ContinuationRequest::prepare(&outbound, request.route.api_format, endpoint.origin.is_gateway()) {
                Ok(value) => value,
                Err(error) => { yield Err(error); return; }
            };
            timing.sending();
            let response_start_deadline = Instant::now() + endpoint.total_timeout;
            let mut response = match timeout_at(response_start_deadline, outbound.send(client.clone())).await {
                Ok(Ok(value)) => value,
                Ok(Err(mut error)) => {
                    if endpoint.origin.is_gateway() && error.safe_code == ProviderErrorCode::TransportFailed {
                        error.safe_code = ProviderErrorCode::GatewayUnavailable;
                    }
                    dependency_recovery::observe(shared.take(), Some(&error)).await;
                    yield Err(error);
                    return;
                }
                Err(_) => {
                    tracing::warn!(provider.failure = "response_start_timeout", "Model response did not start");
                    let mut error = timeout_error();
                    if endpoint.origin.is_gateway() { error.safe_code = ProviderErrorCode::GatewayUnavailable; }
                    dependency_recovery::observe(shared.take(), Some(&error)).await;
                    yield Err(error);
                    return;
                }
            };
            drop(wire_credit);
            timing.headers();
            let (resolved, recovered) = match self.resolve_existing_response(&request, &endpoint, &recovery_headers, response).await {
                Ok(response) => response,
                Err(error) => {
                    if error.safe_code == ProviderErrorCode::ExistingInvocationFailed
                        && let Some(control) = &control
                        && let Err(error) = control.terminal_observed() {
                        yield Err(error);
                        return;
                    }
                    // Accepted/pending is not remote-stop proof. A missing or
                    // failed lookup must never open provider retry/fallback.
                    dependency_recovery::observe(shared.take(), Some(&error)).await;
                    yield Err(error);
                    return;
                }
            };
            response = resolved;
            if !response.status().is_success() {
                if let Some(control) = &control
                    && let Err(error) = control.terminal_observed() {
                    yield Err(error);
                    return;
                }
                let mut error = http_status_error(
                    request.route.api_format,
                    endpoint.origin.is_gateway(),
                    response,
                    stream_budget
                        .max_event_bytes
                        .get()
                        .min(MAX_PROVIDER_ERROR_BODY_BYTES),
                ).await;
                dependency_recovery::observe_upstream(shared.take(), &mut error).await;
                yield Err(error);
                return;
            }
            // A success status is startup admission only. Shared health is
            // published once from the terminal generation outcome below; an
            // interrupted or cancelled stream publishes no health claim.
            let invocation_ref = format!(
                "invocation-{}",
                request.invocation.invocation_id
            );
            let publish_disclosed_reasoning =
                request.reasoning_policy == ReasoningPolicy::Summary
                    && request
                        .route
                        .capabilities
                        .iter()
                        .any(|capability| capability.as_str() == "reasoning_summary");
            let normalizer_result = ProviderNormalizer::new(
                request.route.api_format,
                ModelRouteRef::from_key(request.route.route_ref.clone()),
                request.route.resolved_model_key.clone(),
                invocation_ref,
            )
            .and_then(|normalizer| normalizer.with_stream_budget(stream_budget))
            .and_then(|normalizer| normalizer.with_tool_definitions(&request.tools))
            .map(|normalizer| {
                normalizer.with_gateway(endpoint.origin.is_gateway()).with_disclosed_reasoning_output(publish_disclosed_reasoning)
            });
            let mut state = ModelStreamState::default();
            let recovery_profile =
                DirectProviderRecoveryProfile::for_api_format(request.route.api_format);
            // Every exit from this block is the single terminal outcome of the
            // physical generation; the shared dependency observation is
            // published once from `outcome` after the block.
            let outcome: Result<(), ProviderError> = 'generation: {
            let mut normalizer = match normalizer_result {
                Ok(value) => value,
                Err(_) => break 'generation Err(protocol_error()),
            };
            if recovery_profile.wire_protocol == DirectProviderWireProtocol::AwsEventStream {
                let mut raw = response.bytes_stream();
                let mut buffer = BytesMut::new();
                let mut decoder = MessageFrameDecoder::new();
                let mut total_wire_bytes = 0_u64;
                let mut provider_event_count = 0_u32;
                loop {
                    let next = match timeout_at(
                        Instant::now() + endpoint.read_timeout,
                        raw.next(),
                    ).await {
                        Ok(value) => value,
                        Err(_) => {
                            tracing::warn!(provider.failure = "stream_read_idle_timeout", "Model transport became inactive");
                            break 'generation Err(timeout_error());
                        }
                    };
                    match next {
                        Some(Ok(chunk)) => {
                            timing.transport_bytes(!chunk.is_empty());
                            let Ok(chunk_bytes) = u64::try_from(chunk.len()) else {
                                break 'generation Err(protocol_error());
                            };
                            total_wire_bytes = match total_wire_bytes.checked_add(chunk_bytes) {
                                Some(value) if value <= stream_budget.max_total_bytes.get() => value,
                                _ => {
                                    break 'generation Err(protocol_error());
                                }
                            };
                            if buffer.len().saturating_add(chunk.len())
                                > stream_budget.max_event_bytes.get()
                            {
                                break 'generation Err(protocol_error());
                            }
                            buffer.extend_from_slice(&chunk);
                            loop {
                                match decoder.decode_frame(&mut buffer) {
                                    Ok(DecodedFrame::Complete(message)) => {
                                        provider_event_count =
                                            match provider_event_count.checked_add(1) {
                                                Some(value)
                                                    if value <= stream_budget.max_events.get() =>
                                                {
                                                    value
                                                }
                                                _ => {
                                                    break 'generation Err(protocol_error());
                                                }
                                            };
                                        let (event_name, value) = match decode_aws_message(
                                            &message,
                                            request.route.api_format,
                                            stream_budget.max_event_bytes.get(),
                                        ) {
                                            Ok(value) => value,
                                            Err(()) => {
                                                break 'generation Err(protocol_error());
                                            }
                                        };
                                        timing.provider_event();
                                        let event_subtype = provider_event_subtype(&value);
                                        let events = match normalizer.observe(
                                            Some(&event_name),
                                            value,
                                        ) {
                                            Ok(value) => value,
                                            Err(error) => {
                                                tracing::warn!(
                                                    provider.failure = "response_normalization",
                                                    api_format = ?request.route.api_format,
                                                    event_name,
                                                    event_subtype = event_subtype.as_deref().unwrap_or(""),
                                                    provider.protocol_error = error.telemetry_kind().as_str(),
                                                    "Model Provider response normalization failed"
                                                );
                                                break 'generation Err(protocol_error());
                                            }
                                        };
                                        for mut event in events {
                                            if let ModelStreamEvent::ResponseError(error) = &mut event
                                            {
                                                existing_response::classify_terminal_error(error, recovered);
                                                dependency_recovery::observe_upstream(shared.take(), error).await;
                                            }
                                            if let Err(error) = state.accept(&event) {
                                                tracing::warn!(
                                                    provider.failure = "canonical_stream_validation",
                                                    api_format = ?request.route.api_format,
                                                    event_name,
                                                    provider.protocol_error = canonical_error_kind(error).as_str(),
                                                    "Model Provider canonical stream validation failed"
                                                );
                                                break 'generation Err(protocol_error());
                                            }
                                            if matches!(event, ModelStreamEvent::ResponseCompleted { .. } | ModelStreamEvent::ResponseError(_))
                        && let Some(control) = &control
                        && let Err(error) = control.terminal_observed() {
                        break 'generation Err(error);
                    }
                    timing.canonical(&event);
                                            yield Ok(event);
                                        }
                                    }
                                    Ok(DecodedFrame::Incomplete) => break,
                                    Err(_) => {
                                        break 'generation Err(protocol_error());
                                    }
                                }
                            }
                        }
                        Some(Err(_)) => {
                            break 'generation Err(provider_error(
                                ProviderErrorKind::Transport,
                                ProviderErrorCode::TransportFailed,
                                true,
                                None,
                            ));
                        }
                        None => break,
                    }
                }
                if !buffer.is_empty() {
                    // EOF inside a syntactically incomplete AWS frame is a
                    // transport interruption. A fully received malformed
                    // frame is still rejected above as a protocol violation.
                    break 'generation Err(transport_interruption());
                }
            } else {
                let mut provider_event_count = 0_u32;
                let mut total_wire_bytes = 0_u64;
                loop {
                let raw = response.bytes_stream();
                let transport_timing = &timing;
                let budgeted = stream! {
                    futures_util::pin_mut!(raw);
                    loop {
                        // Measure transport activity before SSE decoding: a
                        // comment heartbeat or a partial frame is still a read.
                        // Canonical progress has its own Agent Loop deadline.
                        let next = match timeout_at(Instant::now() + endpoint.read_timeout, raw.next()).await {
                            Ok(Some(next)) => next,
                            Ok(None) => break,
                            Err(_) => {
                                tracing::warn!(provider.failure = "stream_read_idle_timeout", "Model transport became inactive");
                                yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "model transport inactive"));
                                return;
                            }
                        };
                        match next {
                            Ok(chunk) => {
                                transport_timing.transport_bytes(!chunk.is_empty());
                                let Ok(chunk_bytes) = u64::try_from(chunk.len()) else {
                                    yield Err(std::io::Error::other("provider chunk size overflow"));
                                    return;
                                };
                                total_wire_bytes = match total_wire_bytes.checked_add(chunk_bytes) {
                                    Some(value) if value <= stream_budget.max_total_bytes.get() => value,
                                    _ => {
                                        yield Err(std::io::Error::other(
                                            "provider stream cumulative byte budget exceeded",
                                        ));
                                        return;
                                    }
                                };
                                yield Ok(chunk);
                            }
                            Err(error) => {
                                http_clients::transport_error(&error);
                                yield Err(std::io::Error::other(error));
                                return;
                            }
                        }
                    }
                };
                let mut events = Box::pin(budgeted).eventsource();
                loop {
                    let next = events.next().await;
                    let Some(frame) = next else {
                        break;
                    };
                    let frame = match frame {
                        Ok(value) => value,
                        Err(EventStreamError::Transport(error)) => {
                            break 'generation Err(if error.kind() == std::io::ErrorKind::TimedOut {
                                timeout_error()
                            } else {
                                transport_interruption()
                            });
                        }
                        Err(EventStreamError::Utf8(_) | EventStreamError::Parser(_)) => {
                            break 'generation Err(protocol_error());
                        }
                    };
                    provider_event_count = match provider_event_count.checked_add(1) {
                        Some(value) if value <= stream_budget.max_events.get() => value,
                        _ => {
                            break 'generation Err(protocol_error());
                        }
                    };
                    if frame.data.len() > stream_budget.max_event_bytes.get() {
                        break 'generation Err(protocol_error());
                    }
                    match keepalive::classify(&frame.event, &frame.data) {
                        Ok(true) => continue,
                        Ok(false) => {},
                        Err(()) => {
                            // Report protocol shape only; never log response
                            // contents, timestamps, credentials or tool args.
                            let keys = serde_json::from_str::<Value>(&frame.data).ok()
                                .and_then(|v| v.as_object().map(|m| m.keys().filter(|k| k.len() <= 32
                                    && k.bytes().all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)))
                                    .take(12).cloned().collect::<Vec<_>>()));
                            tracing::warn!(provider.failure="invalid_transport_keepalive", metadata_keys=?keys,
                                "Named keepalive contained unsupported payload fields");
                            break 'generation Err(protocol_error());
                        }
                    }
                    timing.provider_event();
                    if frame.data.trim() == "[DONE]" {
                        let terminal = match normalizer.finish() {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::warn!(
                                    provider.failure = "response_finish",
                                    api_format = ?request.route.api_format,
                                    provider.protocol_error = error.telemetry_kind().as_str(),
                                    "Model Provider response normalization failed"
                                );
                                break 'generation Err(finish_error(error));
                            }
                        };
                        for mut event in terminal {
                            if let ModelStreamEvent::ResponseError(error) = &mut event {
                                existing_response::classify_terminal_error(error, recovered);
                                dependency_recovery::observe_upstream(shared.take(), error).await;
                            }
                            if let Err(error) = state.accept(&event) {
                                tracing::warn!(
                                    provider.failure = "canonical_stream_validation",
                                    api_format = ?request.route.api_format,
                                    provider.protocol_error = canonical_error_kind(error).as_str(),
                                    "Model Provider canonical stream validation failed"
                                );
                                break 'generation Err(protocol_error());
                            }
                            if matches!(event, ModelStreamEvent::ResponseCompleted { .. } | ModelStreamEvent::ResponseError(_))
                        && let Some(control) = &control
                        && let Err(error) = control.terminal_observed() {
                        break 'generation Err(error);
                    }
                    timing.canonical(&event);
                            yield Ok(event);
                        }
                        break;
                    }
                    let value: Value = match serde_json::from_str(&frame.data) {
                        Ok(value) => value,
                        Err(_) => {
                            break 'generation Err(protocol_error());
                        }
                    };
                    let event_name = frame.event.as_str();
                    let event_subtype = provider_event_subtype(&value);
                    let normalized = match normalizer.observe(Some(event_name), value) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::warn!(
                                provider.failure = "response_normalization",
                                api_format = ?request.route.api_format,
                                event_name,
                                event_subtype = event_subtype.as_deref().unwrap_or(""),
                                provider.protocol_error = error.telemetry_kind().as_str(),
                                "Model Provider response normalization failed"
                            );
                            break 'generation Err(protocol_error());
                        }
                    };
                    for mut event in normalized {
                        if let ModelStreamEvent::ResponseError(error) = &mut event {
                            existing_response::classify_terminal_error(error, recovered);
                            dependency_recovery::observe_upstream(shared.take(), error).await;
                        }
                        if let Err(error) = state.accept(&event) {
                            tracing::warn!(
                                provider.failure = "canonical_stream_validation",
                                api_format = ?request.route.api_format,
                                event_name,
                                provider.protocol_error = canonical_error_kind(error).as_str(),
                                "Model Provider canonical stream validation failed"
                            );
                            break 'generation Err(protocol_error());
                        }
                        if matches!(event, ModelStreamEvent::ResponseCompleted { .. } | ModelStreamEvent::ResponseError(_))
                        && let Some(control) = &control
                        && let Err(error) = control.terminal_observed() {
                        break 'generation Err(error);
                    }
                    timing.canonical(&event);
                        yield Ok(event);
                    }
                }
                drop(events);
                let Some(proof) = normalizer.take_google_continuation() else { break; };
                let Some(continuation) = google_continuation.as_mut() else {
                    break 'generation Err(protocol_error());
                };
                let next = match continuation.next(request.route.api_format, proof) {
                    Ok(next) => next,
                    Err(error) => break 'generation Err(error),
                };
                // This is a new, provider-proven hop of the same generation,
                // never a retry of an uncertain request. Supervision and the
                // cumulative byte/event budgets span the complete generation.
                response = match timeout_at(Instant::now() + endpoint.total_timeout, client.execute(next)).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => break 'generation Err(http_clients::transport_error(&error)),
                    Err(_) => break 'generation Err(timeout_error()),
                };
                if !response.status().is_success() {
                    break 'generation Err(http_status_error(request.route.api_format, false, response, stream_budget.max_event_bytes.get().min(MAX_PROVIDER_ERROR_BODY_BYTES)).await);
                }
                }
            }
            if !state.is_terminal() {
                let terminal = match normalizer.finish() {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(
                            provider.failure = "response_finish",
                            api_format = ?request.route.api_format,
                            provider.protocol_error = error.telemetry_kind().as_str(),
                            "Model Provider response normalization failed"
                        );
                        break 'generation Err(finish_error(error));
                    }
                };
                for mut event in terminal {
                    if let ModelStreamEvent::ResponseError(error) = &mut event {
                        existing_response::classify_terminal_error(error, recovered);
                        dependency_recovery::observe_upstream(shared.take(), error).await;
                    }
                    if let Err(error) = state.accept(&event) {
                        tracing::warn!(
                            provider.failure = "canonical_stream_validation",
                            api_format = ?request.route.api_format,
                            provider.protocol_error = canonical_error_kind(error).as_str(),
                            "Model Provider canonical stream validation failed"
                        );
                        break 'generation Err(protocol_error());
                    }
                    if matches!(event, ModelStreamEvent::ResponseCompleted { .. } | ModelStreamEvent::ResponseError(_))
                        && let Some(control) = &control
                        && let Err(error) = control.terminal_observed() {
                        break 'generation Err(error);
                    }
                    timing.canonical(&event);
                    yield Ok(event);
                }
            }
            Ok(())
            };
            match outcome {
                Ok(()) => {
                    // A terminal canonical ResponseError already published its
                    // own outcome above; `take` makes this completion write
                    // the health observation at most once.
                    dependency_recovery::observe(shared.take(), None).await;
                }
                Err(mut error) => {
                    if recovered {
                        // Replaying an accepted result is lookup, not a fresh
                        // inference. An interrupted/malformed replay must not
                        // enter ordinary dependency retries with a new key.
                        error = crate::existing_invocation::failure(ProviderErrorCode::ExistingInvocationLookupFailed);
                    }
                    // An interrupted connection to our own gateway carries no
                    // observation about its upstream provider. Typed terminal
                    // frames already published their upstream evidence above.
                    if endpoint.origin.is_gateway() && matches!(error.safe_code,
                        ProviderErrorCode::TransportFailed | ProviderErrorCode::DeadlineExceeded) {
                        error.safe_code = ProviderErrorCode::GatewayUnavailable;
                    }
                    dependency_recovery::observe(shared.take(), Some(&error)).await;
                    yield Err(error);
                }
            }
        })
    }
}
