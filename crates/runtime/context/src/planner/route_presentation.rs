//! Present canonical attachments within the active route's original capacity.
//! This changes provider representation, never source identity or authority.
use super::*;

#[cfg(test)]
pub(super) fn present_message_for_route(
    message: &CanonicalMessage,
    route: &ModelRoute,
) -> Result<CanonicalMessage, ContextPlanError> {
    present_messages_for_route(&[message], route)
        .and_then(|mut messages| messages.pop().ok_or_else(ContextPlanError::invariant))
}

#[cfg(test)]
pub(super) fn present_messages_for_route(
    messages: &[&CanonicalMessage],
    route: &ModelRoute,
) -> Result<Vec<CanonicalMessage>, ContextPlanError> {
    present_messages_for_route_with_vision_tool(messages, route, true)
}

/// Native first: every attachment the active route can receive natively is
/// sent as original bytes; only what it cannot receive falls back to our own
/// readers (vision_analyze for images when available, the file tools for
/// documents) or to an honest notice for audio, video, or unavailable vision.
pub(super) fn present_messages_for_route_with_vision_tool(
    messages: &[&CanonicalMessage],
    route: &ModelRoute,
    vision_tool_available: bool,
) -> Result<Vec<CanonicalMessage>, ContextPlanError> {
    let image_presented = if route.accepts_native_image() {
        present_messages_for_visual_route(messages, route)?
    } else {
        messages
            .iter()
            .map(|message| {
                present_message_for_nonvisual_image_route(message, vision_tool_available)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let presented = image_presented.iter().collect::<Vec<_>>();
    let documents = native_document_selection(&presented, route);
    presented
        .iter()
        .zip(documents)
        .map(|(message, include)| {
            present_message_with_documents(message, &include)
                .and_then(|message| present_media_for_route(&message, route))
        })
        .collect()
}

/// Projects authorized media parts returned by `files.read` onto the active
/// route's native input capabilities.
///
/// Parts the route can receive natively remain typed so the caller can read and
/// hash-verify their bytes before mounting them. Unsupported parts become
/// route-presentation notices using the same formatting rules as context
/// planning, without reading their source bytes or instructing the model to
/// call a tool that is not available in this run.
pub fn present_file_read_parts_for_route(
    parts: &[CanonicalContentPart],
    route: &ModelRoute,
    vision_tool_available: bool,
) -> Result<Vec<CanonicalContentPart>, ContextPlanError> {
    let unsupported_image_ids = if route.accepts_native_image() {
        Vec::new()
    } else {
        parts
            .iter()
            .filter_map(|part| match part {
                CanonicalContentPart::Image(reference) => Some(reference.content_id.to_string()),
                CanonicalContentPart::Text(_)
                | CanonicalContentPart::Audio(_)
                | CanonicalContentPart::Video(_)
                | CanonicalContentPart::File(_) => None,
            })
            .collect::<Vec<_>>()
    };
    let image_notice = if unsupported_image_ids.is_empty() {
        None
    } else {
        Some(nonvisual_image_notice(
            &unsupported_image_ids,
            vision_tool_available,
        )?)
    };
    let mut emitted_image_notice = false;
    let mut presented = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            CanonicalContentPart::Image(_) if !route.accepts_native_image() => {
                if !emitted_image_notice {
                    presented.push(CanonicalContentPart::Text(
                        image_notice
                            .clone()
                            .ok_or_else(ContextPlanError::invariant)?,
                    ));
                    emitted_image_notice = true;
                }
            }
            CanonicalContentPart::Audio(reference) if !route.accepts_native_audio() => {
                presented.push(CanonicalContentPart::Text(unsupported_media_notice(
                    "audio", reference,
                )?));
            }
            CanonicalContentPart::Video(reference) if !route.accepts_native_video() => {
                presented.push(CanonicalContentPart::Text(unsupported_media_notice(
                    "video", reference,
                )?));
            }
            CanonicalContentPart::File(reference)
                if !route.accepts_native_document(reference.media_type.as_str()) =>
            {
                presented.push(CanonicalContentPart::Text(
                    unsupported_file_read_document_notice(reference)?,
                ));
            }
            _ => presented.push(part.clone()),
        }
    }
    Ok(presented)
}

fn nonvisual_image_notice(
    image_ids: &[String],
    vision_tool_available: bool,
) -> Result<ModelText, ContextPlanError> {
    let text = if vision_tool_available {
        format!(
            "[Authorized image access: vision_tool_required content_ids=[{}]. No matching image pixels are included because the active model route cannot inspect them natively. Before making visual claims, call the already exposed vision_analyze Tool once with images=[{{content_id: ...}}, ...]; do not call once per image. Use analysis_mode=joint for cross-image comparison and independent otherwise.]",
            image_ids.join(",")
        )
    } else {
        format!(
            "[Authorized image access: content_ids=[{}]. No matching image pixels are included because the active model route cannot inspect images natively and no visual analysis tool is available in this run. Do not guess what the image contains; adjust your approach or tell the user this run cannot inspect images directly.]",
            image_ids.join(",")
        )
    };
    ModelText::new(text).map_err(|_| ContextPlanError::invariant())
}

fn unsupported_media_notice(
    kind: &str,
    reference: &musteragent_content_ref::ContentRef,
) -> Result<ModelText, ContextPlanError> {
    ModelText::new(format!(
        "[Authorized {kind} attachment content_id={} ({}, {} bytes). The active model route cannot hear or watch {kind} natively, so none of its content is included. Do not guess what it contains; tell the user this model cannot process {kind} and that a model with native {kind} input can.]",
        reference.content_id,
        reference.media_type.as_str(),
        reference.byte_length,
    ))
    .map_err(|_| ContextPlanError::invariant())
}

fn unsupported_file_read_document_notice(
    reference: &musteragent_content_ref::ContentRef,
) -> Result<ModelText, ContextPlanError> {
    ModelText::new(format!(
        "[Authorized document content_id={} ({}, {} bytes). The active model route cannot receive this document as original bytes, so none of its raw content is included. Do not guess what it contains; use another available document or parser tool if exposed, or tell the user this model cannot read this document directly.]",
        reference.content_id,
        reference.media_type.as_str(),
        reference.byte_length,
    ))
    .map_err(|_| ContextPlanError::invariant())
}

/// Bounds the original document bytes one request carries. Newest messages
/// win; the rest are read through the file tools like before.
pub(super) const MAX_NATIVE_DOCUMENTS_PER_REQUEST: usize = 5;
pub(super) const MAX_NATIVE_DOCUMENT_BYTES: u64 = 20 * 1024 * 1024;

fn native_document_selection(messages: &[&CanonicalMessage], route: &ModelRoute) -> Vec<Vec<bool>> {
    let mut include = messages
        .iter()
        .map(|message| vec![false; message.content.len()])
        .collect::<Vec<_>>();
    let mut count = 0_usize;
    let mut total = 0_u64;
    for (message_index, message) in messages.iter().enumerate().rev() {
        for (part_index, part) in message.content.iter().enumerate() {
            let CanonicalContentPart::File(reference) = part else {
                continue;
            };
            let Some(next) = total.checked_add(reference.byte_length) else {
                continue;
            };
            if count < MAX_NATIVE_DOCUMENTS_PER_REQUEST
                && next <= MAX_NATIVE_DOCUMENT_BYTES
                && route.accepts_native_document(reference.media_type.as_str())
            {
                include[message_index][part_index] = true;
                count += 1;
                total = next;
            }
        }
    }
    include
}

/// Audio and video the route cannot receive become a plain notice instead of
/// failing the whole turn, so the model can tell the user what it lacks.
fn present_media_for_route(
    message: &CanonicalMessage,
    route: &ModelRoute,
) -> Result<CanonicalMessage, ContextPlanError> {
    let unsupported = |part: &CanonicalContentPart| match part {
        CanonicalContentPart::Audio(_) => !route.accepts_native_audio(),
        CanonicalContentPart::Video(_) => !route.accepts_native_video(),
        _ => false,
    };
    if !message.content.iter().any(unsupported) {
        return Ok(message.clone());
    }
    let mut presented = message.clone();
    presented.content = message
        .content
        .iter()
        .map(|part| {
            if !unsupported(part) {
                return Ok(part.clone());
            }
            let (kind, reference) = match part {
                CanonicalContentPart::Audio(reference) => ("audio", reference),
                CanonicalContentPart::Video(reference) => ("video", reference),
                _ => return Err(ContextPlanError::invariant()),
            };
            unsupported_media_notice(kind, reference).map(CanonicalContentPart::Text)
        })
        .collect::<Result<Vec<_>, _>>()?;
    presented
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(presented)
}

fn present_messages_for_visual_route(
    messages: &[&CanonicalMessage],
    route: &ModelRoute,
) -> Result<Vec<CanonicalMessage>, ContextPlanError> {
    let profile =
        VisionInputProfile::from_route(route).map_err(|_| ContextPlanError::invariant())?;
    let mut include = messages
        .iter()
        .map(|message| vec![false; message.content.len()])
        .collect::<Vec<_>>();
    let mut image_count = 0_usize;
    let mut total_bytes = 0_u64;
    for (message_index, message) in messages.iter().enumerate().rev() {
        // Prefer the newest message, but preserve attachment order within that
        // message so capacity overflow forms deterministic contiguous batches.
        for (part_index, part) in message.content.iter().enumerate() {
            let CanonicalContentPart::Image(reference) = part else {
                continue;
            };
            let Some(next_total) = total_bytes.checked_add(reference.byte_length) else {
                continue;
            };
            if image_count < profile.max_images_per_request
                && reference.byte_length <= profile.max_bytes_per_image
                && next_total <= profile.max_total_image_bytes
            {
                include[message_index][part_index] = true;
                image_count += 1;
                total_bytes = next_total;
            }
        }
    }

    messages
        .iter()
        .zip(include)
        .map(|(message, include)| present_visual_message(message, &include, profile))
        .collect()
}

fn present_visual_message(
    message: &CanonicalMessage,
    include: &[bool],
    profile: VisionInputProfile,
) -> Result<CanonicalMessage, ContextPlanError> {
    if include.len() != message.content.len() {
        return Err(ContextPlanError::invariant());
    }
    let omitted: Vec<_> = message
        .content
        .iter()
        .zip(include)
        .filter_map(|(part, included)| match part {
            CanonicalContentPart::Image(reference) if !included => {
                Some(reference.content_id.to_string())
            }
            _ => None,
        })
        .collect();
    if omitted.is_empty() {
        return Ok(message.clone());
    }
    let hint = ModelText::new(format!(
        "[Image access: vision_tool_required content_ids=[{}]. These images exceed the current input capacity. Use vision_analyze for their pixels in ordered groups of at most {}.]",
        omitted.join(","), profile.max_images_per_request,
    )).map_err(|_| ContextPlanError::invariant())?;
    let mut hint = Some(CanonicalContentPart::Text(hint));
    let mut content = Vec::with_capacity(message.content.len());
    for (part, included) in message.content.iter().zip(include) {
        if matches!(part, CanonicalContentPart::Image(_)) && !included {
            content.extend(hint.take());
        } else {
            content.push(part.clone());
        }
    }
    let mut presented = message.clone();
    presented.content = content;
    presented
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(presented)
}

fn present_message_for_nonvisual_image_route(
    message: &CanonicalMessage,
    vision_tool_available: bool,
) -> Result<CanonicalMessage, ContextPlanError> {
    let mut presented = message.clone();
    let image_ids = message
        .content
        .iter()
        .filter_map(|part| match part {
            CanonicalContentPart::Image(reference) => Some(reference.content_id.to_string()),
            CanonicalContentPart::Text(_)
            | CanonicalContentPart::Audio(_)
            | CanonicalContentPart::Video(_)
            | CanonicalContentPart::File(_) => None,
        })
        .collect::<Vec<_>>();
    let batch_hint = if image_ids.is_empty() {
        None
    } else {
        Some(nonvisual_image_notice(&image_ids, vision_tool_available)?)
    };
    let mut emitted_batch_hint = false;
    let mut content = Vec::with_capacity(message.content.len());
    for part in &message.content {
        match part {
            CanonicalContentPart::Image(_) if !emitted_batch_hint => {
                content.push(CanonicalContentPart::Text(
                    batch_hint.clone().ok_or_else(ContextPlanError::invariant)?,
                ));
                emitted_batch_hint = true;
            }
            CanonicalContentPart::Image(_) => {}
            CanonicalContentPart::Text(_)
            | CanonicalContentPart::Audio(_)
            | CanonicalContentPart::Video(_)
            | CanonicalContentPart::File(_) => {
                content.push(part.clone());
            }
        }
    }
    presented.content = content;
    presented
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(presented)
}

#[cfg(test)]
pub(super) fn present_message_without_opaque_documents(
    message: &CanonicalMessage,
) -> Result<CanonicalMessage, ContextPlanError> {
    present_message_with_documents(message, &vec![false; message.content.len()])
}

/// Keeps the documents marked in `include` as original bytes and replaces the
/// others with one reader notice at the first omitted position.
fn present_message_with_documents(
    message: &CanonicalMessage,
    include: &[bool],
) -> Result<CanonicalMessage, ContextPlanError> {
    if include.len() != message.content.len() {
        return Err(ContextPlanError::invariant());
    }
    let omitted = |index: usize| {
        matches!(message.content[index], CanonicalContentPart::File(_)) && !include[index]
    };
    let document_ids = message
        .content
        .iter()
        .enumerate()
        .filter_map(|(index, part)| match part {
            CanonicalContentPart::File(reference) if omitted(index) => {
                Some(reference.content_id.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if document_ids.is_empty() {
        return Ok(message.clone());
    }
    let document_hint = ModelText::new(format!(
        "[Authorized document attachments in order: content_ids=[{}]. The active model route cannot receive these documents as original bytes (unsupported type or over the per-request budget), and no document content is included here. Use the file-reading tools in the current callable surface to obtain the authorized file_ref and read these sources before making claims about their contents. The listing returns them after the workspace files: keep paging with next_cursor exactly as returned and with the other arguments unchanged until each content_id above is found. Follow the file-tool guidance for the current release; do not activate an old attachment tool when the unified reader is already exposed. Preserve extraction status, coverage and parser issues. A preparing representation is not a broken file; partial extraction and a final text page do not prove full document coverage.]",
        document_ids.join(",")
    ))
    .map_err(|_| ContextPlanError::invariant())?;
    let mut emitted_document_hint = false;
    let mut content = Vec::with_capacity(message.content.len());
    for (index, part) in message.content.iter().enumerate() {
        if !omitted(index) {
            content.push(part.clone());
        } else if !emitted_document_hint {
            content.push(CanonicalContentPart::Text(document_hint.clone()));
            emitted_document_hint = true;
        }
    }
    let mut presented = message.clone();
    presented.content = content;
    presented
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(presented)
}
