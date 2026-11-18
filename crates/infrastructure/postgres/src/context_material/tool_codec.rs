//! Canonical Tool exchange content decoding for Context materialization.

use super::*;

pub(crate) fn decode_tool_message(
    bytes: &[u8],
    expected_call_id: &ToolCallId,
    expected_name: &WireName,
) -> Result<CanonicalMessage, PostgresContextMaterialError> {
    let message = decode_tool_message_body(bytes)?;
    if message.tool_call_id.as_ref() != Some(expected_call_id)
        || message.tool_name.as_ref() != Some(expected_name)
    {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    Ok(message)
}

pub(super) fn decode_tool_message_body(
    bytes: &[u8],
) -> Result<CanonicalMessage, PostgresContextMaterialError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let object = value
        .as_object()
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    let schema = object
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    if !matches!(
        schema,
        "musteragent.legacy.runtime.tool-result.v1" | "musteragent.legacy.runtime.tool-result.v2"
    ) || object.get("role").and_then(serde_json::Value::as_str) != Some("tool")
    {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    let call_id = ToolCallId::new(
        object
            .get("tool_call_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresContextMaterialError::CorruptState)?,
    )
    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let name = WireName::new(
        object
            .get("tool_name")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresContextMaterialError::CorruptState)?,
    )
    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let content = object
        .get("content")
        .and_then(serde_json::Value::as_array)
        .ok_or(PostgresContextMaterialError::CorruptState)?
        .iter()
        .map(|value| decode_tool_content_part(schema, value))
        .collect::<Result<Vec<_>, _>>()?;
    let message = CanonicalMessage {
        role: CanonicalRole::Tool,
        content,
        tool_calls: Vec::new(),
        tool_call_id: Some(call_id),
        tool_name: Some(name),
        reasoning: None,
    };
    message
        .validate()
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    Ok(message)
}

fn decode_tool_content_part(
    schema: &str,
    value: &serde_json::Value,
) -> Result<CanonicalContentPart, PostgresContextMaterialError> {
    if schema == "musteragent.legacy.runtime.tool-result.v1" {
        return ModelText::new(
            value
                .as_str()
                .ok_or(PostgresContextMaterialError::CorruptState)?
                .to_owned(),
        )
        .map(CanonicalContentPart::Text)
        .map_err(|_| PostgresContextMaterialError::CorruptState);
    }
    let object = value
        .as_object()
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    match object.get("type").and_then(serde_json::Value::as_str) {
        Some("text") => ModelText::new(
            object
                .get("text")
                .and_then(serde_json::Value::as_str)
                .ok_or(PostgresContextMaterialError::CorruptState)?
                .to_owned(),
        )
        .map(CanonicalContentPart::Text)
        .map_err(|_| PostgresContextMaterialError::CorruptState),
        Some(kind @ ("image" | "audio" | "video" | "file")) => {
            let reference = decode_embedded_content_ref(
                object
                    .get("content_ref")
                    .ok_or(PostgresContextMaterialError::CorruptState)?,
            )?;
            Ok(match kind {
                "image" => CanonicalContentPart::Image(reference),
                "audio" => CanonicalContentPart::Audio(reference),
                "video" => CanonicalContentPart::Video(reference),
                "file" => CanonicalContentPart::File(reference),
                _ => unreachable!("closed content kind"),
            })
        }
        _ => Err(PostgresContextMaterialError::CorruptState),
    }
}

fn decode_embedded_content_ref(
    value: &serde_json::Value,
) -> Result<ContentRef, PostgresContextMaterialError> {
    let object = value
        .as_object()
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    let content_id = object
        .get("content_id")
        .and_then(serde_json::Value::as_str)
        .ok_or(PostgresContextMaterialError::CorruptState)?
        .parse::<ContentId>()
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let media_type = MediaType::new(
        object
            .get("media_type")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresContextMaterialError::CorruptState)?,
    )
    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let byte_length = object
        .get("byte_length")
        .and_then(serde_json::Value::as_u64)
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    let hash = object
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    if hash.len() != Sha256::LENGTH * 2 {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    let mut hash_bytes = [0_u8; Sha256::LENGTH];
    for (index, byte) in hash_bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&hash[offset..offset + 2], 16)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    }
    Ok(ContentRef {
        content_id,
        media_type,
        byte_length,
        sha256: Sha256::from_bytes(hash_bytes)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
    })
}
