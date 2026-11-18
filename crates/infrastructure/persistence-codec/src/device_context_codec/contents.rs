use super::*;

pub fn contents_query(
    value: &DeviceContextContentsRequest,
) -> Result<wire::NativeDialogueContextContentsQuery, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextContentsQuery {
        binding: Some(receipt::binding(&value.state.binding)),
        compaction_policy_ref: Some(policy(&value.state.compaction_policy_ref)),
        fingerprint: value.fingerprint.into_bytes().to_vec(),
        include_approval_context: value.state.include_approval_context,
        locators: value.locators.iter().copied().map(locator).collect(),
    })
}
pub fn decode_contents_query(
    value: wire::NativeDialogueContextContentsQuery,
) -> Result<DeviceContextContentsRequest, Error> {
    let result = DeviceContextContentsRequest {
        state: DeviceContextStateRequest {
            include_scene_members: false,
            include_approval_context: value.include_approval_context,
            include_execution_context: false,
            binding: decode_binding(value.binding)?,
            compaction_policy_ref: decode_policy(value.compaction_policy_ref)?,
        },
        fingerprint: hash(value.fingerprint)?,
        locators: value
            .locators
            .into_iter()
            .map(|v| decode_locator(Some(v)))
            .collect::<Result<_, _>>()?,
    };
    result.validate().map_err(|_| BAD)?;
    Ok(result)
}
pub fn encode_contents_query(value: &DeviceContextContentsRequest) -> Result<Vec<u8>, Error> {
    bounded(contents_query(value)?, DEVICE_CONTEXT_REQUEST_LIMIT)
}
pub fn contents_query_from_bytes(bytes: &[u8]) -> Result<DeviceContextContentsRequest, Error> {
    decode_contents_query(canonical(bytes, DEVICE_CONTEXT_REQUEST_LIMIT)?)
}
pub fn contents(
    value: &DeviceContextContents,
    request: &DeviceContextContentsRequest,
) -> Result<wire::NativeDialogueContextContents, Error> {
    value.validate_for(request).map_err(|_| BAD)?;
    Ok(wire::NativeDialogueContextContents {
        chunks: value.chunks.iter().map(chunk).collect::<Result<_, _>>()?,
    })
}
pub fn encode_contents(
    value: &DeviceContextContents,
    request: &DeviceContextContentsRequest,
) -> Result<Vec<u8>, Error> {
    bounded(
        wire::ReadNativeDialogueContextContentsResponse {
            contents: Some(contents(value, request)?),
        },
        DEVICE_CONTEXT_CONTENTS_LIMIT,
    )
}
pub fn contents_from_bytes(
    bytes: &[u8],
    request: &DeviceContextContentsRequest,
) -> Result<DeviceContextContents, Error> {
    let response: wire::ReadNativeDialogueContextContentsResponse =
        canonical(bytes, DEVICE_CONTEXT_CONTENTS_LIMIT)?;
    let value = required(response.contents)?;
    if value.chunks.len() != request.locators.len() {
        return Err(BAD);
    }
    let chunks = value
        .chunks
        .into_iter()
        .zip(&request.locators)
        .map(|(value, locator)| decode_chunk(value, &request.content_request(*locator)))
        .collect::<Result<_, _>>()?;
    let result = DeviceContextContents { chunks };
    result.validate_for(request).map_err(|_| BAD)?;
    Ok(result)
}
