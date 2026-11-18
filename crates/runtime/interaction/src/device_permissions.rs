//! Host-authored OS prerequisites. Confirmation resumes the original invocation;
//! it never grants an OS permission or authorizes another device.
use crate::{StructuredInteractionError, StructuredInteractionRequest};
use musteragent_content_ref::SchemaRef;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DevicePermission {
    Accessibility,
    ScreenRecording,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevicePermissions {
    schema: String,
    // Older product hosts lack a device binding; they get no local grant action.
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<Uuid>,
    required: Vec<DevicePermission>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevicePermissionPrompt {
    kind: String,
    title: String,
    description: String,
    required: bool,
    allow_other: bool,
    device_permissions: DevicePermissions,
}
impl DevicePermissionPrompt {
    pub fn new(
        device_id: Option<Uuid>,
        required: Vec<DevicePermission>,
    ) -> Result<Self, StructuredInteractionError> {
        if device_id.is_some_and(|id| id.is_nil())
            || required.is_empty()
            || required.len() > 2
            || (required.len() == 2 && required[0] == required[1])
        {
            return Err(StructuredInteractionError::InvalidRequest);
        }
        Ok(Self {
            kind: "confirm".into(), title: "电脑操作需要授权".into(),
            description: "请授权下面列出的系统权限，完成后继续本次操作。若系统要求重启，请重启应用后回到此对话。".into(),
            required: true, allow_other: false,
            device_permissions: DevicePermissions { schema: "device.os-permissions@1".into(), device_id, required },
        })
    }
    pub fn canonical_prompt_bytes(&self) -> Result<Vec<u8>, StructuredInteractionError> {
        serde_json::to_vec(self).map_err(|_| StructuredInteractionError::InvalidRequest)
    }
    pub const fn interaction_kind(&self) -> musteragent_approval::InteractionKind {
        musteragent_approval::InteractionKind::Confirmation
    }
    pub fn response_schema(&self) -> Result<SchemaRef, StructuredInteractionError> {
        StructuredInteractionRequest::parse(
            br#"{"kind":"confirm","title":"Permission","required":true,"allow_other":false}"#,
        )?
        .response_schema()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn device_permission_prompt_is_bounded_and_uses_original_confirmation() {
        assert!(
            DevicePermissionPrompt::new(Some(Uuid::nil()), vec![DevicePermission::Accessibility])
                .is_err()
        );
        assert!(DevicePermissionPrompt::new(None, vec![]).is_err());
        assert!(
            DevicePermissionPrompt::new(None, vec![DevicePermission::Accessibility; 2]).is_err()
        );
        let prompt = DevicePermissionPrompt::new(
            Some(Uuid::from_u128(1)),
            vec![DevicePermission::ScreenRecording],
        )
        .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&prompt.canonical_prompt_bytes().unwrap()).unwrap();
        assert_eq!(
            value["device_permissions"]["required"],
            serde_json::json!(["screen_recording"])
        );
        assert_eq!(
            prompt.interaction_kind(),
            musteragent_approval::InteractionKind::Confirmation
        );
        assert!(prompt.response_schema().is_ok());
    }
}
