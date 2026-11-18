//! Original publication owner command, issued only by the registered native
//! Tool executor. The owner re-reads the immutable invocation and derives scope.

mod request;
pub use request::{
    ARTIFACTS_DELIVER_V2_TOOL, DialogueToolPublicationRequest, decode_dialogue_tool_publication,
};
mod device;
mod device_artifact;
mod device_artifact_codec;
mod device_artifact_content;
pub use device::{
    DeviceDeliveryBinding, DeviceToolDelivery, DeviceToolDeliveryReceipt,
    DeviceToolDeliveryTransport, DeviceToolPublicationStore, WorkItemDevicePublicationSubject,
};
pub use device_artifact::{
    DeviceArtifactDeliveryReceipt, DeviceArtifactExport, DeviceArtifactExportManifest,
    DeviceArtifactExportRole, DeviceArtifactExportScope, DeviceArtifactProducer,
    MAX_DEVICE_EXPORT_BYTES,
};
pub use device_artifact_content::{
    DeviceArtifactContent, DeviceArtifactContents, MAX_DEVICE_ARTIFACT_ROOT_BYTES,
    MAX_DEVICE_TXT_BYTES,
};

// One publication port is shared by the session owner and executor.
pub use musteragent_runtime_capability::{
    DialoguePublicationActivity, DialogueToolPublicationError, DialogueToolPublicationReceipt,
    FencedDialogueToolPublication,
};

mod device_content_stage;
pub use device_content_stage::*;

#[cfg(test)]
mod device_content_stage_tests;
