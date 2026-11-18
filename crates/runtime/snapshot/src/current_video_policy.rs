//! Current release retirement applies across execution homes, independently of
//! whether a capability is still present in the compiled tool catalog.
use super::{DIALOGUE_DEVICE_DISCOVERY_POLICY, dialogue_device_discovery_policy_for_release};

pub(super) fn retired(logical_key: &str) -> bool {
    matches!(
        logical_key,
        "video.scene.preview"
            | "video.scene.render"
            | "video.narrate"
            | "device.workspace.video.render"
    )
}

/// The current release's retired renderers cannot regain authority through a
/// signed device entitlement or a different execution home.
pub fn video_tool_retired_for_release(
    release: &musteragent_identity::ProductReleaseRef,
    capability_key: &str,
) -> bool {
    capability_key
        .rsplit_once('@')
        .is_some_and(|(logical, _)| retired(logical))
        && dialogue_device_discovery_policy_for_release(release)
            == Some(DIALOGUE_DEVICE_DISCOVERY_POLICY)
}
