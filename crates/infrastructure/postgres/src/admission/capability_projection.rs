//! The release ceiling encloses both signed origin-device placement and the
//! ordinary runtime/discovery projection. Neither branch may bypass retirement.
use musteragent_identity::ProductReleaseRef;
use musteragent_runtime_session::{ProductSnapshotRuntime, capability_available_on_runtime};

pub(super) fn allowed(
    release: Option<&ProductReleaseRef>,
    key: &str,
    runtime: ProductSnapshotRuntime,
    bound_device: bool,
    discovery: impl FnOnce() -> bool,
) -> bool {
    let retired = release.is_some_and(|release| {
        musteragent_runtime_snapshot::video_tool_retired_for_release(release, key)
    });
    !retired && (bound_device || capability_available_on_runtime(runtime, key) && discovery())
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::ProductId;

    #[test]
    fn current_release_retirement_encloses_both_projection_branches() {
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        for key in [
            "video.scene.preview@1",
            "video.scene.render@1",
            "video.scene.render@2",
            "video.narrate@1",
            "device.workspace.video.render@1",
            "device.workspace.video.render@99",
        ] {
            for runtime in [
                ProductSnapshotRuntime::Cloud,
                ProductSnapshotRuntime::Device,
            ] {
                // Keep the origin branch covered even though today's native
                // executor also omits these retired renderers from its catalog.
                for (bound_device, discovery) in [(true, false), (false, true), (true, true)] {
                    assert!(allowed(
                        None,
                        key,
                        ProductSnapshotRuntime::Device,
                        bound_device,
                        || discovery
                    ));
                    assert!(
                        !allowed(Some(&release), key, runtime, bound_device, || discovery),
                        "retirement bypass: {key} {runtime:?} bound={bound_device} discovery={discovery}"
                    );
                }
            }
        }
    }

    #[test]
    fn active_capabilities_retain_runtime_and_discovery_restrictions() {
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let key = "device.workspace.file.read@5";
        assert!(allowed(
            Some(&release),
            key,
            ProductSnapshotRuntime::Cloud,
            true,
            || false
        ));
        assert!(!allowed(
            Some(&release),
            key,
            ProductSnapshotRuntime::Cloud,
            false,
            || true
        ));
        assert!(allowed(
            Some(&release),
            key,
            ProductSnapshotRuntime::Device,
            false,
            || true
        ));
        assert!(!allowed(
            Some(&release),
            key,
            ProductSnapshotRuntime::Device,
            false,
            || false
        ));
        assert!(allowed(
            Some(&release),
            "system.time.current@1",
            ProductSnapshotRuntime::Cloud,
            false,
            || true
        ));
        assert!(!allowed(
            Some(&release),
            "system.time.current@1",
            ProductSnapshotRuntime::Cloud,
            false,
            || false
        ));
    }
}
