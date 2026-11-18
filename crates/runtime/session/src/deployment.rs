//! Physical deployment checks shared by admission and host composition.
/// Runtime location; sandbox executor selection remains the routed Tool authority.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProductSnapshotRuntime {
    Cloud,
    Device,
}
/// Whether an entitlement can be executed in this physical runtime.
pub fn capability_available_on_runtime(runtime: ProductSnapshotRuntime, key: &str) -> bool {
    match runtime {
        ProductSnapshotRuntime::Cloud => !key.starts_with("device."),
        ProductSnapshotRuntime::Device => {
            !key.starts_with("workspace.file.") && !key.starts_with("workspace.asset.")
        }
    }
}
