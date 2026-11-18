fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("storage-descriptor.bin"),
    );
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.boxed(".musteragent.v1.ExecutionLivenessRecordV1.state.checkpoint");
    config.boxed(".musteragent.v1.ExecutionLivenessRecordV1.state.recovering");
    config.boxed(".musteragent.v1.RunExecutionPolicySnapshot.execution_liveness_v1");
    let mut files = std::fs::read_dir("proto")?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    config.compile_protos(
        &files,
        &[
            std::path::PathBuf::from("proto"),
            protoc_bin_vendored::include_path()?,
        ],
    )?;
    Ok(())
}
