fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    // The persistence crate is the unique Rust owner of every shared message.
    // Enumerate declarations so additive shared fields/types cannot be duplicated.
    for entry in std::fs::read_dir("../persistence-codec/proto")? {
        let path = entry?.path();
        let source = std::fs::read_to_string(path)?;
        for line in source.lines() {
            if let Some(declaration) = line
                .strip_prefix("message ")
                .or_else(|| line.strip_prefix("enum "))
            {
                let name = declaration
                    .split_whitespace()
                    .next()
                    .ok_or("missing declaration name")?;
                config.extern_path(
                    format!(".musteragent.v1.{name}"),
                    format!("::musteragent_persistence_codec::wire::{name}"),
                );
            }
        }
    }
    config.boxed(".musteragent.v1.AgentJournalCommand.operation.tool");
    config.boxed(".musteragent.v1.AgentJournalCommand.operation.vision");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.begin_supervised");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.complete_supervised");
    config.boxed(".musteragent.v1.AgentContextCommand.operation.commit_plan");
    config.boxed(".musteragent.v1.AgentContextCommand.operation.compaction_model");
    config.boxed(".musteragent.v1.AgentContextReply.result.material");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.latest_generation");
    config.boxed(
        ".musteragent.v1.AgentCompactionCommand.operation.latest_generation_with_sealed_estimate",
    );
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.latest_role_candidate");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.load");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.seal");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.dispatch");
    config.boxed(".musteragent.v1.AgentCompactionCommand.operation.complete");
    let mut files = std::fs::read_dir("proto")?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=../persistence-codec/proto");
    tonic_prost_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("wire-descriptor.bin"),
        )
        .compile_with_config(
            config,
            &files,
            &[
                std::path::PathBuf::from("proto"),
                std::path::PathBuf::from("../persistence-codec/proto"),
                protoc_bin_vendored::include_path()?,
            ],
        )?;
    Ok(())
}
