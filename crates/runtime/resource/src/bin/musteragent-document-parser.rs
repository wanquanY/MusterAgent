//! Standalone entrypoint for parser isolation qualification.
fn main() {
    std::process::exit(
        musteragent_runtime_resource::document_parser_process_entrypoint().unwrap_or(64),
    );
}
