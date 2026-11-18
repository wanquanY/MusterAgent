fn main() {
    // `sqlx::migrate!` embeds the migration set in the compiled crate. Cargo
    // does not otherwise guarantee that adding a new file to the directory
    // invalidates every dependent development binary, which can leave a new
    // database reporting `UnknownVersion` to a stale API/worker executable.
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=../../../migration-lock.toml");
}
