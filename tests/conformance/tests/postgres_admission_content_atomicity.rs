#[path = "support/postgres_admission_content_atomicity.rs"]
mod support;

#[tokio::test]
async fn protected_locator_restart_compares_semantics_not_randomized_ciphertext() {
    support::semantic_restart_and_rotation_are_verified().await;
}

#[tokio::test]
async fn protected_locator_and_blob_drift_fail_closed() {
    support::hostile_protection_and_material_drift_fail_closed().await;
}
