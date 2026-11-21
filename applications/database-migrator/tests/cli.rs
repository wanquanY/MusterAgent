use musteragent_testkit::IsolatedPostgres;
use tokio::process::Command;
#[tokio::test]
async fn real_migrator_initializes_and_verifies_only_the_kernel_chain() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("migrator process fixture");
    let run = |mode| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_musteragent-database-migrator"));
        cmd.arg(mode)
            .env("MUSTERAGENT_DATABASE_URL", pg.connection_url());
        cmd
    };
    assert!(
        !run("verify")
            .output()
            .await
            .expect("migrator process fixture")
            .status
            .success()
    );
    for mode in ["apply", "verify", "apply", "status"] {
        let result = run(mode).output().await.expect("migrator process fixture");
        assert!(
            result.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let pool = pg.pool().await.expect("migrator process fixture");
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("migrator process fixture");
    assert_eq!(versions, vec![1, 2]);
    assert!(
        !run("unknown")
            .output()
            .await
            .expect("migrator process fixture")
            .status
            .success()
    );
}
