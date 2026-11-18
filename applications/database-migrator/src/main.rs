//! Apply or verify only the kernel baseline and its append-only migrations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "verify".into());
    if !matches!(command.as_str(), "apply" | "verify" | "status") {
        return Err("expected apply, verify or status".into());
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("MUSTERAGENT_DATABASE_URL")?)
        .await?;
    let result = async {
        if command == "apply" {
            musteragent_postgres::migrate(&pool).await?;
        }
        musteragent_postgres::verify_migrations(&pool).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    pool.close().await;
    result?;
    println!("kernel migrations verified");
    Ok(())
}
