use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "fluree-sql-bridge", version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8080", env = "BRIDGE_LISTEN")]
    listen: SocketAddr,

    /// Database URL: postgres://…, mysql://…, or sqlite://path.db
    #[arg(long, env = "DATABASE_URL")]
    database: String,

    /// Require this bearer token on every request.
    #[arg(long, env = "BRIDGE_TOKEN")]
    token: Option<String>,

    /// Connection pool size.
    #[arg(long, default_value_t = 8)]
    max_connections: u32,

    /// Rows per protocol page.
    #[arg(long, default_value_t = 5000)]
    page_rows: usize,

    /// Scale reported for NUMERIC/DECIMAL columns (`decimal(38, N)`); values
    /// with more fractional digits are rounded half-even.
    #[arg(long, default_value_t = 6)]
    decimal_scale: i64,

    /// Abandoned statements are dropped after this many seconds without a fetch.
    #[arg(long, default_value_t = 300)]
    idle_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let args = Args::parse();

    let backend = fluree_sql_bridge::connect_backend(
        &args.database,
        args.max_connections,
        args.decimal_scale,
    )
    .await?;
    info!(dialect = backend.dialect(), listen = %args.listen, "fluree-sql-bridge ready");

    let app = fluree_sql_bridge::App::new(
        backend,
        args.token,
        args.page_rows,
        Duration::from_secs(args.idle_secs),
    );
    app.spawn_reaper();

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .map_err(|e| format!("bind {}: {e}", args.listen))?;
    axum::serve(listener, app.router())
        .await
        .map_err(|e| e.to_string())
}
