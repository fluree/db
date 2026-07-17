//! Live, secret-gated regression for the Iceberg bounded row/column sampler
//! (WP-DB2: `sample_iceberg_rows` / `sample_column_values`).
//!
//! Exercises the SAME public entry points the LLM data-peek tool and solo #756
//! (row preview) call, against the live Snowflake-managed Iceberg catalog. It
//! reuses `it_iceberg_generate_live`'s connection + PAT handling verbatim.
//!
//! **Skips cleanly** when the credential/opt-in are absent (never fails in CI):
//! it is `#[ignore]` (out of the default `cargo test` set) AND returns early
//! unless `FLUREE_ICEBERG_LIVE=1` and a PAT is resolvable.
//!
//! Run it:
//! ```bash
//! FLUREE_ICEBERG_LIVE=1 \
//!   ICEBERG_PAT_FILE=~/Downloads/bplatz-handoff/snowflake-pat.txt \
//!   cargo test -p fluree-db-api --features iceberg \
//!   --test it_iceberg_sample_live -- --ignored --nocapture
//! ```
//! Connection defaults target `ENTERPRISE_DEMO.DW`; the sampled table is
//! `DW.DIM_CUSTOMER` unless overridden via `ICEBERG_NAMESPACE` / `ICEBERG_TABLE`.
//! `ICEBERG_CATALOG_URI` is REQUIRED (no account default is committed; the test
//! skips without it). Other connection fields are overridable via env
//! (`ICEBERG_WAREHOUSE`, `ICEBERG_OAUTH2_TOKEN_URL`, `ICEBERG_OAUTH2_SCOPE`,
//! `ICEBERG_OAUTH2_CLIENT_ID`, `ICEBERG_OAUTH2_CLIENT_SECRET`).

use fluree_db_api::{
    sample_column_values, sample_iceberg_rows, IcebergConnectionConfig, TableIdentifier,
};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Resolve the PAT (OAuth2 client_secret) from env or a file, trimming
/// whitespace. Returns `None` when no source is configured — the skip signal.
fn resolve_pat() -> Option<String> {
    if let Ok(secret) = std::env::var("ICEBERG_OAUTH2_CLIENT_SECRET") {
        let secret = secret.trim().to_string();
        if !secret.is_empty() {
            return Some(secret);
        }
    }
    let candidates = std::env::var("ICEBERG_PAT_FILE").ok().into_iter().chain(
        std::env::var("HOME")
            .ok()
            .map(|h| format!("{h}/Downloads/bplatz-handoff/snowflake-pat.txt")),
    );
    for path in candidates {
        if let Ok(contents) = std::fs::read_to_string(&path) {
            let secret = contents.trim().to_string();
            if !secret.is_empty() {
                return Some(secret);
            }
        }
    }
    None
}

/// Resolve the REST catalog URI from `ICEBERG_CATALOG_URI`. REQUIRED: no
/// account default is committed to the repo — `None` is the skip signal.
fn resolve_catalog_uri() -> Option<String> {
    std::env::var("ICEBERG_CATALOG_URI")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Build the connection EXACTLY as solo's `build_iceberg_connection` does:
/// REST + OAuth2 client-credentials with an EMPTY `client_id`, a `session:role:`
/// scope, and the warehouse.
fn build_connection(pat: &str, catalog_uri: String) -> IcebergConnectionConfig {
    let token_url = env_or(
        "ICEBERG_OAUTH2_TOKEN_URL",
        &format!("{catalog_uri}/v1/oauth/tokens"),
    );
    let scope = env_or("ICEBERG_OAUTH2_SCOPE", "session:role:ICEBERG_READER");
    let client_id = env_or("ICEBERG_OAUTH2_CLIENT_ID", "");
    let warehouse = env_or("ICEBERG_WAREHOUSE", "ENTERPRISE_DEMO");

    IcebergConnectionConfig::rest(catalog_uri)
        .with_auth_oauth2(token_url, client_id, pat)
        .with_oauth2_scope(scope)
        .with_warehouse(warehouse)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Live Snowflake. Set FLUREE_ICEBERG_LIVE=1 + a PAT (ICEBERG_PAT_FILE / ICEBERG_OAUTH2_CLIENT_SECRET)."]
async fn sample_rows_and_column_live_snowflake() {
    if std::env::var("FLUREE_ICEBERG_LIVE").ok().as_deref() != Some("1") {
        eprintln!("SKIP: set FLUREE_ICEBERG_LIVE=1 to run the live sampler test");
        return;
    }
    let Some(pat) = resolve_pat() else {
        eprintln!("SKIP: no PAT resolvable (set ICEBERG_OAUTH2_CLIENT_SECRET or ICEBERG_PAT_FILE)");
        return;
    };
    let Some(catalog_uri) = resolve_catalog_uri() else {
        eprintln!("SKIP: set ICEBERG_CATALOG_URI (no account default is committed)");
        return;
    };

    let conn = build_connection(&pat, catalog_uri);
    let namespace = env_or("ICEBERG_NAMESPACE", "DW");
    let table_name = env_or("ICEBERG_TABLE", "DIM_CUSTOMER");
    let table = TableIdentifier::new(&namespace, &table_name);

    const N: usize = 5;

    // -- Row sample (all columns) -------------------------------------------
    let rows = sample_iceberg_rows(conn.clone(), table.clone(), None, N)
        .await
        .expect("sample_iceberg_rows must succeed against live Snowflake");
    eprintln!(
        "sampled {} row(s) from {namespace}.{table_name} (n={N})",
        rows.len()
    );
    assert!(rows.len() <= N, "row sample must be bounded by n");
    for row in &rows {
        assert!(row.is_object(), "each sampled row is a JSON object");
    }

    // -- Projected row sample (first two column names) ----------------------
    if let Some(first) = rows.first().and_then(|r| r.as_object()) {
        let cols: Vec<String> = first.keys().take(2).cloned().collect();
        let projected = sample_iceberg_rows(conn.clone(), table.clone(), Some(cols.clone()), N)
            .await
            .expect("projected sample_iceberg_rows must succeed");
        for row in &projected {
            let obj = row.as_object().expect("projected row is an object");
            assert!(
                obj.len() <= cols.len(),
                "projected row exposes only the requested columns"
            );
        }

        // -- Single-column sample: same row count as the all-columns sample
        //    (both read the first row group of the same first data file). --
        let col = cols[0].clone();
        let values = sample_column_values(conn.clone(), table.clone(), col.clone(), N)
            .await
            .expect("sample_column_values must succeed");
        eprintln!("sampled {} value(s) of column {col}", values.len());
        assert!(values.len() <= N, "column sample must be bounded by n");
        assert_eq!(
            values.len(),
            rows.len(),
            "column sample and row sample read the same first row group"
        );
    } else {
        eprintln!("NOTE: {namespace}.{table_name} returned no rows (empty first row group)");
    }
}
