//! Live, secret-gated integration test for multi-table `generate_r2rml`.
//!
//! This exercises the SAME engine entry point solo's "Generate Mapping" calls
//! (`fluree_db_api::Fluree::generate_r2rml`, via solo's
//! `handle_iceberg_generate_r2rml`) against the live Snowflake-managed Iceberg
//! catalog. It reproduces the `UpstreamError` solo surfaces by capturing the
//! REAL engine error, and — once fixed — asserts a sane multi-table mapping.
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
//!   --test it_iceberg_generate_live -- --ignored --nocapture
//! ```
//! Connection defaults target the `ENTERPRISE_DEMO.DW` 16-table star schema.
//! `ICEBERG_CATALOG_URI` is REQUIRED (no account default is committed; the test
//! skips without it). Other fields are overridable via env (`ICEBERG_WAREHOUSE`,
//! `ICEBERG_OAUTH2_TOKEN_URL`, `ICEBERG_OAUTH2_SCOPE`, `ICEBERG_OAUTH2_CLIENT_ID`,
//! `ICEBERG_OAUTH2_CLIENT_SECRET`).

use fluree_db_api::{
    FlureeBuilder, GenerateOptions, GenerateR2rmlRequest, GenerateR2rmlResponse,
    IcebergConnectionConfig, TableIdentifier,
};
use std::collections::HashMap;

/// The `ENTERPRISE_DEMO.DW` star schema: 8 dimensions + 8 facts.
const DIMENSIONS: &[&str] = &[
    "DIM_ACCOUNT",
    "DIM_CUSTOMER",
    "DIM_DATE",
    "DIM_EMPLOYEE",
    "DIM_GEOGRAPHY",
    "DIM_PRODUCT",
    "DIM_STORE",
    "DIM_SUPPLIER",
];
const FACTS: &[&str] = &[
    "FACT_ORDER",
    "FACT_ORDER_LINE",
    "FACT_PAYMENT",
    "FACT_SHIPMENT",
    "FACT_INVENTORY_SNAPSHOT",
    "FACT_GL_JOURNAL",
    "FACT_SUPPORT_TICKET",
    "FACT_WEB_EVENT",
];

const NAMESPACE: &str = "DW";
const BASE_NAMESPACE: &str = "http://example/org/ns";

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

fn table_ids(names: &[&str]) -> Vec<TableIdentifier> {
    names
        .iter()
        .map(|n| TableIdentifier::new(NAMESPACE, *n))
        .collect()
}

async fn run_generate(
    fluree: &fluree_db_api::Fluree,
    conn: &IcebergConnectionConfig,
    tables: Vec<TableIdentifier>,
) -> fluree_db_api::Result<GenerateR2rmlResponse> {
    let req = GenerateR2rmlRequest {
        connection: conn.clone(),
        tables,
        base_namespace: BASE_NAMESPACE.to_string(),
        per_table_overrides: HashMap::new(),
        options: GenerateOptions::default(),
        target_model_ledger_id: None,
    };
    fluree.generate_r2rml(req).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Live Snowflake. Set FLUREE_ICEBERG_LIVE=1 + a PAT (ICEBERG_PAT_FILE / ICEBERG_OAUTH2_CLIENT_SECRET)."]
async fn generate_r2rml_multitable_live_snowflake() {
    if std::env::var("FLUREE_ICEBERG_LIVE").ok().as_deref() != Some("1") {
        eprintln!("SKIP: set FLUREE_ICEBERG_LIVE=1 to run the live generate_r2rml test");
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

    let fluree = FlureeBuilder::memory().build_memory();
    let conn = build_connection(&pat, catalog_uri);

    let all: Vec<&str> = DIMENSIONS.iter().chain(FACTS.iter()).copied().collect();

    // -- Per-table bisection: which table(s), if any, fail on their own? --
    eprintln!("\n=== per-table single-table generate_r2rml ===");
    let mut per_table_failures = Vec::new();
    for name in &all {
        match run_generate(&fluree, &conn, table_ids(&[name])).await {
            Ok(resp) => {
                let tm = &resp.structured.table_mappings[0];
                let joins = tm
                    .columns
                    .iter()
                    .filter(|c| c.foreign_key.is_some())
                    .count();
                eprintln!(
                    "  OK   {NAMESPACE}.{name:<24} cols={:>3} joins={joins} subj={}",
                    tm.columns.len(),
                    if tm.subject_template.is_empty() {
                        "<none>"
                    } else {
                        "<set>"
                    }
                );
            }
            Err(e) => {
                eprintln!("  FAIL {NAMESPACE}.{name:<24} {e}");
                per_table_failures.push((name.to_string(), e.to_string()));
            }
        }
    }

    // -- The multi-table generate solo actually invokes (all 16 together). The
    //    wall time is the load-bearing signal: with the sequential fetch it ran
    //    ~47s (measured; over solo's 30s/60s synchronous ceilings → UpstreamError);
    //    the bounded-concurrency fetch brings it to ~10s, well under the ceiling. --
    eprintln!("\n=== multi-table generate_r2rml (all 16) ===");
    let t0 = std::time::Instant::now();
    let multi = run_generate(&fluree, &conn, table_ids(&all)).await;
    let multi_elapsed = t0.elapsed();
    match &multi {
        Ok(resp) => eprintln!(
            "  OK   {} table mappings, {} diagnostics in {:.1}s",
            resp.structured.table_mappings.len(),
            resp.diagnostics.len(),
            multi_elapsed.as_secs_f64(),
        ),
        Err(e) => eprintln!(
            "  FAIL multi-table in {:.1}s: {e}",
            multi_elapsed.as_secs_f64()
        ),
    }

    // -- A dimensions-only probe (narrows FACT involvement). --
    eprintln!("\n=== dimensions-only (8) ===");
    let t0 = std::time::Instant::now();
    match run_generate(&fluree, &conn, table_ids(DIMENSIONS)).await {
        Ok(resp) => eprintln!(
            "  OK   {} mappings in {:.1}s",
            resp.structured.table_mappings.len(),
            t0.elapsed().as_secs_f64(),
        ),
        Err(e) => eprintln!("  FAIL dims-only: {e}"),
    }

    if !per_table_failures.is_empty() {
        eprintln!("\nPer-table failures:");
        for (t, e) in &per_table_failures {
            eprintln!("  {t}: {e}");
        }
    }

    // -- Assertions the FIX must satisfy. --
    let resp = multi.expect("multi-table generate_r2rml must succeed against live Snowflake");
    assert_eq!(
        resp.structured.table_mappings.len(),
        all.len(),
        "one mapping per requested table"
    );
    // Regression guard: the bounded-concurrency fetch must keep a 16-table
    // generate well under solo's synchronous ceiling. Sequential fetching ran
    // ~80s; the parallel fetch clears the same schema in ~10-20s, so a generous
    // 45s bound catches a silent regression back to sequential without flaking on
    // a merely-slow link.
    assert!(
        multi_elapsed.as_secs() < 45,
        "multi-table generate took {:.1}s — expected well under solo's ~30-60s ceiling; \
         did the per-table preview fetch regress to sequential?",
        multi_elapsed.as_secs_f64()
    );
    // A sane star-schema mapping resolves at least one FK join (facts → dims).
    let total_joins: usize = resp
        .structured
        .table_mappings
        .iter()
        .flat_map(|tm| &tm.columns)
        .filter(|c| c.foreign_key.is_some())
        .count();
    assert!(
        total_joins > 0,
        "expected at least one resolved FK join across the star schema"
    );
    // The rendered Turtle must be non-empty and carry every table.
    assert!(!resp.turtle.is_empty(), "turtle must render");
    for name in &all {
        assert!(
            resp.turtle.contains(&format!("{NAMESPACE}.{name}")),
            "turtle must mention {NAMESPACE}.{name}"
        );
    }
    eprintln!("\nturtle bytes: {}", resp.turtle.len());
}
