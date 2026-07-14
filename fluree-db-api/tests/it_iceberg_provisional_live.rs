//! Live, secret-gated regression for the provisional-R2RML query lane
//! (WP-DB3: `query_provisional_r2rml`).
//!
//! Exercises the SAME public entry the LLM agent's mapping-validation tool calls:
//! it compiles a candidate R2RML mapping in memory and runs a SPARQL probe
//! against the live Snowflake-managed Iceberg catalog, creating NO persisted
//! graph source. It reuses the sampler live test's connection + PAT handling.
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
//!   --test it_iceberg_provisional_live -- --ignored --nocapture
//! ```
//! Connection defaults target `ENTERPRISE_DEMO.DW`; the probed table is
//! `DW.DIM_CUSTOMER` unless overridden via `ICEBERG_NAMESPACE` / `ICEBERG_TABLE`.
//! `ICEBERG_CATALOG_URI` is REQUIRED (no account default is committed; the test
//! skips without it). Other connection fields are overridable via env
//! (`ICEBERG_WAREHOUSE`, `ICEBERG_OAUTH2_TOKEN_URL`, `ICEBERG_OAUTH2_SCOPE`,
//! `ICEBERG_OAUTH2_CLIENT_ID`, `ICEBERG_OAUTH2_CLIENT_SECRET`).

use fluree_db_api::{FlureeBuilder, IcebergConnectionConfig};

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

/// A minimal one-TriplesMap R2RML mapping (Turtle) over `namespace.table`, keyed
/// on `key_column`, declaring `class_iri`. This is what the agent renders from a
/// candidate IR — never persisted.
fn provisional_ttl(namespace: &str, table: &str, key_column: &str, class_iri: &str) -> String {
    format!(
        r#"@base <http://mapping.fluree.dev/r2rml> .
@prefix rr: <http://www.w3.org/ns/r2rml#> .

<#TM> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "{namespace}.{table}" ] ;
  rr:subjectMap [
    rr:template "http://example.org/row/{{{key_column}}}" ;
    rr:class <{class_iri}>
  ] .
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Live Snowflake. Set FLUREE_ICEBERG_LIVE=1 + a PAT (ICEBERG_PAT_FILE / ICEBERG_OAUTH2_CLIENT_SECRET)."]
async fn query_provisional_r2rml_live_snowflake() {
    if std::env::var("FLUREE_ICEBERG_LIVE").ok().as_deref() != Some("1") {
        eprintln!("SKIP: set FLUREE_ICEBERG_LIVE=1 to run the live provisional-query test");
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
    let key_column = env_or("ICEBERG_KEY_COLUMN", "CUSTOMER_KEY");
    let class_iri = "http://example.org/WebEvent";

    let fluree = FlureeBuilder::memory().build_memory();

    // -- Matching class probe: `?e a <WebEvent> LIMIT 5` returns rows -----------
    let ttl = provisional_ttl(&namespace, &table_name, &key_column, class_iri);
    let matching = format!("SELECT ?e WHERE {{ ?e a <{class_iri}> }} LIMIT 5");
    let result = fluree
        .query_provisional_r2rml(conn.clone(), ttl.clone(), matching)
        .await
        .expect("provisional query against live Snowflake must succeed");
    let rows: usize = result.batches.iter().fold(0, |acc, b| acc + b.len());
    eprintln!("provisional probe bound {rows} row(s) from {namespace}.{table_name} (limit 5)");
    assert!(rows <= 5, "LIMIT 5 must bound the probe");
    assert!(
        rows > 0,
        "a non-empty table must bind at least one subject for the mapped class"
    );

    // -- Non-matching class probe: a class absent from the mapping → empty ------
    let nonmatch = "SELECT ?e WHERE { ?e a <http://example.org/DefinitelyAbsentClass> } LIMIT 5";
    let result = fluree
        .query_provisional_r2rml(conn.clone(), ttl, nonmatch.to_string())
        .await
        .expect("a non-matching class is an empty result, not an error");
    let rows: usize = result.batches.iter().fold(0, |acc, b| acc + b.len());
    assert_eq!(rows, 0, "a class not in the mapping must return no rows");
}
