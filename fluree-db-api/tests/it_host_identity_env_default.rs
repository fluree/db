//! An instance built without `with_host_identity_policy` takes the operator's
//! host-identity policy from the environment
//! (`FLUREE_GRAPH_SOURCE_HOST_IDENTITIES`), so an embedder that never sets one
//! cannot widen what the operator allowed.
//!
//! Process shape: the test re-executes itself (`current_exe`) in "child" mode
//! with the environment under test. A separate process is mandatory: the
//! policy is read from the environment once per process
//! (`HostIdentityPolicy::from_env`), so setting the variable in this process
//! would leak into, or be shadowed by, every other test in the binary. The
//! child writes what it observed to a result file, and a missing file fails
//! the test, so a child that silently did nothing cannot pass.
//!
//! A standalone `[[test]]` target for that isolation reason.

#![cfg(all(unix, feature = "iceberg"))]

use fluree_db_api::{CatalogGrant, FlureeBuilder, IcebergCreateConfig};

const CHILD_ENV: &str = "FLUREE_HOST_IDENTITY_DEFAULT_CHILD";
const RESULT_ENV: &str = "FLUREE_HOST_IDENTITY_DEFAULT_RESULT";
const HOST_IDENTITIES_ENV: &str = "FLUREE_GRAPH_SOURCE_HOST_IDENTITIES";
const AMBIENT_BUCKETS_ENV: &str = "FLUREE_GRAPH_SOURCE_AMBIENT_BUCKETS";

/// In the child: decide a Direct source's storage on the host's ambient AWS
/// identity, on an instance that never set a policy, and record the outcome
/// (`allowed`, or the refusal's wire code and status).
async fn child_main() {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut config = IcebergCreateConfig::new_direct("gs-ambient", "s3://lake/wh/t");
    config.connection = config.connection.with_s3_region("us-east-1");
    let outcome = match fluree
        .graph_source_storage(&config.to_iceberg_gs_config(), &CatalogGrant::direct())
        .await
    {
        Ok(_) => "allowed".to_string(),
        Err(refusal) => format!("{} {}", refusal.error_type(), refusal.status_code()),
    };
    let result = std::env::var(RESULT_ENV).expect("result path");
    std::fs::write(result, outcome).expect("write the result");
}

/// Run the child with `HOST_IDENTITIES_ENV` set to `value` (or unset), and
/// return what it recorded.
fn child_outcome(value: Option<&str>) -> String {
    let dir = tempfile::tempdir().expect("result dir");
    let result = dir.path().join("outcome");
    let mut cmd = std::process::Command::new(std::env::current_exe().expect("current exe"));
    cmd.args([
        "a_builder_takes_the_host_identity_policy_from_the_environment",
        "--exact",
        "--nocapture",
    ])
    .env(CHILD_ENV, "1")
    .env(RESULT_ENV, &result)
    // Determinism: the invoking environment's policy never reaches the child.
    .env_remove(AMBIENT_BUCKETS_ENV)
    .env_remove(HOST_IDENTITIES_ENV);
    if let Some(value) = value {
        cmd.env(HOST_IDENTITIES_ENV, value);
    }
    let output = cmd.output().expect("spawn the child");
    assert!(
        output.status.success(),
        "child failed ({HOST_IDENTITIES_ENV}={value:?}).\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    std::fs::read_to_string(&result).expect("the child must record an outcome")
}

#[tokio::test]
async fn a_builder_takes_the_host_identity_policy_from_the_environment() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_main().await;
        return;
    }
    // The operator refuses every host identity: an instance that never set
    // a policy refuses ambient AWS.
    assert_eq!(
        child_outcome(Some("none")),
        format!("{} 403", fluree_vocab::errors::HOST_IDENTITY_REFUSED)
    );
    // The same call with the variable unset allows it (every host identity),
    // so the refusal above is the environment's doing.
    assert_eq!(child_outcome(None), "allowed");
}
