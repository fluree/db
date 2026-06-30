//! `fluree cluster` subcommand — bootstrap and manage a Raft cluster.
//!
//! Each action issues exactly one HTTP request against the Fluree
//! server's private `/cluster/*` admin endpoints. The endpoints
//! themselves carry no authentication; the operator is expected to
//! reach them over a VPC-internal network, SSH tunnel, or similar
//! private channel.
//!
//! See `docs/operations/raft-clusters.md` for the full bootstrap
//! recipe and operational notes.

use crate::cli::ClusterAction;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Suffix for the private cluster admin router. Matches the prefix
/// `fluree-db-server` nests `RaftAdmin::router` under.
const CLUSTER_PATH_PREFIX: &str = "/cluster";

/// Default HTTP timeout for cluster ops. `add-learner --blocking`
/// can run longer when the new peer is far behind; callers can set
/// `--timeout` on the top-level CLI to override.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

// ============================================================================
// Wire types — mirror the server's `RaftAdmin` JSON surface.
// ============================================================================

#[derive(Serialize)]
struct InitializeRequest {
    members: BTreeMap<u64, NodeAddrs>,
}

#[derive(Serialize)]
struct AddLearnerRequest {
    node_id: u64,
    #[serde(flatten)]
    addrs: NodeAddrs,
    blocking: bool,
}

#[derive(Serialize)]
struct ChangeMembershipRequest {
    members: Vec<u64>,
    retain: bool,
}

#[derive(Serialize)]
struct NodeAddrs {
    raft_addr: String,
    client_addr: String,
}

#[derive(Debug, Deserialize)]
struct ClusterStatus {
    current_leader: Option<u64>,
    current_term: u64,
    last_applied_index: Option<u64>,
    voters: Vec<u64>,
    learners: Vec<u64>,
}

// ============================================================================
// Dispatch
// ============================================================================

pub async fn run(action: ClusterAction) -> CliResult<()> {
    let client = reqwest::Client::builder()
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .build()
        .map_err(|e| CliError::Remote(format!("building HTTP client: {e}")))?;

    match action {
        ClusterAction::Init {
            addr,
            node_id,
            raft_url,
            client_url,
        } => init(&client, addr, node_id, raft_url, client_url).await,
        ClusterAction::Add {
            leader,
            node_id,
            raft_url,
            client_url,
            blocking,
        } => add(&client, leader, node_id, raft_url, client_url, blocking).await,
        ClusterAction::Promote {
            leader,
            members,
            retain,
        } => promote(&client, leader, members, retain).await,
        ClusterAction::Status { addr } => status(&client, addr).await,
    }
}

// ============================================================================
// Actions
// ============================================================================

async fn init(
    client: &reqwest::Client,
    addr: String,
    node_id: u64,
    raft_url: String,
    client_url: String,
) -> CliResult<()> {
    let body = InitializeRequest {
        members: BTreeMap::from([(
            node_id,
            NodeAddrs {
                raft_addr: raft_url,
                client_addr: client_url,
            },
        )]),
    };
    let resp = client
        .post(endpoint(&addr, "/initialize"))
        .json(&body)
        .send()
        .await
        .map_err(network_err)?;
    expect_no_content(resp, "initialize").await?;
    println!(
        "{} cluster initialized — node {} is the seed (auto-leader after election)",
        "✓".green().bold(),
        node_id
    );
    Ok(())
}

async fn add(
    client: &reqwest::Client,
    leader: String,
    node_id: u64,
    raft_url: String,
    client_url: String,
    blocking: bool,
) -> CliResult<()> {
    let body = AddLearnerRequest {
        node_id,
        addrs: NodeAddrs {
            raft_addr: raft_url,
            client_addr: client_url,
        },
        blocking,
    };
    let resp = client
        .post(endpoint(&leader, "/add-learner"))
        .json(&body)
        .send()
        .await
        .map_err(network_err)?;
    expect_no_content(resp, "add").await?;
    println!(
        "{} learner {} added{}",
        "✓".green().bold(),
        node_id,
        if blocking { " (caught up)" } else { "" }
    );
    Ok(())
}

async fn promote(
    client: &reqwest::Client,
    leader: String,
    members: Vec<u64>,
    retain: bool,
) -> CliResult<()> {
    if members.is_empty() {
        return Err(CliError::Usage(
            "--members must list at least one voter id".into(),
        ));
    }
    let body = ChangeMembershipRequest {
        members: members.clone(),
        retain,
    };
    let resp = client
        .post(endpoint(&leader, "/change-membership"))
        .json(&body)
        .send()
        .await
        .map_err(network_err)?;
    expect_no_content(resp, "promote").await?;
    let voter_list = members
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "{} voter set updated to [{}]{}",
        "✓".green().bold(),
        voter_list,
        if retain {
            " (dropped voters kept as learners)"
        } else {
            ""
        }
    );
    Ok(())
}

async fn status(client: &reqwest::Client, addr: String) -> CliResult<()> {
    let resp = client
        .get(endpoint(&addr, "/status"))
        .send()
        .await
        .map_err(network_err)?;
    let status = resp.status();
    if !status.is_success() {
        return Err(error_from_response(status, resp, "status").await);
    }
    let body: ClusterStatus = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("status: invalid JSON response: {e}")))?;
    print_status(&body);
    Ok(())
}

fn print_status(s: &ClusterStatus) {
    let leader = s
        .current_leader
        .map(|id| id.to_string())
        .unwrap_or_else(|| "(none)".into());
    let last_idx = s
        .last_applied_index
        .map(|i| i.to_string())
        .unwrap_or_else(|| "(none)".into());
    println!("{}", "Cluster status".bold());
    println!("  leader:        {leader}");
    println!("  term:          {}", s.current_term);
    println!("  last_applied:  {last_idx}");
    println!("  voters:        {}", format_id_list(&s.voters));
    println!("  learners:      {}", format_id_list(&s.learners));
}

fn format_id_list(ids: &[u64]) -> String {
    if ids.is_empty() {
        "(none)".into()
    } else {
        ids.iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ============================================================================
// HTTP helpers
// ============================================================================

fn endpoint(base: &str, path: &str) -> String {
    format!(
        "{}{}{}",
        base.trim_end_matches('/'),
        CLUSTER_PATH_PREFIX,
        path
    )
}

fn network_err(e: reqwest::Error) -> CliError {
    CliError::Remote(format!("cluster request failed: {e}"))
}

async fn expect_no_content(resp: reqwest::Response, op: &str) -> CliResult<()> {
    let status = resp.status();
    if status == StatusCode::NO_CONTENT || status.is_success() {
        Ok(())
    } else {
        Err(error_from_response(status, resp, op).await)
    }
}

async fn error_from_response(status: StatusCode, resp: reqwest::Response, op: &str) -> CliError {
    let body = resp.text().await.unwrap_or_default();
    let trimmed = body.trim();
    if trimmed.is_empty() {
        CliError::Remote(format!("{op}: HTTP {status}"))
    } else {
        CliError::Remote(format!("{op}: HTTP {status} — {trimmed}"))
    }
}
