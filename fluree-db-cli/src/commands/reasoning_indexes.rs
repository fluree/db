//! `fluree reasoning-indexes`: register a reasoning program for persistence on a
//! branch, and list, refresh or retire its indexes.

use crate::cli::ReasoningIndexesAction;
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::reasoning_index::ReasoningIndexId;
use serde_json::{json, Value as JsonValue};

fn program_of(reasoning: Option<String>, program: Option<String>) -> CliResult<JsonValue> {
    match (reasoning, program) {
        (_, Some(program)) => serde_json::from_str(&program)
            .map_err(|e| CliError::Usage(format!("--program is not JSON: {e}"))),
        (Some(reasoning), None) => Ok(json!({"reasoning": reasoning})),
        (None, None) => Err(CliError::Usage(
            "name the program with --reasoning <modes> or --program <json>".into(),
        )),
    }
}

fn index_id(raw: &str) -> CliResult<ReasoningIndexId> {
    ReasoningIndexId::parse(raw).map_err(CliError::Usage)
}

fn print(value: &JsonValue) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

pub async fn run(action: ReasoningIndexesAction, dirs: &FlureeDir) -> CliResult<()> {
    let (ledger, remote) = match &action {
        ReasoningIndexesAction::Register { ledger, remote, .. }
        | ReasoningIndexesAction::List { ledger, remote }
        | ReasoningIndexesAction::Refresh { ledger, remote, .. }
        | ReasoningIndexesAction::Retire { ledger, remote, .. } => (ledger.clone(), remote.clone()),
    };
    let (op, body) = match &action {
        ReasoningIndexesAction::Register {
            reasoning, program, ..
        } => (
            "register",
            json!({"program": program_of(reasoning.clone(), program.clone())?}),
        ),
        ReasoningIndexesAction::List { .. } => ("list", json!({})),
        ReasoningIndexesAction::Refresh { index, .. } => ("refresh", json!({"indexId": index})),
        ReasoningIndexesAction::Retire { index, .. } => ("retire", json!({"indexId": index})),
    };

    if let Some(remote_name) = remote.as_deref() {
        let alias = context::resolve_ledger(ledger.as_deref(), dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        print(&client.reasoning_indexes(op, &alias, body).await?);
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return Ok(());
    }
    match context::resolve_ledger_mode(ledger.as_deref(), dirs).await? {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            print(&client.reasoning_indexes(op, &remote_alias, body).await?);
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
        }
        LedgerMode::Local { fluree, alias } => {
            let out = match action {
                ReasoningIndexesAction::Register {
                    reasoning, program, ..
                } => {
                    let index = fluree
                        .register_reasoning_index(&alias, &program_of(reasoning, program)?)
                        .await?;
                    json!({"ledger": alias, "indexId": index.to_string()})
                }
                ReasoningIndexesAction::List { .. } => {
                    serde_json::to_value(fluree.reasoning_indexes(&alias).await?)
                        .unwrap_or(JsonValue::Null)
                }
                ReasoningIndexesAction::Refresh { index, .. } => {
                    let results = match index {
                        Some(raw) => {
                            let index = index_id(&raw)?;
                            let outcome = fluree.refresh_reasoning_index(&alias, &index).await?;
                            vec![(index, outcome)]
                        }
                        None => fluree.refresh_reasoning_indexes(&alias).await?,
                    };
                    json!({
                        "ledger": alias,
                        "results": results
                            .into_iter()
                            .map(|(index, outcome)| {
                                let mut entry = serde_json::to_value(outcome)
                                    .unwrap_or(JsonValue::Null);
                                entry["indexId"] = json!(index.to_string());
                                entry
                            })
                            .collect::<Vec<_>>()
                    })
                }
                ReasoningIndexesAction::Retire { index, .. } => {
                    let index = index_id(&index)?;
                    fluree.retire_reasoning_index(&alias, &index).await?;
                    json!({"ledger": alias, "indexId": index.to_string()})
                }
            };
            print(&out);
        }
    }
    Ok(())
}
