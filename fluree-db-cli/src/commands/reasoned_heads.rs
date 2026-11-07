//! `fluree reasoned-heads`: register a reasoning program for persistence on a
//! branch, and list, refresh or retire its heads.

use crate::cli::ReasonedHeadsAction;
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::reasoned_head::HeadId;
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

fn head_id(raw: &str) -> CliResult<HeadId> {
    HeadId::parse(raw).map_err(CliError::Usage)
}

fn print(value: &JsonValue) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

pub async fn run(action: ReasonedHeadsAction, dirs: &FlureeDir) -> CliResult<()> {
    let (ledger, remote) = match &action {
        ReasonedHeadsAction::Register { ledger, remote, .. }
        | ReasonedHeadsAction::List { ledger, remote }
        | ReasonedHeadsAction::Refresh { ledger, remote, .. }
        | ReasonedHeadsAction::Retire { ledger, remote, .. } => (ledger.clone(), remote.clone()),
    };
    let (op, body) = match &action {
        ReasonedHeadsAction::Register {
            reasoning, program, ..
        } => (
            "register",
            json!({"program": program_of(reasoning.clone(), program.clone())?}),
        ),
        ReasonedHeadsAction::List { .. } => ("list", json!({})),
        ReasonedHeadsAction::Refresh { head, .. } => ("refresh", json!({"headId": head})),
        ReasonedHeadsAction::Retire { head, .. } => ("retire", json!({"headId": head})),
    };

    if let Some(remote_name) = remote.as_deref() {
        let alias = context::resolve_ledger(ledger.as_deref(), dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        print(&client.reasoned_heads(op, &alias, body).await?);
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
            print(&client.reasoned_heads(op, &remote_alias, body).await?);
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
        }
        LedgerMode::Local { fluree, alias } => {
            let out = match action {
                ReasonedHeadsAction::Register {
                    reasoning, program, ..
                } => {
                    let head = fluree
                        .register_reasoned_head(&alias, &program_of(reasoning, program)?)
                        .await?;
                    json!({"ledger": alias, "headId": head.to_string()})
                }
                ReasonedHeadsAction::List { .. } => {
                    serde_json::to_value(fluree.reasoned_heads(&alias).await?)
                        .unwrap_or(JsonValue::Null)
                }
                ReasonedHeadsAction::Refresh { head, .. } => {
                    let results = match head {
                        Some(raw) => {
                            let head = head_id(&raw)?;
                            let outcome = fluree.refresh_reasoned_head(&alias, &head).await?;
                            vec![(head, outcome)]
                        }
                        None => fluree.refresh_reasoned_heads(&alias).await?,
                    };
                    json!({
                        "ledger": alias,
                        "results": results
                            .into_iter()
                            .map(|(head, outcome)| {
                                let mut entry = serde_json::to_value(outcome)
                                    .unwrap_or(JsonValue::Null);
                                entry["headId"] = json!(head.to_string());
                                entry
                            })
                            .collect::<Vec<_>>()
                    })
                }
                ReasonedHeadsAction::Retire { head, .. } => {
                    let head = head_id(&head)?;
                    fluree.retire_reasoned_head(&alias, &head).await?;
                    json!({"ledger": alias, "headId": head.to_string()})
                }
            };
            print(&out);
        }
    }
    Ok(())
}
