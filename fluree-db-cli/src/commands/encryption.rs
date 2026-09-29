//! `fluree encryption` — held keys and key rotation.
//!
//! Every action runs either against a remote server (`--remote`) or
//! directly against the storage a connection config describes
//! (`--connection-config`). The two share one small interface so the
//! output is the same either way.

use crate::cli::{EncryptionAction, EncryptionTarget};
use crate::error::{CliError, CliResult};
use crate::{config, context};
use colored::Colorize;
use fluree_db_api::key_rotation::{KeyRotationOptions, KeyRotationState};
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Where the commands run.
enum Target {
    Remote {
        client: Box<crate::remote_client::RemoteLedgerClient>,
        name: String,
        dirs: fluree_db_api::server_defaults::FlureeDir,
    },
    Local(Box<Fluree>),
}

impl Target {
    async fn resolve(target: &EncryptionTarget, config_path: Option<&Path>) -> CliResult<Self> {
        if let Some(name) = &target.remote {
            let dirs = config::require_fluree_dir(config_path)?;
            let client = context::build_remote_client(name, &dirs).await?;
            return Ok(Self::Remote {
                client: Box::new(client),
                name: name.clone(),
                dirs,
            });
        }
        if let Some(path) = &target.connection_config {
            let text = std::fs::read_to_string(path)
                .map_err(|e| CliError::Config(format!("cannot read {}: {e}", path.display())))?;
            let json: Value = serde_json::from_str(&text)
                .map_err(|e| CliError::Config(format!("{} is not JSON: {e}", path.display())))?;
            let fluree = FlureeBuilder::from_json_ld(&json)?
                .without_indexing()
                .build_client()
                .await?;
            return Ok(Self::Local(Box::new(fluree)));
        }
        Err(CliError::Usage(
            "encryption commands need --remote <name> or --connection-config <path>".to_string(),
        ))
    }

    fn holder() -> String {
        format!(
            "cli@{}:{}",
            std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string()),
            std::process::id()
        )
    }

    async fn keys(&self) -> CliResult<Value> {
        match self {
            Self::Remote { client, .. } => Ok(client.encryption_keys().await?),
            Self::Local(fluree) => match fluree.key_rotation_status().await {
                Ok(status) => Ok(json!({
                    "encrypted": true,
                    "key_ids": status.key_ids,
                    "current_key_id": status.current_key_id,
                })),
                Err(fluree_db_api::ApiError::Config(_)) => Ok(json!({
                    "encrypted": false, "key_ids": [], "current_key_id": null
                })),
                Err(e) => Err(e.into()),
            },
        }
    }

    async fn status(&self) -> CliResult<Value> {
        match self {
            Self::Remote { client, .. } => Ok(client.encryption_rotate_status().await?),
            Self::Local(fluree) => Ok(serde_json::to_value(fluree.key_rotation_status().await?)?),
        }
    }

    async fn rotate(&self, body: Value) -> CliResult<Value> {
        match self {
            Self::Remote { client, .. } => Ok(client.encryption_rotate(&body).await?),
            Self::Local(fluree) => {
                let progress = fluree
                    .start_key_rotation(KeyRotationOptions {
                        retire_key_id: body["retire_key_id"].as_u64().unwrap_or(0) as u32,
                        dry_run: body["dry_run"].as_bool().unwrap_or(false),
                        ledger: body["ledger"].as_str().map(str::to_string),
                        max_bytes_per_sec: body["max_bytes_per_sec"].as_u64(),
                        holder: Self::holder(),
                    })
                    .await?;
                Ok(serde_json::to_value(progress)?)
            }
        }
    }

    async fn signal(&self, signal: &str) -> CliResult<Value> {
        match self {
            Self::Remote { client, .. } => Ok(client.encryption_rotate_signal(signal).await?),
            Self::Local(fluree) => {
                match signal {
                    "pause" => fluree.pause_key_rotation()?,
                    _ => fluree.cancel_key_rotation()?,
                }
                Ok(json!({"ok": true}))
            }
        }
    }

    async fn verify(&self, retire: u32) -> CliResult<Value> {
        match self {
            Self::Remote { client, .. } => Ok(client.encryption_rotate_verify(retire).await?),
            Self::Local(fluree) => Ok(serde_json::to_value(
                fluree.verify_key_rotation(retire).await?,
            )?),
        }
    }

    /// A local sweep lives in this process: block until it stops, since
    /// exiting would abandon it mid-unit (the record would resume it, but
    /// nobody would be running).
    async fn wait_local(&self) -> CliResult<()> {
        if let Self::Local(fluree) = self {
            fluree.wait_for_key_rotation().await?;
        }
        Ok(())
    }

    async fn finish(&self) {
        if let Self::Remote { client, name, dirs } = self {
            context::persist_refreshed_tokens(client, name, dirs).await;
        }
    }
}

pub async fn run(action: EncryptionAction, config_path: Option<&Path>) -> CliResult<()> {
    match action {
        EncryptionAction::GenerateKey => {
            use base64::Engine;
            use rand::RngCore;
            let mut key = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut key);
            println!("{}", base64::engine::general_purpose::STANDARD.encode(key));
            Ok(())
        }
        EncryptionAction::Status { target } => {
            let t = Target::resolve(&target, config_path).await?;
            let keys = t.keys().await?;
            if keys["encrypted"] == json!(false) {
                if target.json {
                    println!("{}", serde_json::to_string_pretty(&keys)?);
                } else {
                    println!("Storage is not encrypted.");
                }
                t.finish().await;
                return Ok(());
            }
            let status = t.status().await?;
            if target.json {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                print_status(&status);
            }
            t.finish().await;
            Ok(())
        }
        EncryptionAction::Rotate {
            retire,
            dry_run,
            ledger,
            rate,
            wait,
            target,
        } => {
            let t = Target::resolve(&target, config_path).await?;
            let mut body = json!({ "retire_key_id": retire, "dry_run": dry_run });
            if let Some(ledger) = ledger {
                body["ledger"] = json!(ledger);
            }
            if let Some(rate) = rate {
                body["max_bytes_per_sec"] = json!(parse_rate(&rate)?);
            }
            let progress = t.rotate(body).await?;
            if target.json && !wait {
                println!("{}", serde_json::to_string_pretty(&progress)?);
            } else {
                eprintln!(
                    "  {} retiring key {} onto key {}{}",
                    "rotate:".cyan().bold(),
                    retire,
                    progress["current_key_id"],
                    if dry_run { " (dry run)" } else { "" }
                );
            }
            if wait || matches!(t, Target::Local(_)) {
                wait_for_stop(&t, target.json).await?;
            }
            t.finish().await;
            Ok(())
        }
        EncryptionAction::Resume { wait, target } => {
            let t = Target::resolve(&target, config_path).await?;
            let status = t.status().await?;
            let Some(retire) = status["progress"]["retire_key_id"].as_u64() else {
                return Err(CliError::NotFound(
                    "no rotation record to resume".to_string(),
                ));
            };
            let progress = t
                .rotate(json!({
                    "retire_key_id": retire,
                    "ledger": status["progress"]["ledger_scope"],
                }))
                .await?;
            if target.json && !wait {
                println!("{}", serde_json::to_string_pretty(&progress)?);
            } else {
                eprintln!(
                    "  {} resumed at unit {}/{}",
                    "rotate:".cyan().bold(),
                    progress["units_done"],
                    progress["units_total"]
                );
            }
            if wait || matches!(t, Target::Local(_)) {
                wait_for_stop(&t, target.json).await?;
            }
            t.finish().await;
            Ok(())
        }
        EncryptionAction::Pause { target } => {
            let t = Target::resolve(&target, config_path).await?;
            let out = t.signal("pause").await?;
            if target.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Pause requested; the sweep stops after its next blob.");
            }
            t.finish().await;
            Ok(())
        }
        EncryptionAction::Cancel { target } => {
            let t = Target::resolve(&target, config_path).await?;
            let out = t.signal("cancel").await?;
            if target.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Cancel requested; the next rotate starts over.");
            }
            t.finish().await;
            Ok(())
        }
        EncryptionAction::Verify { retire, target } => {
            let t = Target::resolve(&target, config_path).await?;
            let progress = t.verify(retire).await?;
            if target.json {
                println!("{}", serde_json::to_string_pretty(&progress)?);
            } else {
                let remaining = progress["completion"]["remaining_on_retired"]
                    .as_u64()
                    .unwrap_or(0);
                if remaining == 0 {
                    println!(
                        "{} no blob remains on key {retire}; it can be removed from configuration.",
                        "verified:".green().bold()
                    );
                } else {
                    println!(
                        "{} {remaining} blob(s) still on key {retire}; run `fluree encryption resume`.",
                        "incomplete:".yellow().bold()
                    );
                }
            }
            t.finish().await;
            Ok(())
        }
    }
}

/// Poll status until the sweep leaves `running`, printing one line per
/// change. A local sweep is awaited directly instead of polled.
async fn wait_for_stop(t: &Target, json_out: bool) -> CliResult<()> {
    if matches!(t, Target::Local(_)) {
        t.wait_local().await?;
        let status = t.status().await?;
        if json_out {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            print_status(&status);
        }
        return Ok(());
    }
    let mut last_line = String::new();
    loop {
        let status = t.status().await?;
        let progress = &status["progress"];
        let state = progress["state"].as_str().unwrap_or("");
        let line = progress_line(&status);
        if line != last_line {
            eprintln!("  {line}");
            last_line = line;
        }
        if state != "running" {
            if json_out {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                print_status(&status);
            }
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn progress_line(status: &Value) -> String {
    let p = &status["progress"];
    let flag = if status["stalled"].as_bool() == Some(true) {
        " STALLED"
    } else if status["released"].as_bool() == Some(true) {
        " RELEASED"
    } else {
        ""
    };
    format!(
        "{} units {}/{} scanned {} rewritten {} failed {}{}",
        p["state"].as_str().unwrap_or("?"),
        p["units_done"],
        p["units_total"],
        p["scanned"],
        p["rewritten"],
        p["failed"],
        flag
    )
}

fn print_status(status: &Value) {
    println!(
        "Keys held: {} (current: {})",
        status["key_ids"], status["current_key_id"]
    );
    let p = &status["progress"];
    if p.is_null() {
        println!("No rotation has been started.");
        return;
    }
    let state = p["state"].as_str().unwrap_or("?");
    let headline = match serde_json::from_value::<KeyRotationState>(p["state"].clone()) {
        Ok(KeyRotationState::Completed) => "completed".green().bold(),
        Ok(KeyRotationState::Running) if status["stalled"].as_bool() == Some(true) => {
            "running (STALLED: no checkpoint recently)".red().bold()
        }
        Ok(KeyRotationState::Running) if status["released"].as_bool() == Some(true) => {
            "running (RELEASED: waiting for the next holder; `resume` takes it over)"
                .yellow()
                .bold()
        }
        Ok(KeyRotationState::Running) => "running".cyan().bold(),
        Ok(KeyRotationState::Swept | KeyRotationState::Failed) => state.yellow().bold(),
        _ => state.normal(),
    };
    println!(
        "Rotation: {} — retiring key {} onto key {}{}",
        headline,
        p["retire_key_id"],
        p["current_key_id"],
        if p["dry_run"].as_bool() == Some(true) {
            " (dry run)"
        } else {
            ""
        }
    );
    println!(
        "  holder {}  last checkpoint {}s ago{}",
        p["holder"].as_str().unwrap_or("-"),
        status["seconds_since_update"],
        if status["active_here"].as_bool() == Some(true) {
            "  (running on this node)"
        } else {
            ""
        }
    );
    println!(
        "  units {}/{}{}",
        p["units_done"],
        p["units_total"],
        p["unit"]
            .as_str()
            .map(|u| format!("  in {u}"))
            .unwrap_or_default()
    );
    println!(
        "  scanned {}  rewritten {} ({} bytes)  already current {}  other keys {}  not enveloped {}  failed {}",
        p["scanned"], p["rewritten"], p["bytes_rewritten"], p["already_current"],
        p["on_other_keys"], p["not_enveloped"], p["failed"]
    );
    if let Some(c) = p["completion"].as_object() {
        println!(
            "  verified: {} blob(s) remain on the retiring key",
            c["remaining_on_retired"]
        );
    }
    if let Some(err) = p["last_error"].as_str() {
        println!("  last error: {err}");
    }
    if let Some(addrs) = p["failed_addresses"].as_array().filter(|a| !a.is_empty()) {
        println!("  failed addresses (first {}):", addrs.len());
        for a in addrs.iter().take(10) {
            println!("    {}", a.as_str().unwrap_or("?"));
        }
    }
}

/// "50mb" → bytes per second; a bare number is bytes.
fn parse_rate(s: &str) -> CliResult<u64> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1u64 << 30)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1u64 << 20)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1u64 << 10)
    } else {
        (s.as_str(), 1)
    };
    num.trim()
        .parse::<u64>()
        .map(|n| n * mult)
        .map_err(|_| CliError::Usage(format!("cannot parse rate '{s}': use e.g. 50mb")))
}

#[cfg(test)]
mod tests {
    use super::parse_rate;

    #[test]
    fn rate_suffixes() {
        assert_eq!(parse_rate("50mb").unwrap(), 50 << 20);
        assert_eq!(parse_rate("2GB").unwrap(), 2 << 30);
        assert_eq!(parse_rate("512").unwrap(), 512);
        assert!(parse_rate("fast").is_err());
    }
}
