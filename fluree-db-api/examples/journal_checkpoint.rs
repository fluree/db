//! Offline checkpoint and retirement; stop every process using this root first.
use fluree_db_api::local_journal_ledger::JournalLedger;
use std::time::Instant;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 1 && !(args.len() == 3 && args[1] == "--metrics") {
        return Err("usage: journal_checkpoint JOURNAL_ROOT [--metrics OUTPUT_JSON] (server must be stopped)".into());
    }
    let started = Instant::now();
    JournalLedger::checkpoint_offline(args[0].clone().into()).await?;
    let checkpoint_ms = started.elapsed().as_secs_f64() * 1000.0;
    let opening = Instant::now();
    let reopened = JournalLedger::open(args[0].clone().into()).await?;
    let head = reopened.head().await?;
    let reopen_ms = opening.elapsed().as_secs_f64() * 1000.0;
    if args.len() == 3 {
        // Diagnostic output only, outside both measured operations. This is not
        // a transaction receipt or a substitute for checkpoint/root validation.
        std::fs::write(
            &args[2],
            serde_json::to_vec_pretty(&serde_json::json!({
                "checkpoint_ms": checkpoint_ms, "reopen_ms": reopen_ms, "head": head,
                "reopen_cache": "same-process immediately after checkpoint"
            }))?,
        )?;
    }
    println!("{}", serde_json::to_string(&head)?);
    Ok(())
}
