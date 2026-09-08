//! Create a bounded WAL root from a quiescent indexed ordinary ledger.
//! Usage: journal_bootstrap SOURCE_ROOT EMPTY_TARGET_ROOT LEDGER_ID
use fluree_db_api::local_journal_ledger::JournalLedger;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: journal_bootstrap SOURCE_ROOT EMPTY_TARGET_ROOT LEDGER_ID".into());
    }
    let ledger = args[2]
        .to_str()
        .ok_or("ledger ID must be UTF-8")?
        .to_string();
    let target = JournalLedger::bootstrap(
        args[1].clone().into(),
        args[0].clone().into(),
        ledger,
        "http-benchmark-v1".into(),
    )
    .await?;
    println!("{}", serde_json::to_string(&target.head().await?)?);
    Ok(())
}
