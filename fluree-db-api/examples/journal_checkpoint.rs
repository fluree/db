//! Offline checkpoint and retirement; stop every process using this root first.
use fluree_db_api::local_journal_ledger::JournalLedger;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 1 {
        return Err("usage: journal_checkpoint JOURNAL_ROOT (server must be stopped)".into());
    }
    JournalLedger::checkpoint_offline(args[0].clone().into()).await?;
    let reopened = JournalLedger::open(args[0].clone().into()).await?;
    println!("{}", serde_json::to_string(&reopened.head().await?)?);
    Ok(())
}
