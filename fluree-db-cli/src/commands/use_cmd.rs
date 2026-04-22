use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(ledger: &str, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(ledger);

    // Check if it's a local ledger
    let record = fluree.nameservice().lookup(&ledger_id).await?;
    if record.is_some() {
        config::write_active_ledger(dirs.data_dir(), ledger)?;
        println!("Now using ledger '{ledger}'");
        return Ok(());
    }

    // Check if it's a tracked ledger
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(ledger).is_some() || store.get_tracked(&ledger_id).is_some() {
        config::write_active_ledger(dirs.data_dir(), ledger)?;
        println!("Now using tracked ledger '{ledger}'");
        return Ok(());
    }

    Err(CliError::NotFound(format!("ledger '{ledger}' not found")))
}
