use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(ledger: &str, dirs: &FlureeDir) -> CliResult<()> {
    let ledger_id = context::to_ledger_id(ledger)?;

    // Check if it's a local ledger, asking the server running on the
    // project's storage when there is one.
    let local = match context::local_server_client(dirs) {
        Some(server) => server.ledger_exists(&ledger_id).await?,
        None => {
            let fluree = context::build_fluree(dirs).await?;
            fluree.nameservice().lookup(&ledger_id).await?.is_some()
        }
    };
    if local {
        config::write_active_ledger(dirs.data_dir(), ledger)?;
        println!("Now using ledger '{ledger}'");
        return Ok(());
    }

    // Check if it's a tracked ledger
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(&ledger_id).is_some() {
        config::write_active_ledger(dirs.data_dir(), ledger)?;
        println!("Now using tracked ledger '{ledger}'");
        return Ok(());
    }

    Err(CliError::NotFound(format!("ledger '{ledger}' not found")))
}
