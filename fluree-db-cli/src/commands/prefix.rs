use crate::cli::PrefixAction;
use crate::config;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub fn run(action: PrefixAction, dirs: &FlureeDir) -> CliResult<()> {
    match action {
        PrefixAction::Add { prefix, iri } => {
            // Validate prefix (no colons, not empty)
            if prefix.is_empty() {
                return Err(CliError::Usage("prefix cannot be empty".into()));
            }
            if prefix.contains(':') {
                return Err(CliError::Usage("prefix cannot contain ':'".into()));
            }

            // Validate IRI (should end with / or #)
            if !iri.ends_with('/') && !iri.ends_with('#') {
                eprintln!(
                    "warning: IRI '{iri}' doesn't end with '/' or '#'; this may cause issues with IRI expansion"
                );
            }

            config::add_prefix(dirs.data_dir(), &prefix, &iri)?;
            println!("Added prefix: {prefix} = <{iri}>");
            Ok(())
        }

        PrefixAction::Remove { prefix } => {
            let removed = config::remove_prefix(dirs.data_dir(), &prefix)?;
            if removed {
                println!("Removed prefix: {prefix}");
            } else {
                return Err(CliError::NotFound(format!("prefix '{prefix}' not found")));
            }
            Ok(())
        }

        PrefixAction::List => {
            let prefixes = config::read_prefixes(dirs.data_dir());
            if prefixes.is_empty() {
                println!("(no prefixes defined)");
                println!();
                println!("Add prefixes with: fluree prefix add <prefix> <iri>");
                println!("Example: fluree prefix add ex http://example.org/");
            } else {
                // Sort for consistent output
                let mut sorted: Vec<_> = prefixes.iter().collect();
                sorted.sort_by_key(|(k, _)| *k);

                for (prefix, iri) in sorted {
                    println!("{prefix}: <{iri}>");
                }
            }
            Ok(())
        }
    }
}
