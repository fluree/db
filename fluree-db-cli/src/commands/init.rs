use crate::config;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::{generate_config_template_for, ConfigFormat};

pub fn run(global: bool, format: ConfigFormat) -> CliResult<()> {
    let dirs = config::resolve_init_dirs(global)?;

    // For global mode with split dirs, use the absolute data-dir storage path
    // so the server finds the right directory regardless of working directory.
    let storage_override = if !dirs.is_unified() {
        let path = dirs.data_dir().join("storage");
        let path_str = path.to_str().ok_or_else(|| {
            CliError::Config(format!(
                "data directory path is not valid UTF-8: {}",
                path.display()
            ))
        })?;
        Some(path_str.to_owned())
    } else {
        None
    };
    let template = generate_config_template_for(format, storage_override.as_deref());

    config::init_fluree_dir(&dirs, &template, format.filename())?;

    if dirs.is_unified() {
        println!("Initialized Fluree in {}", dirs.data_dir().display());
    } else {
        println!(
            "Initialized Fluree globally:\n  config: {}\n  data:   {}",
            dirs.config_dir().display(),
            dirs.data_dir().display(),
        );
    }
    Ok(())
}
