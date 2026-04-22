use crate::cli::ConfigAction;
use crate::config::{detect_config_file, ConfigFileFormat};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;

pub fn run(action: ConfigAction, dirs: &FlureeDir) -> CliResult<()> {
    let (config_path, format) = detect_config_file(dirs.config_dir()).unwrap_or_else(|| {
        // Default to TOML when no config file exists yet
        (
            dirs.config_dir().join("config.toml"),
            ConfigFileFormat::Toml,
        )
    });

    match action {
        // SetOrigins is handled in main.rs (async dispatch) before calling run().
        ConfigAction::SetOrigins { .. } => unreachable!("handled in main.rs"),

        ConfigAction::Get { key } => match format {
            ConfigFileFormat::Toml => get_toml(&config_path, &key),
            ConfigFileFormat::JsonLd => get_json(&config_path, &key),
        },

        ConfigAction::Set { key, value } => match format {
            ConfigFileFormat::Toml => set_toml(&config_path, &key, &value),
            ConfigFileFormat::JsonLd => set_json(&config_path, &key, &value),
        },

        ConfigAction::List => match format {
            ConfigFileFormat::Toml => list_toml(&config_path),
            ConfigFileFormat::JsonLd => list_json(&config_path),
        },
    }
}

// ---------------------------------------------------------------------------
// TOML operations (existing logic)
// ---------------------------------------------------------------------------

fn get_toml(config_path: &Path, key: &str) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    let doc: toml::Value = content
        .parse()
        .map_err(|e: toml::de::Error| CliError::Config(format!("failed to parse config: {e}")))?;

    match lookup_toml_key(&doc, key) {
        Some(val) => {
            println!("{}", format_toml_value(val));
            Ok(())
        }
        None => Err(CliError::NotFound(format!("config key '{key}' not set"))),
    }
}

fn set_toml(config_path: &Path, key: &str, value: &str) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    let mut doc: toml_edit::DocumentMut = content.parse().map_err(|e: toml_edit::TomlError| {
        CliError::Config(format!("failed to parse config: {e}"))
    })?;

    set_toml_key(&mut doc, key, value)?;

    std::fs::write(config_path, doc.to_string())
        .map_err(|e| CliError::Config(format!("failed to write config: {e}")))?;

    println!("Set '{key}' = '{value}'");
    Ok(())
}

fn list_toml(config_path: &Path) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    if content.trim().is_empty() {
        println!("(no configuration set)");
        return Ok(());
    }

    let doc: toml::Value = content
        .parse()
        .map_err(|e: toml::de::Error| CliError::Config(format!("failed to parse config: {e}")))?;

    // A file with only comments parses as an empty table — treat it
    // the same as an empty file.
    if doc.as_table().is_some_and(toml::map::Map::is_empty) {
        println!("(no configuration set)");
        return Ok(());
    }

    print_toml_flat("", &doc);
    Ok(())
}

/// Look up a dotted key path in a TOML value.
fn lookup_toml_key<'a>(val: &'a toml::Value, key: &str) -> Option<&'a toml::Value> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut current = val;
    for part in &parts {
        current = current.get(part)?;
    }
    Some(current)
}

/// Set a dotted key path in a toml_edit document.
fn set_toml_key(doc: &mut toml_edit::DocumentMut, key: &str, value: &str) -> CliResult<()> {
    let parts: Vec<&str> = key.split('.').collect();

    if parts.is_empty() {
        return Err(CliError::Usage("empty config key".into()));
    }

    // Navigate to the parent table, creating intermediate tables as needed
    let mut table = doc.as_table_mut();
    for part in &parts[..parts.len() - 1] {
        if !table.contains_key(part) {
            table.insert(part, toml_edit::Item::Table(toml_edit::Table::new()));
        }
        table = table
            .get_mut(part)
            .and_then(|item| item.as_table_mut())
            .ok_or_else(|| CliError::Config(format!("key component '{part}' is not a table")))?;
    }

    let leaf = parts.last().unwrap();

    // Auto-detect value type
    let toml_value = if value == "true" {
        toml_edit::value(true)
    } else if value == "false" {
        toml_edit::value(false)
    } else if let Ok(n) = value.parse::<i64>() {
        toml_edit::value(n)
    } else if let Ok(f) = value.parse::<f64>() {
        toml_edit::value(f)
    } else {
        toml_edit::value(value)
    };

    table.insert(leaf, toml_value);
    Ok(())
}

/// Print a TOML value in flat key=value format.
fn print_toml_flat(prefix: &str, val: &toml::Value) {
    match val {
        toml::Value::Table(map) => {
            for (k, v) in map {
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                print_toml_flat(&full_key, v);
            }
        }
        _ => {
            println!("{prefix} = {}", format_toml_value(val));
        }
    }
}

/// Format a TOML value for display.
fn format_toml_value(val: &toml::Value) -> String {
    match val {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(n) => n.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_toml_value).collect();
            format!("[{}]", items.join(", "))
        }
        toml::Value::Table(_) => {
            // For tables, show as TOML inline
            val.to_string()
        }
        toml::Value::Datetime(dt) => dt.to_string(),
    }
}

// ---------------------------------------------------------------------------
// JSON-LD operations
// ---------------------------------------------------------------------------

fn get_json(config_path: &Path, key: &str) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    let doc: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CliError::Config(format!("failed to parse config: {e}")))?;

    match lookup_json_key(&doc, key) {
        Some(val) => {
            println!("{}", format_json_value(val));
            Ok(())
        }
        None => Err(CliError::NotFound(format!("config key '{key}' not set"))),
    }
}

fn set_json(config_path: &Path, key: &str, value: &str) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    let mut doc: serde_json::Value = if content.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(&content)
            .map_err(|e| CliError::Config(format!("failed to parse config: {e}")))?
    };

    set_json_key(&mut doc, key, value)?;

    let pretty = serde_json::to_string_pretty(&doc)
        .map_err(|e| CliError::Config(format!("failed to serialize config: {e}")))?;
    std::fs::write(config_path, pretty)
        .map_err(|e| CliError::Config(format!("failed to write config: {e}")))?;

    println!("Set '{key}' = '{value}'");
    Ok(())
}

fn list_json(config_path: &Path) -> CliResult<()> {
    let content = std::fs::read_to_string(config_path).unwrap_or_default();
    if content.trim().is_empty() {
        println!("(no configuration set)");
        return Ok(());
    }

    let doc: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CliError::Config(format!("failed to parse config: {e}")))?;

    let obj = match doc.as_object() {
        Some(o) if o.is_empty() => {
            println!("(no configuration set)");
            return Ok(());
        }
        Some(o) => o,
        None => {
            println!("(no configuration set)");
            return Ok(());
        }
    };

    // Skip @context and _comment keys in listing — they're metadata, not config
    let has_config_keys = obj
        .keys()
        .any(|k| !k.starts_with('@') && !k.starts_with('_'));
    if !has_config_keys {
        println!("(no configuration set)");
        return Ok(());
    }

    print_json_flat("", &doc);
    Ok(())
}

/// Look up a dotted key path in a JSON value.
fn lookup_json_key<'a>(val: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut current = val;
    for part in &parts {
        current = current.get(part)?;
    }
    Some(current)
}

/// Set a dotted key path in a JSON value.
fn set_json_key(doc: &mut serde_json::Value, key: &str, value: &str) -> CliResult<()> {
    let parts: Vec<&str> = key.split('.').collect();

    if parts.is_empty() {
        return Err(CliError::Usage("empty config key".into()));
    }

    // Navigate to the parent object, creating intermediate objects as needed
    let mut current = doc;
    for part in &parts[..parts.len() - 1] {
        if !current.get(part).is_some_and(serde_json::Value::is_object) {
            current[part] = serde_json::json!({});
        }
        current = current.get_mut(part).unwrap();
    }

    let leaf = parts.last().unwrap();

    // Auto-detect value type (same heuristic as TOML)
    let json_value = if value == "true" {
        serde_json::Value::Bool(true)
    } else if value == "false" {
        serde_json::Value::Bool(false)
    } else if let Ok(n) = value.parse::<i64>() {
        serde_json::Value::Number(n.into())
    } else if let Ok(f) = value.parse::<f64>() {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
    } else {
        serde_json::Value::String(value.to_string())
    };

    current[leaf] = json_value;
    Ok(())
}

/// Print a JSON value in flat key=value format.
/// Skips `@context` and `_comment` metadata keys.
fn print_json_flat(prefix: &str, val: &serde_json::Value) {
    match val {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                // Skip JSON-LD metadata keys
                if k.starts_with('@') || k.starts_with('_') {
                    continue;
                }
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                print_json_flat(&full_key, v);
            }
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_json_value).collect();
            println!("{prefix} = [{}]", items.join(", "));
        }
        _ => {
            println!("{prefix} = {}", format_json_value(val));
        }
    }
}

/// Format a JSON value for display.
fn format_json_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_json_value).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(_) => val.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Set-origins (async, unchanged)
// ---------------------------------------------------------------------------

/// Set origin configuration for a ledger (stores LedgerConfig blob in CAS
/// and updates the config_id on the NsRecord).
pub async fn run_set_origins(ledger: &str, file: &Path, dirs: &FlureeDir) -> CliResult<()> {
    use fluree_db_core::ContentKind;
    use fluree_db_core::ContentStore;
    use fluree_db_nameservice::{
        ConfigCasResult, ConfigLookup, ConfigPayload, ConfigPublisher, ConfigValue, LedgerConfig,
    };

    let config_json = std::fs::read(file)
        .map_err(|e| CliError::Config(format!("failed to read origins file: {e}")))?;
    let config: LedgerConfig = serde_json::from_slice(&config_json)
        .map_err(|e| CliError::Config(format!("invalid origins config: {e}")))?;

    let ledger_id = context::to_ledger_id(ledger);
    let fluree = context::build_fluree(dirs)?;

    // Serialize to canonical bytes and store in CAS.
    let canonical_bytes = config.to_bytes();
    let content_store = fluree.content_store(&ledger_id);
    let cid = content_store
        .put(ContentKind::LedgerConfig, &canonical_bytes)
        .await
        .map_err(|e| CliError::Config(format!("failed to store LedgerConfig: {e}")))?;

    // Update config_id on the NsRecord via ConfigPublisher.
    let current = fluree
        .nameservice_mode()
        .get_config(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to get config: {e}")))?;
    let existing_payload = current
        .as_ref()
        .and_then(|c| c.payload.clone())
        .unwrap_or_default();
    let new_config = ConfigValue::new(
        current.as_ref().map_or(1, |c| c.v + 1),
        Some(ConfigPayload {
            config_id: Some(cid.clone()),
            default_context: existing_payload.default_context,
            extra: existing_payload.extra,
        }),
    );
    match fluree
        .nameservice_mode()
        .push_config(&ledger_id, current.as_ref(), &new_config)
        .await
        .map_err(|e| CliError::Config(format!("failed to set config: {e}")))?
    {
        ConfigCasResult::Updated => {}
        ConfigCasResult::Conflict { .. } => {
            return Err(CliError::Config(format!(
                "config for '{ledger_id}' was modified concurrently; retry"
            )));
        }
    }

    println!("Config set for '{ledger_id}' (CID: {cid})");
    Ok(())
}
