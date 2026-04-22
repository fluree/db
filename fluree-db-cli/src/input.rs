use crate::error::{CliError, CliResult};
use std::io::{self, IsTerminal, Read};
use std::path::{Path, PathBuf};

/// Where the input data comes from.
pub enum InputSource {
    /// From a file on disk.
    File(PathBuf),
    /// From the `-e` inline expression.
    Inline(String),
    /// From stdin (piped).
    Stdin,
}

/// Resolve the input source with priority: `-e` > positional inline > `-f` > positional file > stdin.
pub fn resolve_input(
    expr: Option<&str>,
    positional_inline: Option<&str>,
    file_flag: Option<&Path>,
    positional_file: Option<&Path>,
) -> CliResult<InputSource> {
    if let Some(e) = expr {
        return Ok(InputSource::Inline(e.to_string()));
    }
    if let Some(q) = positional_inline {
        return Ok(InputSource::Inline(q.to_string()));
    }
    if let Some(f) = file_flag.or(positional_file) {
        return Ok(InputSource::File(f.to_path_buf()));
    }
    if !io::stdin().is_terminal() {
        return Ok(InputSource::Stdin);
    }
    Err(CliError::Input(format!(
        "no input provided\n  {} pass inline, use -f for a file, or pipe via stdin",
        colored::Colorize::bold(colored::Colorize::cyan("help:"))
    )))
}

/// Read content from the resolved input source.
pub fn read_input(source: &InputSource) -> CliResult<String> {
    match source {
        InputSource::File(path) => std::fs::read_to_string(path)
            .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display()))),
        InputSource::Inline(s) => Ok(s.clone()),
        InputSource::Stdin => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            Ok(buf)
        }
    }
}
