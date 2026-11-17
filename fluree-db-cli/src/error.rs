use colored::Colorize;
use std::fmt;
use std::process;

/// Exit codes for the CLI.
pub const EXIT_SUCCESS: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_USAGE: i32 = 2;
/// `fluree verify`: provenance-only problems — state and every replication
/// path are intact, so a gate that only cares about replication may proceed.
pub const EXIT_VERIFY_PROVENANCE: i32 = 3;
/// `fluree verify`: the commit chain or index root is broken.
pub const EXIT_VERIFY_CHAIN: i32 = 4;

/// Unified error type for CLI operations.
pub enum CliError {
    /// Error from the Fluree API layer.
    Api(fluree_db_api::ApiError),
    /// Configuration / init issues.
    Config(String),
    /// Bad file path, unreadable input, parse failure.
    Input(String),
    /// Entity not found (ledger, record, etc.).
    NotFound(String),
    /// No active ledger set and no explicit argument.
    NoActiveLedger,
    /// `.fluree/` directory not found.
    NoFlureeDir,
    /// Argument / usage errors.
    Usage(String),
    /// Import errors.
    Import(String),
    /// Credential/token errors.
    Credential(fluree_db_credential::CredentialError),
    /// Remote ledger operation error (track mode).
    Remote(String),
    /// Server lifecycle error (start/stop/status).
    Server(String),
    /// Terminate with a specific exit code and no error message — the
    /// command already wrote its output (e.g. `validate` printed a
    /// non-conforming report). Library consumers of `run()` receive this
    /// as a typed outcome instead of a `process::exit`.
    ExitCode(i32),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Api(e) => write!(f, "{} {e}", "error:".red().bold()),
            CliError::Config(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::Input(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::NotFound(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::NoActiveLedger => write!(
                f,
                "{} no active ledger set and no <ledger> argument provided\n  {} run 'fluree use <ledger>' to set a default",
                "error:".red().bold(),
                "help:".cyan().bold(),
            ),
            CliError::NoFlureeDir => write!(
                f,
                "{} no .fluree/ directory found\n  {} run 'fluree init' to initialize",
                "error:".red().bold(),
                "help:".cyan().bold(),
            ),
            CliError::Usage(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::Import(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::Credential(e) => write!(f, "{} {e}", "error:".red().bold()),
            CliError::Remote(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::Server(msg) => write!(f, "{} {msg}", "error:".red().bold()),
            CliError::ExitCode(code) => write!(f, "exit code {code}"),
        }
    }
}

impl CliError {
    /// This error with `context` ahead of its message, as the same kind, so
    /// it keeps its exit code (a usage error still exits 2) and prints one
    /// `error:` prefix. Formatting a `CliError` into a new one's message
    /// would repeat the prefix and turn every kind into the new one.
    ///
    /// A typed error (`Api`, `Credential`) has no message to prefix and
    /// becomes `Remote` with its text; `NoActiveLedger`, `NoFlureeDir` and
    /// `ExitCode` carry their own guidance and are returned unchanged.
    pub fn context(self, context: impl fmt::Display) -> Self {
        let with = |msg: String| format!("{context}: {msg}");
        match self {
            CliError::Config(msg) => CliError::Config(with(msg)),
            CliError::Input(msg) => CliError::Input(with(msg)),
            CliError::NotFound(msg) => CliError::NotFound(with(msg)),
            CliError::Usage(msg) => CliError::Usage(with(msg)),
            CliError::Import(msg) => CliError::Import(with(msg)),
            CliError::Remote(msg) => CliError::Remote(with(msg)),
            CliError::Server(msg) => CliError::Server(with(msg)),
            CliError::Api(e) => CliError::Remote(with(e.to_string())),
            CliError::Credential(e) => CliError::Remote(with(e.to_string())),
            unchanged @ (CliError::NoActiveLedger
            | CliError::NoFlureeDir
            | CliError::ExitCode(_)) => unchanged,
        }
    }
}

impl fmt::Debug for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<fluree_db_doc::DocError> for CliError {
    fn from(e: fluree_db_doc::DocError) -> Self {
        match e {
            fluree_db_doc::DocError::Config(m) => CliError::Config(m),
            other => CliError::Input(other.to_string()),
        }
    }
}

impl From<fluree_db_api::ApiError> for CliError {
    fn from(e: fluree_db_api::ApiError) -> Self {
        CliError::Api(e)
    }
}

impl From<fluree_db_api::ImportError> for CliError {
    fn from(e: fluree_db_api::ImportError) -> Self {
        CliError::Import(e.to_string())
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::Input(e.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        CliError::Input(format!("JSON parse error: {e}"))
    }
}

impl From<fluree_db_nameservice::NameServiceError> for CliError {
    fn from(e: fluree_db_nameservice::NameServiceError) -> Self {
        CliError::Api(fluree_db_api::ApiError::from(e))
    }
}

impl From<fluree_db_api::FormatError> for CliError {
    fn from(e: fluree_db_api::FormatError) -> Self {
        CliError::Input(format!("format error: {e}"))
    }
}

impl From<fluree_db_novelty::NoveltyError> for CliError {
    fn from(e: fluree_db_novelty::NoveltyError) -> Self {
        CliError::Input(format!("commit read error: {e}"))
    }
}

impl From<fluree_db_credential::CredentialError> for CliError {
    fn from(e: fluree_db_credential::CredentialError) -> Self {
        CliError::Credential(e)
    }
}

impl From<crate::remote_client::RemoteLedgerError> for CliError {
    fn from(e: crate::remote_client::RemoteLedgerError) -> Self {
        CliError::Remote(e.to_string())
    }
}

impl From<fluree_db_core::Error> for CliError {
    fn from(e: fluree_db_core::Error) -> Self {
        CliError::Input(format!("core error: {e}"))
    }
}

impl From<fluree_db_core::ledger_id::LedgerIdParseError> for CliError {
    fn from(e: fluree_db_core::ledger_id::LedgerIdParseError) -> Self {
        CliError::Input(e.to_string())
    }
}

/// Print error and exit with the appropriate code.
pub fn exit_with_error(err: CliError) -> ! {
    // A typed exit carries no message — the command already wrote its output.
    if let CliError::ExitCode(code) = &err {
        process::exit(*code);
    }
    eprintln!("{err}");
    let code = match &err {
        CliError::Usage(_) => EXIT_USAGE,
        _ => EXIT_ERROR,
    };
    process::exit(code)
}

pub type CliResult<T> = std::result::Result<T, CliError>;

#[cfg(test)]
mod tests {
    use super::CliError;

    #[test]
    fn context_keeps_the_kind_and_one_prefix() {
        let remote = CliError::Remote("bad request: no such table".into()).context("failed to map");
        assert!(
            matches!(remote, CliError::Remote(ref m) if m == "failed to map: bad request: no such table")
        );
        assert_eq!(remote.to_string().matches("error:").count(), 1, "{remote}");
        // A usage error stays one, so it still exits 2.
        let usage = CliError::Usage("--table is required".into()).context("failed to map");
        assert!(
            matches!(usage, CliError::Usage(ref m) if m == "failed to map: --table is required")
        );
        assert!(matches!(
            CliError::ExitCode(3).context("failed to map"),
            CliError::ExitCode(3)
        ));
    }
}
