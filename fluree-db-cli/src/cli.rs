use clap::{Args, Parser, Subcommand, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Policy enforcement flags shared by query and transaction subcommands.
///
/// Use these to test that policies stored in the ledger behave as configured,
/// or to test ad-hoc policies (`--policy` / `--policy-file`) that haven't been
/// persisted yet. Works against local and remote ledgers — see
/// `docs/security/policy-in-queries.md` for the full reference and the
/// remote impersonation rules.
#[derive(Args, Debug, Clone, Default)]
pub struct PolicyArgs {
    /// Execute as this identity (IRI). Resolves applicable policies via
    /// `f:policyClass` on the identity subject in the ledger.
    #[arg(long = "as", value_name = "IRI")]
    pub identity: Option<String>,

    /// Apply policies of the given class IRI. Repeatable. Narrows the active
    /// policy set to the intersection of the identity's policies and these
    /// classes, or (without `--as`) applies these classes directly.
    #[arg(long = "policy-class", value_name = "IRI")]
    pub policy_class: Vec<String>,

    /// Inline JSON-LD policy document(s) to apply for this request only.
    /// Useful for testing rules before persisting them to the ledger.
    /// Mutually exclusive with `--policy-file`. Pass a JSON object or array.
    #[arg(long = "policy", value_name = "JSON", conflicts_with = "policy_file")]
    pub policy: Option<String>,

    /// Read inline JSON-LD policy from a file (alternative to `--policy`).
    /// Same semantics — applied for this request only.
    #[arg(long = "policy-file", value_name = "PATH", conflicts_with = "policy")]
    pub policy_file: Option<PathBuf>,

    /// Bind variables for parameterized policies (e.g. `?$dept`). Pass a JSON
    /// object: `--policy-values '{"?$dept":"engineering"}'`. Variable keys
    /// must start with `?$`. Mutually exclusive with `--policy-values-file`.
    #[arg(
        long = "policy-values",
        value_name = "JSON",
        conflicts_with = "policy_values_file"
    )]
    pub policy_values: Option<String>,

    /// Read policy variable bindings from a JSON file (alternative to
    /// `--policy-values`).
    #[arg(
        long = "policy-values-file",
        value_name = "PATH",
        conflicts_with = "policy_values"
    )]
    pub policy_values_file: Option<PathBuf>,

    /// Allow access when no matching policy rules exist for the requested
    /// operation. Defaults to false (deny-by-default).
    #[arg(long = "default-allow")]
    pub default_allow: bool,
}

impl PolicyArgs {
    /// Returns true if the user supplied any policy flag.
    pub fn is_set(&self) -> bool {
        self.identity.is_some()
            || !self.policy_class.is_empty()
            || self.policy.is_some()
            || self.policy_file.is_some()
            || self.policy_values.is_some()
            || self.policy_values_file.is_some()
            || self.default_allow
    }

    /// Resolve `--policy` / `--policy-file` into a parsed JSON value, returning
    /// `None` when neither is set.
    pub fn resolve_policy(&self) -> Result<Option<serde_json::Value>, String> {
        if let Some(s) = &self.policy {
            return serde_json::from_str(s)
                .map(Some)
                .map_err(|e| format!("--policy is not valid JSON: {e}"));
        }
        if let Some(p) = &self.policy_file {
            let bytes = std::fs::read(p)
                .map_err(|e| format!("could not read --policy-file '{}': {e}", p.display()))?;
            return serde_json::from_slice(&bytes).map(Some).map_err(|e| {
                format!(
                    "--policy-file '{}' did not contain valid JSON: {e}",
                    p.display()
                )
            });
        }
        Ok(None)
    }

    /// Resolve `--policy-values` / `--policy-values-file` into a parsed JSON
    /// object, returning `None` when neither is set. Errors when the value is
    /// present but not a JSON object.
    pub fn resolve_policy_values(
        &self,
    ) -> Result<Option<std::collections::HashMap<String, serde_json::Value>>, String> {
        let raw = if let Some(s) = &self.policy_values {
            Some(
                serde_json::from_str::<serde_json::Value>(s)
                    .map_err(|e| format!("--policy-values is not valid JSON: {e}"))?,
            )
        } else if let Some(p) = &self.policy_values_file {
            let bytes = std::fs::read(p).map_err(|e| {
                format!("could not read --policy-values-file '{}': {e}", p.display())
            })?;
            Some(
                serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|e| {
                    format!(
                        "--policy-values-file '{}' did not contain valid JSON: {e}",
                        p.display()
                    )
                })?,
            )
        } else {
            None
        };
        match raw {
            None => Ok(None),
            Some(serde_json::Value::Object(obj)) => Ok(Some(obj.into_iter().collect())),
            Some(_) => Err(
                "--policy-values must be a JSON object (e.g. '{\"?$dept\":\"eng\"}')".to_string(),
            ),
        }
    }

    /// Convert into a `QueryConnectionOptions` usable by the fluree-db-api.
    /// Returns an error if `--policy` or `--policy-values` failed to parse.
    pub fn to_options(&self) -> Result<fluree_db_api::QueryConnectionOptions, String> {
        Ok(fluree_db_api::QueryConnectionOptions {
            identity: self.identity.clone(),
            policy_class: if self.policy_class.is_empty() {
                None
            } else {
                Some(self.policy_class.clone())
            },
            policy: self.resolve_policy()?,
            policy_values: self.resolve_policy_values()?,
            default_allow: self.default_allow,
            tracking: Default::default(),
        })
    }
}

#[derive(Parser)]
#[command(name = "fluree", about = "Fluree database CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable verbose output
    #[arg(long, short = 'v', global = true, conflicts_with = "quiet")]
    pub verbose: bool,

    /// Suppress non-essential output
    #[arg(long, short = 'q', global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Disable colored output (also respects NO_COLOR env var)
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Execute directly via the CLI, bypassing auto-routing through a local server.
    /// By default, if a local server is running (started via `fluree server start`),
    /// commands like query/insert/upsert/update are automatically routed through it.
    #[arg(long, global = true)]
    pub direct: bool,

    /// Path to config file
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Memory budget in MB for bulk import (0 = auto: 60% of system RAM).
    /// Derives chunk size, concurrency limits, and run budget when not set explicitly.
    #[arg(long, global = true, default_value_t = 0)]
    pub memory_budget_mb: usize,

    /// Number of parallel parse threads for bulk import.
    /// 0 = auto (system cores, default cap 6). Explicit values are not capped.
    #[arg(long, global = true, default_value_t = 0)]
    pub parallelism: usize,

    /// Timeout in seconds for remote HTTP requests (default: 300).
    /// Set higher for long-running queries or transactions.
    #[arg(long, global = true, default_value_t = 300)]
    pub timeout: u64,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new Fluree project directory
    Init {
        /// Create global config instead of local .fluree/ (uses $FLUREE_HOME or platform data dir)
        #[arg(long)]
        global: bool,

        /// Config file format
        #[arg(long, value_enum, default_value_t = InitFormat::Toml)]
        format: InitFormat,
    },

    /// Create a new ledger
    Create {
        /// Ledger name
        ledger: String,

        /// Import data from a file or directory.
        /// Accepts a single .ttl, .json, or .jsonld file, or a directory of
        /// .ttl/.trig or .jsonld files (bulk import, bypasses novelty).
        /// Files in a directory are processed in lexicographic order.
        #[arg(long)]
        from: Option<PathBuf>,

        /// Import memory history from a git-tracked .fluree-memory/ directory.
        /// Each git commit becomes a Fluree transaction, enabling time-travel
        /// queries over the memory store's evolution.
        /// Defaults to the current repo if no path is given.
        #[arg(long, num_args = 0..=1, default_missing_value = ".")]
        memory: Option<PathBuf>,

        /// Exclude user-scoped memories (.local/user.ttl) from --memory import
        #[arg(long)]
        no_user: bool,

        /// Chunk size in MB for splitting large Turtle files (0 = derive from memory budget).
        /// Only used when --from points to a .ttl file.
        #[arg(long, default_value_t = 0)]
        chunk_size_mb: usize,

        /// Memory budget in MB for bulk import (0 = auto: 60% of system RAM).
        /// Derives chunk size, concurrency limits, and run budget when not set explicitly.
        #[arg(long, default_value_t = 0)]
        memory_budget_mb: usize,

        /// Number of parallel parse threads for bulk import.
        /// 0 = auto (system cores, default cap 6). Explicit values are not capped.
        #[arg(long, default_value_t = 0)]
        parallelism: usize,

        /// Records per leaflet in index files. Default: 25000.
        /// Larger values produce fewer, bigger leaflets (less I/O, more memory per read).
        #[arg(long, default_value_t = 25_000)]
        leaflet_rows: usize,

        /// Leaflets per leaf file. Default: 10.
        /// Larger values produce fewer leaf files (shallower tree, bigger reads).
        #[arg(long, default_value_t = 10)]
        leaflets_per_leaf: usize,
    },

    /// Set the active ledger
    Use {
        /// Ledger name to set as active
        ledger: String,
    },

    /// List all ledgers
    List {
        /// List ledgers on a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Show detailed information about a ledger
    Info {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Query a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,

        /// Scope stats to a specific named graph within the ledger.
        ///
        /// Accepts a well-known name (`default`, `txn-meta`) or a graph IRI.
        #[arg(long)]
        graph: Option<String>,
    },

    /// Manage branches for a ledger
    Branch {
        #[command(subcommand)]
        action: BranchAction,
    },

    /// Drop (delete) a ledger
    Drop {
        /// Ledger name to drop
        name: String,

        /// Required flag to confirm deletion
        #[arg(long)]
        force: bool,
    },

    /// Insert data into a ledger
    ///
    /// Examples:
    ///   fluree insert '<http://example.org/alice> a <http://example.org/Person> .'
    ///   fluree insert -f data.ttl
    ///   fluree insert mydb -f data.jsonld
    ///   cat data.ttl | fluree insert
    Insert {
        /// Optional ledger name and/or inline data.
        ///
        /// With 0 args: uses active ledger; provide data via -e, -f, or stdin.
        /// With 1 arg: if it looks like data (JSON or Turtle), uses it as
        ///   inline data with the active ledger; if it's an existing file,
        ///   reads from it; otherwise treats it as a ledger name.
        /// With 2 args: first is ledger name, second is inline data.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline data expression (Turtle or JSON-LD).
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Read data from a file
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,

        #[command(flatten)]
        policy: PolicyArgs,
    },

    /// Update with full WHERE/DELETE/INSERT semantics
    ///
    /// Examples:
    ///   fluree update '{"where": [...], "delete": [...], "insert": [...]}'
    ///   fluree update -f update.json
    ///   fluree update -f update.ru --format sparql
    ///   cat update.json | fluree update
    Update {
        /// Optional ledger name and/or inline data.
        ///
        /// With 0 args: uses active ledger; provide data via -e, -f, or stdin.
        /// With 1 arg: if it looks like data (JSON or SPARQL UPDATE), uses it as
        ///   inline data with the active ledger; if it's an existing file,
        ///   reads from it; otherwise treats it as a ledger name.
        /// With 2 args: first is ledger name, second is inline data.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline data expression (JSON-LD or SPARQL UPDATE).
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Read data from a file
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,

        /// Data format (jsonld or sparql); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,

        #[command(flatten)]
        policy: PolicyArgs,
    },

    /// Upsert data into a ledger (insert or update existing)
    ///
    /// Examples:
    ///   fluree upsert '<http://example.org/alice> <http://example.org/name> "Alice" .'
    ///   fluree upsert mydb -f data.ttl
    ///   cat data.jsonld | fluree upsert
    Upsert {
        /// Optional ledger name and/or inline data.
        ///
        /// With 0 args: uses active ledger; provide data via -e, -f, or stdin.
        /// With 1 arg: if it looks like data (JSON or Turtle), uses it as
        ///   inline data with the active ledger; if it's an existing file,
        ///   reads from it; otherwise treats it as a ledger name.
        /// With 2 args: first is ledger name, second is inline data.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline data expression (Turtle or JSON-LD).
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Read data from a file
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,

        #[command(flatten)]
        policy: PolicyArgs,
    },

    /// Query a ledger
    ///
    /// Examples:
    ///   fluree query 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
    ///   fluree query mydb '{"select": ["*"], "where": {"@type": "Person"}}'
    ///   fluree query -f query.sparql
    ///   cat query.rq | fluree query
    Query {
        /// Optional ledger name and/or inline query.
        ///
        /// With 0 args: uses active ledger; provide query via -e, -f, or stdin.
        /// With 1 arg: if it looks like a query (SPARQL or JSON-LD), uses it
        ///   as inline input with the active ledger; if it's an existing file,
        ///   reads from it; otherwise treats it as a ledger name.
        /// With 2 args: first is ledger name, second is inline query.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline query expression (SPARQL or JSON-LD query).
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Read query from a file
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,

        /// Output format (json, typed-json, table, csv, or tsv)
        #[arg(long, default_value = "table")]
        format: String,

        /// Normalize arrays: always wrap multi-value properties in arrays (graph crawl only)
        #[arg(long)]
        normalize_arrays: bool,

        /// Benchmark mode: time execution only and print first 5 rows as a table (no full-result JSON formatting)
        #[arg(long)]
        bench: bool,

        /// Print an explain plan (no execution)
        #[arg(long)]
        explain: bool,

        /// Force SPARQL query format
        #[arg(long, conflicts_with = "jsonld")]
        sparql: bool,

        /// Force JSON-LD query format
        #[arg(long, conflicts_with = "sparql")]
        jsonld: bool,

        /// Query at a specific point in time (transaction number, commit hash, or ISO-8601 timestamp)
        #[arg(long)]
        at: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,

        #[command(flatten)]
        policy: PolicyArgs,
    },

    /// Show change history for an entity
    History {
        /// Entity IRI (e.g., "ex:alice" or full IRI). Uses stored prefixes for expansion.
        entity: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Start of time range (transaction number, default: 1)
        #[arg(long, default_value = "1")]
        from: String,

        /// End of time range (transaction number or "latest", default: latest)
        #[arg(long, default_value = "latest")]
        to: String,

        /// Filter to specific predicate
        #[arg(short = 'p', long)]
        predicate: Option<String>,

        /// Output format (json, table, csv, or tsv)
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Manage the default JSON-LD context for a ledger
    Context {
        #[command(subcommand)]
        action: ContextAction,
    },

    /// Export ledger data as Turtle, N-Triples, N-Quads, TriG, or JSON-LD
    Export {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Output format: turtle (ttl), ntriples (nt), jsonld, trig, or nquads (default: turtle)
        ///
        /// Note: exporting all graphs requires a dataset-capable format
        /// (`trig` or `nquads`).
        #[arg(long, default_value = "turtle")]
        format: String,

        /// Export all named graphs (dataset export), including system graphs.
        ///
        /// Use `--format trig` or `--format nquads` when this flag is set.
        #[arg(long)]
        all_graphs: bool,

        /// Export a specific named graph by IRI.
        ///
        /// Mutually exclusive with `--all-graphs`.
        #[arg(long)]
        graph: Option<String>,

        /// JSON-LD context for prefix declarations (overrides ledger default).
        ///
        /// Pass as inline JSON: `--context '{"ex": "http://example.org/"}'`
        #[arg(long)]
        context: Option<String>,

        /// Read context from a JSON file (overrides ledger default).
        #[arg(long, value_name = "FILE")]
        context_file: Option<std::path::PathBuf>,

        /// Query at a specific point in time
        #[arg(long)]
        at: Option<String>,
    },

    /// Show commit log for a ledger
    Log {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Show one-line summary per commit
        #[arg(long)]
        oneline: bool,

        /// Maximum number of commits to show
        #[arg(short = 'n', long)]
        count: Option<usize>,
    },

    /// Show the contents of a commit (decoded flakes with resolved IRIs)
    Show {
        /// Commit identifier: t:<N>, hex-digest prefix (min 6 chars), or full CID
        commit: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Manage IRI prefix mappings
    Prefix {
        #[command(subcommand)]
        action: PrefixAction,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, powershell, elvish)
        shell: clap_complete::Shell,
    },

    /// Manage JWS tokens for authentication
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },

    /// Manage remote servers
    Remote {
        #[command(subcommand)]
        action: RemoteAction,
    },

    /// Manage authentication tokens for remotes
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },

    /// Manage upstream tracking configuration
    Upstream {
        #[command(subcommand)]
        action: UpstreamAction,
    },

    /// Fetch refs from a remote (like git fetch)
    Fetch {
        /// Remote name (e.g., "origin")
        remote: String,
    },

    /// Pull (fetch + fast-forward) a ledger from its upstream
    Pull {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Skip pulling binary index data (indexes are pulled by default)
        #[arg(long)]
        no_indexes: bool,
    },

    /// Push a ledger to its upstream remote
    Push {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
    },

    /// Publish a local ledger to a remote server (create + push all commits)
    ///
    /// Creates the ledger on the remote if it doesn't exist, pushes all local
    /// commits, and configures upstream tracking for subsequent push/pull.
    ///
    /// Usage:
    ///   fluree publish <remote> [ledger]
    Publish {
        /// Remote name (e.g., "origin")
        remote: String,

        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Remote ledger name (defaults to local ledger name)
        #[arg(long)]
        remote_name: Option<String>,
    },

    /// Clone a ledger from a remote server (downloads all commits and indexes)
    ///
    /// Usage:
    ///   fluree clone <remote> <ledger>                        # named-remote clone
    ///   fluree clone --origin <uri> <ledger>                  # CID-based clone
    ///   fluree clone --origin <uri> --token <tok> <ledger>    # with auth
    Clone {
        /// Positional args: <remote> <ledger> (named-remote) or <ledger> (with --origin)
        #[arg(num_args = 1..=2)]
        args: Vec<String>,

        /// Bootstrap URI (e.g., "http://localhost:8090") — CID-based clone
        #[arg(long)]
        origin: Option<String>,

        /// Auth token for the origin server
        #[arg(long, requires = "origin")]
        token: Option<String>,

        /// Local alias for the cloned ledger (defaults to remote ledger name).
        /// Not yet supported: CAS addresses embed ledger paths, so aliasing
        /// requires address rewriting (planned for a future release).
        #[arg(long, hide = true)]
        alias: Option<String>,

        /// Skip pulling binary index data (indexes are pulled by default)
        #[arg(long)]
        no_indexes: bool,

        /// Skip cloning original transaction payloads.
        ///
        /// Commits are still cloned (the chain remains valid and verifiable),
        /// but the raw transaction blobs referenced by each commit are not
        /// transferred. Useful when only the materialized ledger state is
        /// needed — e.g., when cloning a large ledger for read-only query use.
        #[arg(long)]
        no_txns: bool,
    },

    /// Track a remote ledger (remote-only, no local data)
    Track {
        #[command(subcommand)]
        action: TrackAction,
    },

    /// Build or update the binary index for a ledger
    ///
    /// Performs incremental indexing when possible (merges only new commits
    /// into the existing index). Falls back to a full rebuild otherwise.
    /// Run this after transactions to clear novelty and speed up queries.
    Index {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
    },

    /// Full reindex from commit history
    ///
    /// Rebuilds the binary index from scratch by replaying all commits.
    /// Use this when the index is corrupted or you want a clean rebuild.
    /// For routine indexing after transactions, prefer `fluree index`.
    Reindex {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Manage the Fluree HTTP server
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },

    /// Developer memory — store and recall facts, decisions, constraints
    Memory {
        #[command(subcommand)]
        action: MemoryAction,
    },

    /// Model Context Protocol (MCP) server
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },

    /// Manage Apache Iceberg table connections
    Iceberg {
        #[command(subcommand)]
        action: IcebergAction,
    },
}

/// Branch subcommands.
#[derive(Subcommand)]
pub enum BranchAction {
    /// Create a new branch
    Create {
        /// New branch name (e.g., "dev", "feature-x")
        name: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Source branch to create from (defaults to "main")
        #[arg(long)]
        from: Option<String>,

        /// Commit to branch at (defaults to source branch HEAD).
        ///
        /// Accepts `t:N` for a transaction number, or a hex digest / full
        /// CID for prefix resolution. The source branch must be indexed
        /// for `t:` / prefix resolution (full CIDs work unconditionally).
        #[arg(long)]
        at: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Drop a branch
    Drop {
        /// Branch name to drop (e.g., "dev", "feature-x")
        name: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// List all branches
    List {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// List branches on a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Rebase a branch onto its source branch's current HEAD
    Rebase {
        /// Branch name to rebase (e.g., "dev", "feature-x")
        name: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Conflict resolution strategy (default: "take-both")
        /// Options: take-both, abort, take-source (theirs), take-branch (ours), skip
        #[arg(long, default_value = "take-both")]
        strategy: String,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Merge a branch into another branch
    Merge {
        /// Source branch name to merge from (e.g., "dev", "feature-x")
        source: String,

        /// Target branch to merge into (defaults to source's parent branch)
        #[arg(long)]
        target: Option<String>,

        /// Conflict resolution strategy (default: "take-both")
        /// Options: take-both, abort, take-source, take-branch
        #[arg(long, default_value = "take-both")]
        strategy: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Show a read-only merge preview between two branches
    ///
    /// Returns the rich diff (commits ahead/behind, conflict keys,
    /// fast-forward eligibility) without mutating any state.
    Diff {
        /// Source branch name (e.g., "dev", "feature-x")
        source: String,

        /// Target branch (defaults to source's parent branch)
        #[arg(long)]
        target: Option<String>,

        /// Cap on per-side commit list (default: 50 in CLI; 500 over HTTP).
        /// Pass 0 for unbounded (local mode only).
        #[arg(long, default_value_t = 50)]
        max_commits: usize,

        /// Cap on conflict keys returned (default: 50).
        /// Pass 0 for unbounded (local mode only).
        #[arg(long, default_value_t = 50)]
        max_conflict_keys: usize,

        /// Skip the conflict computation when only counts are needed
        #[arg(long)]
        no_conflicts: bool,

        /// Include source/target values for returned conflict keys
        #[arg(long)]
        conflict_details: bool,

        /// Strategy used to annotate conflict resolution labels
        /// Options: take-both, abort, take-source, take-branch
        #[arg(long)]
        strategy: Option<String>,

        /// Emit the raw JSON preview instead of a human-readable summary
        #[arg(long)]
        json: bool,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },
}

/// Memory subcommands.
#[derive(Subcommand)]
pub enum MemoryAction {
    /// Initialize the memory store and configure MCP for detected AI tools
    Init {
        /// Auto-confirm all detected tool installations (non-interactive)
        #[arg(long, short = 'y')]
        yes: bool,

        /// Skip MCP tool detection and installation
        #[arg(long)]
        no_mcp: bool,
    },

    /// Store a new memory
    Add {
        /// Memory kind: fact, decision, constraint
        #[arg(long, default_value = "fact")]
        kind: String,

        /// Content text (or provide via --file or stdin)
        #[arg(long)]
        text: Option<String>,

        /// Tags for categorization (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// File/artifact references (comma-separated)
        #[arg(long, value_delimiter = ',')]
        refs: Vec<String>,

        /// Severity for constraints: must, should, prefer
        #[arg(long)]
        severity: Option<String>,

        /// Scope: repo (default) or user
        #[arg(long)]
        scope: Option<String>,

        /// Why this memory exists (available on any kind)
        #[arg(long)]
        rationale: Option<String>,

        /// Alternatives considered (comma-separated)
        #[arg(long)]
        alternatives: Option<String>,

        /// Output format: text or json
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Recall relevant memories for a query
    Recall {
        /// Search query
        query: String,

        /// Maximum number of results
        #[arg(long, short = 'n', default_value_t = 3)]
        limit: usize,

        /// Skip the first N results (for pagination)
        #[arg(long, default_value_t = 0)]
        offset: usize,

        /// Filter by kind
        #[arg(long)]
        kind: Option<String>,

        /// Filter by tag(s) (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Filter by scope: repo or user
        #[arg(long)]
        scope: Option<String>,

        /// Output format: text, json, or context (XML for LLM)
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Update (supersede) an existing memory
    Update {
        /// ID of the memory to update
        id: String,

        /// New content text
        #[arg(long)]
        text: Option<String>,

        /// New tags (replaces all existing tags)
        #[arg(long, value_delimiter = ',')]
        tags: Option<Vec<String>>,

        /// New artifact refs (replaces all existing refs)
        #[arg(long, value_delimiter = ',')]
        refs: Option<Vec<String>>,

        /// Output format: text or json
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Delete a memory
    Forget {
        /// ID of the memory to delete
        id: String,
    },

    /// Show memory store status
    Status,

    /// Export all memories as JSON
    Export,

    /// Import memories from a JSON file
    Import {
        /// Path to JSON file
        file: std::path::PathBuf,
    },

    /// Install MCP configuration for an IDE
    McpInstall {
        /// Target: claude-code, vscode, cursor, windsurf, zed (auto-detected if omitted)
        #[arg(long)]
        ide: Option<String>,
    },
}

/// MCP subcommands.
#[derive(Subcommand)]
pub enum McpAction {
    /// Start the MCP server (stdio transport for IDE integration)
    Serve {
        /// Transport: stdio (default) — reads JSON-RPC from stdin, writes to stdout
        #[arg(long, default_value = "stdio")]
        transport: String,
    },
}

/// Server lifecycle subcommands.
#[derive(Subcommand)]
pub enum ServerAction {
    /// Run the server in the foreground (Ctrl-C to stop)
    Run {
        /// Listen address (e.g., "0.0.0.0:8090")
        #[arg(long)]
        listen_addr: Option<SocketAddr>,

        /// Storage path override
        #[arg(long)]
        storage_path: Option<PathBuf>,

        /// Path to a JSON-LD connection config file (S3, DynamoDB, etc.)
        #[arg(long)]
        connection_config: Option<PathBuf>,

        /// Log level (trace, debug, info, warn, error)
        #[arg(long)]
        log_level: Option<String>,

        /// Configuration profile to activate
        #[arg(long)]
        profile: Option<String>,

        /// Additional server arguments (passed through to server config)
        #[arg(last = true)]
        extra_args: Vec<String>,
    },

    /// Start the server as a background process
    Start {
        /// Listen address (e.g., "0.0.0.0:8090")
        #[arg(long)]
        listen_addr: Option<SocketAddr>,

        /// Storage path override
        #[arg(long)]
        storage_path: Option<PathBuf>,

        /// Path to a JSON-LD connection config file (S3, DynamoDB, etc.)
        #[arg(long)]
        connection_config: Option<PathBuf>,

        /// Log level (trace, debug, info, warn, error)
        #[arg(long)]
        log_level: Option<String>,

        /// Configuration profile to activate
        #[arg(long)]
        profile: Option<String>,

        /// Print resolved configuration without starting the server
        #[arg(long)]
        dry_run: bool,

        /// Additional server arguments (passed through to server config)
        #[arg(last = true)]
        extra_args: Vec<String>,
    },

    /// Stop a backgrounded server
    Stop {
        /// Force kill (SIGKILL) after timeout
        #[arg(long)]
        force: bool,
    },

    /// Show server status
    Status,

    /// Restart a backgrounded server
    Restart {
        /// Listen address (e.g., "0.0.0.0:8090")
        #[arg(long)]
        listen_addr: Option<SocketAddr>,

        /// Storage path override
        #[arg(long)]
        storage_path: Option<PathBuf>,

        /// Path to a JSON-LD connection config file (S3, DynamoDB, etc.)
        #[arg(long)]
        connection_config: Option<PathBuf>,

        /// Log level (trace, debug, info, warn, error)
        #[arg(long)]
        log_level: Option<String>,

        /// Configuration profile to activate
        #[arg(long)]
        profile: Option<String>,

        /// Additional server arguments (passed through to server config)
        #[arg(last = true)]
        extra_args: Vec<String>,
    },

    /// View server logs
    Logs {
        /// Follow log output (like tail -f)
        #[arg(long, short = 'f')]
        follow: bool,

        /// Number of lines to show (default: 50)
        #[arg(long, short = 'n', default_value_t = 50)]
        lines: usize,
    },

    /// Internal: child process entry point for background server.
    /// Do not invoke directly.
    #[command(hide = true)]
    Child {
        /// Serialized server arguments
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[derive(Subcommand)]
pub enum TrackAction {
    /// Start tracking a remote ledger
    Add {
        /// Ledger alias (local name for this tracked ledger)
        ledger: String,

        /// Remote name (e.g., "origin"); defaults to the only configured remote
        #[arg(long)]
        remote: Option<String>,

        /// Alias on the remote (defaults to local alias)
        #[arg(long)]
        remote_alias: Option<String>,
    },

    /// Stop tracking a remote ledger
    Remove {
        /// Ledger alias to stop tracking
        ledger: String,
    },

    /// List all tracked ledgers
    List,

    /// Show status of tracked ledger(s) from remote
    Status {
        /// Ledger alias (shows all if omitted)
        ledger: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Get a configuration value
    Get {
        /// Configuration key (e.g., "storage.path")
        key: String,
    },

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// Configuration value
        value: String,
    },

    /// List all configuration values
    List,

    /// Set origin configuration for a ledger (content origins for CID-based fetch)
    SetOrigins {
        /// Ledger name
        ledger: String,

        /// Path to origins config JSON file
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum ContextAction {
    /// Show the default JSON-LD context for a ledger
    Get {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
    },

    /// Set (replace) the default JSON-LD context for a ledger
    ///
    /// Accepts a JSON object mapping prefixes to IRIs, either inline or from a file.
    /// Examples:
    ///   fluree context set mydb '{"ex": "http://example.org/"}'
    ///   fluree context set mydb -f context.json
    Set {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Inline JSON context (prefix → IRI mappings)
        #[arg(long, short = 'e')]
        expr: Option<String>,

        /// Read context from a JSON file
        #[arg(long, short = 'f')]
        file: Option<std::path::PathBuf>,
    },
}

#[derive(Subcommand)]
pub enum PrefixAction {
    /// Add a prefix mapping (e.g., "ex" "http://example.org/")
    Add {
        /// Prefix (e.g., "ex")
        prefix: String,

        /// IRI namespace (e.g., "http://example.org/")
        iri: String,
    },

    /// Remove a prefix mapping
    Remove {
        /// Prefix to remove
        prefix: String,
    },

    /// List all prefix mappings
    List,
}

/// Arguments for `fluree token create` (extracted to reduce enum size).
#[derive(Debug, Clone, clap::Args)]
pub struct TokenCreateArgs {
    /// Ed25519 private key (hex with 0x prefix, base58btc, @filepath, or @- for stdin)
    #[arg(long, required = true)]
    pub private_key: String,

    /// Token lifetime (e.g., "1h", "30m", "7d", "1w") [default: 1h]
    #[arg(long, default_value = "1h")]
    pub expires_in: String,

    /// Subject claim (sub) - identity of the token holder
    #[arg(long)]
    pub subject: Option<String>,

    /// Audience claim (aud) - repeatable for multiple audiences
    #[arg(long = "audience")]
    pub audiences: Vec<String>,

    /// Fluree identity claim (fluree.identity) - takes precedence over sub for policy
    #[arg(long)]
    pub identity: Option<String>,

    /// Grant access to all ledgers (fluree.events.all=true, fluree.storage.all=true)
    #[arg(long)]
    pub all: bool,

    /// Grant events access to specific ledger (repeatable)
    #[arg(long = "events-ledger")]
    pub events_ledgers: Vec<String>,

    /// Grant storage access to specific ledger (repeatable)
    #[arg(long = "storage-ledger")]
    pub storage_ledgers: Vec<String>,

    /// Grant data API read access to all ledgers (fluree.ledger.read.all=true)
    #[arg(long)]
    pub read_all: bool,

    /// Grant data API read access to a specific ledger (repeatable)
    #[arg(long = "read-ledger")]
    pub read_ledgers: Vec<String>,

    /// Grant data API write access to all ledgers (fluree.ledger.write.all=true)
    #[arg(long)]
    pub write_all: bool,

    /// Grant data API write access to a specific ledger (repeatable)
    #[arg(long = "write-ledger")]
    pub write_ledgers: Vec<String>,

    /// Grant access to specific graph source (repeatable)
    #[arg(long = "graph-source")]
    pub graph_sources: Vec<String>,

    /// Output format
    #[arg(long, default_value = "token", value_enum)]
    pub output: TokenOutputFormat,

    /// Print decoded claims to stderr (for verification)
    #[arg(long)]
    pub print_claims: bool,
}

#[derive(Subcommand)]
pub enum TokenAction {
    /// Create a new JWS token for authentication
    Create(Box<TokenCreateArgs>),

    /// Generate a new Ed25519 keypair
    Keygen {
        /// Output format for the keypair
        #[arg(long, default_value = "hex", value_enum)]
        format: KeyFormat,

        /// Write private key to file (otherwise prints to stdout)
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
    },

    /// Inspect (decode and verify) a JWS token
    Inspect {
        /// JWS token string or @filepath
        token: String,

        /// Skip signature verification
        #[arg(long)]
        no_verify: bool,

        /// Output format
        #[arg(long, default_value = "pretty", value_enum)]
        output: InspectOutputFormat,
    },
}

/// Config file format for `fluree init`.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InitFormat {
    /// TOML format (default)
    Toml,
    /// JSON-LD format with @context
    Jsonld,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TokenOutputFormat {
    /// Just the JWS token string
    Token,
    /// JSON object with token and decoded claims
    Json,
    /// Ready-to-use curl command for events endpoint
    Curl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KeyFormat {
    /// Hex with 0x prefix (64 chars)
    Hex,
    /// Base58btc with z prefix (multibase)
    Base58,
    /// JSON object with hex, base58, and did:key
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InspectOutputFormat {
    /// Human-readable formatted output
    Pretty,
    /// Raw JSON
    Json,
    /// Table format for claims
    Table,
}

#[derive(Subcommand)]
pub enum RemoteAction {
    /// Add a remote server
    Add {
        /// Remote name (e.g., "origin")
        name: String,

        /// Server URL (e.g., "http://localhost:8090")
        url: String,

        /// Authentication token (or @filepath to read from file)
        #[arg(long)]
        token: Option<String>,
    },

    /// Remove a remote
    Remove {
        /// Remote name to remove
        name: String,
    },

    /// List all remotes
    List,

    /// Show details for a remote
    Show {
        /// Remote name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum AuthAction {
    /// Show authentication status for a remote
    Status {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,
    },

    /// Store a bearer token for a remote
    Login {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,

        /// Token value, @filepath to read from file, or @- for stdin
        #[arg(long)]
        token: Option<String>,
    },

    /// Clear the stored token for a remote
    Logout {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum UpstreamAction {
    /// Set upstream tracking for a ledger
    Set {
        /// Local ledger alias (e.g., "mydb" or "mydb:main")
        local: String,

        /// Remote name (e.g., "origin")
        remote: String,

        /// Remote ledger alias (defaults to local alias)
        #[arg(long)]
        remote_alias: Option<String>,

        /// Automatically pull on fetch
        #[arg(long)]
        auto_pull: bool,
    },

    /// Remove upstream tracking for a ledger
    Remove {
        /// Local ledger alias
        local: String,
    },

    /// List all upstream configurations
    List,
}

// =============================================================================
// Iceberg subcommands
// =============================================================================

/// Iceberg subcommands.
#[derive(Subcommand)]
pub enum IcebergAction {
    /// Map an Iceberg table as a graph source
    ///
    /// Examples:
    ///   fluree iceberg map my-gs --catalog-uri https://polaris.example.com --table openflights.airlines
    ///   fluree iceberg map my-gs --catalog-uri https://... --r2rml mappings/airlines.ttl
    ///   fluree iceberg map my-gs --mode direct --table-location s3://bucket/warehouse/ns/table
    Map(Box<IcebergMapArgs>),

    /// List Iceberg-family graph sources (Iceberg and R2RML mappings)
    List {
        /// List graph sources on a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Show details for an Iceberg-family graph source
    Info {
        /// Graph source name
        name: String,

        /// Query a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Drop an Iceberg-family graph source
    Drop {
        /// Graph source name
        name: String,

        /// Required flag to confirm deletion
        #[arg(long)]
        force: bool,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },
}

/// Arguments for mapping an Iceberg table as a graph source.
#[derive(Debug, Clone, clap::Args)]
pub struct IcebergMapArgs {
    /// Graph source name (e.g., "my-iceberg-gs")
    pub name: String,

    /// Execute against a remote server (by remote name, e.g., "origin")
    #[arg(long)]
    pub remote: Option<String>,

    /// Catalog mode: "rest" (default) or "direct"
    #[arg(long, default_value = "rest")]
    pub mode: String,

    /// REST catalog URI (required for rest mode)
    #[arg(long)]
    pub catalog_uri: Option<String>,

    /// Table identifier in namespace.table format (e.g., "openflights.airlines").
    /// Required for rest mode without --r2rml. When using --r2rml, tables are
    /// defined in the mapping file.
    #[arg(long)]
    pub table: Option<String>,

    /// S3 table location for direct mode (e.g., "s3://bucket/warehouse/ns/table")
    #[arg(long)]
    pub table_location: Option<String>,

    /// R2RML mapping file (Turtle format). Defines how Iceberg table rows
    /// are mapped to RDF triples. When provided, table references come from
    /// the mapping's rr:tableName entries.
    #[arg(long)]
    pub r2rml: Option<PathBuf>,

    /// R2RML mapping media type (e.g., "text/turtle"); inferred from extension if omitted
    #[arg(long)]
    pub r2rml_type: Option<String>,

    /// Branch name (defaults to "main")
    #[arg(long)]
    pub branch: Option<String>,

    /// Bearer token for REST catalog authentication
    #[arg(long)]
    pub auth_bearer: Option<String>,

    /// OAuth2 token URL for client credentials auth
    #[arg(long)]
    pub oauth2_token_url: Option<String>,

    /// OAuth2 client ID
    #[arg(long)]
    pub oauth2_client_id: Option<String>,

    /// OAuth2 client secret
    #[arg(long)]
    pub oauth2_client_secret: Option<String>,

    /// Warehouse identifier (REST mode)
    #[arg(long)]
    pub warehouse: Option<String>,

    /// Disable vended credentials (REST mode, enabled by default)
    #[arg(long)]
    pub no_vended_credentials: bool,

    /// S3 region override
    #[arg(long)]
    pub s3_region: Option<String>,

    /// S3 endpoint override (for MinIO, LocalStack)
    #[arg(long)]
    pub s3_endpoint: Option<String>,

    /// Use path-style S3 URLs
    #[arg(long)]
    pub s3_path_style: bool,
}
