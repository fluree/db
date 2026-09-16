//! Ledger ID parsing and normalization utilities.
//!
//! Centralizes default-branch handling and time-travel parsing so all
//! callers use consistent rules.

use std::fmt;

/// Default branch name used when none is specified.
pub const DEFAULT_BRANCH: &str = "main";

/// Time-travel specification parsed from a ledger ID suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerIdTimeSpec {
    /// @t:<transaction>
    AtT(i64),
    /// @iso:<timestamp> — resolves against commit *event time* (`db:time`)
    AtIso(String),
    /// @commit:<cid>
    AtCommit(String),
    /// @recorded:<timestamp> — resolves against the wall-clock time commits
    /// were recorded (`db:receivedAt`, audit axis). Identical to `@iso:` on
    /// ledgers that never used caller-supplied event times.
    AtRecorded(String),
}

/// Parsed ledger ID parts with optional time-travel spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLedgerId {
    pub name: String,
    pub branch: String,
    pub time: Option<LedgerIdTimeSpec>,
}

/// Error returned when ledger ID parsing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerIdParseError {
    message: String,
}

impl LedgerIdParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LedgerIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LedgerIdParseError {}

/// Split a `name[:branch]` ledger ID into (name, branch), applying the default branch.
pub fn split_ledger_id(ledger_id: &str) -> Result<(String, String), LedgerIdParseError> {
    let parts: Vec<&str> = ledger_id.splitn(2, ':').collect();

    match parts.as_slice() {
        [name] if !name.is_empty() => Ok((name.to_string(), DEFAULT_BRANCH.to_string())),
        [name, branch] if !name.is_empty() && !branch.is_empty() => {
            // Reject multiple colons (e.g., "name:branch:extra") - branch must not contain ':'.
            if branch.contains(':') {
                return Err(LedgerIdParseError::new(format!(
                    "Invalid ledger ID format '{ledger_id}': expected 'name' or 'name:branch'"
                )));
            }
            Ok((name.to_string(), branch.to_string()))
        }
        _ => Err(LedgerIdParseError::new(format!(
            "Invalid ledger ID format '{ledger_id}': expected 'name' or 'name:branch'"
        ))),
    }
}

/// Normalize a ledger ID to `name:branch` form using the default branch.
pub fn normalize_ledger_id(ledger_id: &str) -> Result<String, LedgerIdParseError> {
    let (name, branch) = split_ledger_id(ledger_id)?;
    Ok(format_ledger_id(&name, &branch))
}

/// Format a canonical `name:branch` ledger ID string.
pub fn format_ledger_id(name: &str, branch: &str) -> String {
    format!("{name}:{branch}")
}

/// Parse a ledger ID with optional `@t:`, `@iso:`, `@recorded:`, or `@commit:` time-travel suffix.
pub fn parse_ledger_id_with_time(ledger_id: &str) -> Result<ParsedLedgerId, LedgerIdParseError> {
    let (base, time) = split_time_travel_suffix(ledger_id)?;

    let (name, branch) = split_ledger_id(&base)?;

    Ok(ParsedLedgerId { name, branch, time })
}

/// Shortest commit hex-digest prefix any surface will accept.
///
/// The rule belongs to the prefix *scan* — below this a prefix stops being
/// selective — so `fluree_db_api::ledger_view::normalize_commit_ref` is its
/// authority and applies it after stripping `fluree:commit:` / `sha256:` and
/// decoding canonical CIDs. It lives here because the address grammar
/// ([`parse_time_travel_spec`]) has to reject `@commit:abc` without a ledger in
/// hand, and `fluree-db-api` cannot be depended on from this crate. Re-exported
/// as `fluree_db_api::COMMIT_PREFIX_MIN_LEN`, which is where callers should
/// reach for it.
pub const COMMIT_PREFIX_MIN_LEN: usize = 6;

/// The tags [`parse_time_travel_spec`] recognises, in the order it tries them.
///
/// Exposed so surfaces that layer their own spellings on top of this grammar —
/// `fluree_db_api::TimeSpec::parse_at`, which also accepts a bare integer and a
/// bare ISO-8601 timestamp — can tell "the user reached for a canonical tag and
/// got it wrong" apart from "the user typed one of the bare forms".
pub const TIME_TRAVEL_TAGS: [&str; 4] = ["t:", "iso:", "commit:", "recorded:"];

/// Parse a time-travel spec: the part of a ledger address after `@`, or a bare
/// spec such as a CLI `--at` argument.
///
/// Accepts `t:<N>`, `iso:<timestamp>`, `recorded:<timestamp>` and
/// `commit:<prefix>` (at least [`COMMIT_PREFIX_MIN_LEN`] characters). `t:latest` is
/// deliberately *not*
/// accepted: [`LedgerIdTimeSpec`] has no "latest" variant because resolving one
/// needs the ledger's current `t`, which this layer does not have. Callers that
/// support it (`fluree_db_api::TimeSpec::parse`) take it before delegating here.
///
/// `sigil` is what the calling surface writes in front of a tag when it quotes
/// one back to the user: `"@"` for a ledger address, `""` for a bare spec. It
/// reaches error text only, so `mydb@t:` and `--at t:` each report the spelling
/// that was actually typed rather than the other surface's.
pub fn parse_time_travel_spec(
    spec: &str,
    sigil: &str,
) -> Result<LedgerIdTimeSpec, LedgerIdParseError> {
    if let Some(val) = spec.strip_prefix("t:") {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}t:'"
            )));
        }
        let t: i64 = val.parse().map_err(|_| {
            LedgerIdParseError::new(format!("Invalid integer for {sigil}t: '{val}'"))
        })?;
        Ok(LedgerIdTimeSpec::AtT(t))
    } else if let Some(val) = spec.strip_prefix("iso:") {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}iso:'"
            )));
        }
        Ok(LedgerIdTimeSpec::AtIso(val.to_string()))
    } else if let Some(val) = spec.strip_prefix("commit:") {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}commit:'"
            )));
        }
        if val.len() < COMMIT_PREFIX_MIN_LEN {
            return Err(LedgerIdParseError::new(format!(
                "Commit prefix must be at least {COMMIT_PREFIX_MIN_LEN} characters"
            )));
        }
        Ok(LedgerIdTimeSpec::AtCommit(val.to_string()))
    } else if let Some(val) = spec.strip_prefix("recorded:") {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}recorded:'"
            )));
        }
        Ok(LedgerIdTimeSpec::AtRecorded(val.to_string()))
    } else {
        Err(LedgerIdParseError::new(format!(
            "Invalid time travel format: '{spec}'. Expected {sigil}t:, {sigil}iso:, {sigil}recorded:, or {sigil}commit: prefix"
        )))
    }
}

/// Split a ledger ID string into its base and optional time-travel suffix.
///
/// This does not interpret `:`; it only handles `@t:`, `@iso:`, `@recorded:`, and `@commit:`.
pub fn split_time_travel_suffix(
    ledger_id: &str,
) -> Result<(String, Option<LedgerIdTimeSpec>), LedgerIdParseError> {
    if let Some(at_idx) = ledger_id.find('@') {
        let base = &ledger_id[..at_idx];
        let time_str = &ledger_id[at_idx + 1..];

        if base.is_empty() {
            return Err(LedgerIdParseError::new(
                "Ledger ID cannot be empty before '@'".to_string(),
            ));
        }

        let time = parse_time_travel_spec(time_str, "@")?;

        Ok((base.to_string(), Some(time)))
    } else {
        Ok((ledger_id.to_string(), None))
    }
}

/// Maximum allowed length for a branch name.
const MAX_BRANCH_NAME_LEN: usize = 128;

/// Validate a branch name for use in `create_branch`.
///
/// Branch names must:
/// - Not be empty or purely whitespace
/// - Not contain `:` (reserved as ledger ID separator)
/// - Not contain `@` (reserved for time-travel suffixes)
/// - Not contain null bytes
/// - Not be or contain `..` (path traversal)
/// - Be at most 128 characters
pub fn validate_branch_name(name: &str) -> Result<(), LedgerIdParseError> {
    if name.is_empty() || name.trim().is_empty() {
        return Err(LedgerIdParseError::new("Branch name cannot be empty"));
    }
    if name.len() > MAX_BRANCH_NAME_LEN {
        return Err(LedgerIdParseError::new(format!(
            "Branch name exceeds maximum length of {MAX_BRANCH_NAME_LEN} characters"
        )));
    }
    if name.contains(':') {
        return Err(LedgerIdParseError::new("Branch name cannot contain ':'"));
    }
    if name.contains('@') {
        return Err(LedgerIdParseError::new("Branch name cannot contain '@'"));
    }
    if name.contains('\0') {
        return Err(LedgerIdParseError::new(
            "Branch name cannot contain null bytes",
        ));
    }
    if name == ".." || name.contains("../") || name.contains("/..") {
        return Err(LedgerIdParseError::new(
            "Branch name cannot contain path traversal (..)",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ledger_id_with_branch() {
        let (name, branch) = split_ledger_id("mydb:main").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_split_ledger_id_without_branch() {
        let (name, branch) = split_ledger_id("mydb").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, DEFAULT_BRANCH);
    }

    #[test]
    fn test_parse_ledger_id_with_time_t() {
        let parsed = parse_ledger_id_with_time("ledger:main@t:42").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, "main");
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtT(42))));
    }

    #[test]
    fn test_parse_ledger_id_with_time_iso() {
        let parsed = parse_ledger_id_with_time("ledger@iso:2025-01-01T00:00:00Z").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtIso(_))));
    }

    #[test]
    fn test_parse_ledger_id_with_time_commit() {
        let parsed = parse_ledger_id_with_time("ledger@commit:abc123").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtCommit(_))));
    }

    #[test]
    fn test_validate_branch_name_valid() {
        assert!(validate_branch_name("dev").is_ok());
        assert!(validate_branch_name("feature-x").is_ok());
        assert!(validate_branch_name("release/v1.0").is_ok());
        assert!(validate_branch_name("a").is_ok());
    }

    #[test]
    fn test_validate_branch_name_empty() {
        assert!(validate_branch_name("").is_err());
        assert!(validate_branch_name("   ").is_err());
    }

    #[test]
    fn test_validate_branch_name_colon() {
        assert!(validate_branch_name("foo:bar").is_err());
    }

    #[test]
    fn test_validate_branch_name_at_sign() {
        assert!(validate_branch_name("foo@bar").is_err());
    }

    #[test]
    fn test_validate_branch_name_path_traversal() {
        assert!(validate_branch_name("..").is_err());
        assert!(validate_branch_name("../etc").is_err());
        assert!(validate_branch_name("foo/..").is_err());
    }

    #[test]
    fn test_validate_branch_name_too_long() {
        let long = "a".repeat(MAX_BRANCH_NAME_LEN + 1);
        assert!(validate_branch_name(&long).is_err());
        let ok = "a".repeat(MAX_BRANCH_NAME_LEN);
        assert!(validate_branch_name(&ok).is_ok());
    }
}
