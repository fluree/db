//! Ledger ID parsing and normalization.
//!
//! One grammar for every surface:
//!
//! ```text
//! [urn:fluree:]name[:branch][@<tag>:<spec>][#fragment]
//! ```
//!
//! Parse user input once, at the edge, with [`LedgerRef::parse`] (full address)
//! or [`LedgerId::parse`] (bare id). Everything past the edge carries a
//! [`LedgerId`], which is always canonical `name:branch`. `LedgerId` derefs to
//! `str` for reading but deliberately does not implement `Borrow<str>`: a map
//! keyed by `LedgerId` cannot be probed with a raw string, so a missed
//! normalization is a compile error rather than a silent cache miss.
//!
//! Seams that still receive ids as strings check them with
//! [`LedgerId::expect_canonical`], whose error names the seam.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Default branch name used when none is specified.
pub const DEFAULT_BRANCH: &str = "main";

/// Prefix accepted in front of a ledger address (`urn:fluree:mydb:main`).
pub const LEDGER_URN_PREFIX: &str = "urn:fluree:";

/// Time-travel specification parsed from a ledger ID suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerIdTimeSpec {
    /// @t:<transaction>
    AtT(i64),
    /// @time:<timestamp> (alias `@iso:`) — resolves against commit *event
    /// time* (`db:time`)
    AtIso(String),
    /// @commit:<cid>
    AtCommit(String),
    /// @recorded:<timestamp> — resolves against the wall-clock time commits
    /// were recorded (`db:receivedAt`, audit axis). Identical to `@time:` on
    /// ledgers that never used caller-supplied event times.
    AtRecorded(String),
    /// @snapshot:<id> — a table format's own snapshot identifier. Only a graph
    /// source backed by a snapshotted table (Iceberg) can resolve it; a native
    /// ledger has no such identifier and rejects it.
    AtSnapshot(i64),
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

/// Characters that delimit parts of a ledger address and so can never appear
/// inside a name or branch.
const RESERVED: [(char, &str); 4] = [
    ('@', "'@' starts a time-travel suffix"),
    ('#', "'#' starts a graph fragment"),
    (':', "':' separates the branch"),
    ('\0', "null bytes are not allowed"),
];

fn check_reserved(part: &str, what: &str, input: &str) -> Result<(), LedgerIdParseError> {
    for (c, why) in RESERVED {
        if part.contains(c) {
            return Err(LedgerIdParseError::new(format!(
                "Invalid ledger id '{input}': {what} cannot contain '{}' ({why})",
                c.escape_default()
            )));
        }
    }
    Ok(())
}

fn validate_name_part(name: &str, input: &str) -> Result<(), LedgerIdParseError> {
    if name.trim().is_empty() {
        return Err(LedgerIdParseError::new(format!(
            "Invalid ledger id '{input}': ledger name cannot be empty"
        )));
    }
    check_reserved(name, "ledger name", input)?;
    for segment in name.split('/') {
        if segment.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Invalid ledger id '{input}': ledger name cannot have empty '/' segments"
            )));
        }
        if segment == "." || segment == ".." {
            return Err(LedgerIdParseError::new(format!(
                "Invalid ledger id '{input}': ledger name cannot contain '.' or '..' segments"
            )));
        }
    }
    Ok(())
}

/// Rules every branch name must satisfy to be *read*. Creating a branch also
/// applies [`validate_branch_name`], which is stricter.
fn validate_branch_part(branch: &str, input: &str) -> Result<(), LedgerIdParseError> {
    if branch.trim().is_empty() {
        return Err(LedgerIdParseError::new(format!(
            "Invalid ledger id '{input}': branch cannot be empty"
        )));
    }
    check_reserved(branch, "branch", input)?;
    if branch == ".." || branch.contains("../") || branch.contains("/..") {
        return Err(LedgerIdParseError::new(format!(
            "Invalid ledger id '{input}': branch cannot contain path traversal (..)"
        )));
    }
    Ok(())
}

/// A validated ledger name — the part of a ledger id before `:`.
///
/// Whole-ledger operations (hard drop, sweep, the cross-branch `@shared`
/// namespace) take a `LedgerName` so a branch-qualified id cannot reach them.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LedgerName(Arc<str>);

impl LedgerName {
    /// Parse a bare ledger name. A `:branch` suffix is rejected: whole-ledger
    /// operations must not silently drop the branch the caller wrote.
    pub fn parse(input: &str) -> Result<Self, LedgerIdParseError> {
        if input.contains(':') {
            return Err(LedgerIdParseError::new(format!(
                "Invalid ledger name '{input}': expected a ledger name without ':branch'"
            )));
        }
        validate_name_part(input, input)?;
        Ok(Self(input.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The id of `branch` under this ledger.
    pub fn with_branch(&self, branch: &str) -> Result<LedgerId, LedgerIdParseError> {
        LedgerId::from_parts(&self.0, branch)
    }

    /// Storage path prefix for content shared across all branches.
    pub fn shared_prefix(&self) -> String {
        format!("{}/{}", self.0, crate::address_path::SHARED_NAMESPACE)
    }
}

impl std::ops::Deref for LedgerName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LedgerName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LedgerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for LedgerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

/// A canonical `name:branch` ledger id.
///
/// Also used for graph-source ids, which share the grammar.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LedgerId {
    full: Arc<str>,
    sep: usize,
}

impl LedgerId {
    /// Parse `name` or `name:branch`, applying [`DEFAULT_BRANCH`].
    ///
    /// Rejects time-travel suffixes and graph fragments; parse a full address
    /// with [`LedgerRef::parse`].
    pub fn parse(input: &str) -> Result<Self, LedgerIdParseError> {
        match input.split_once(':') {
            Some((name, branch)) => {
                validate_name_part(name, input)?;
                validate_branch_part(branch, input)?;
                Ok(Self::new_unchecked(name, branch))
            }
            None => {
                validate_name_part(input, input)?;
                Ok(Self::new_unchecked(input, DEFAULT_BRANCH))
            }
        }
    }

    /// Build from separate name and branch.
    pub fn from_parts(name: &str, branch: &str) -> Result<Self, LedgerIdParseError> {
        let input = format!("{name}:{branch}");
        validate_name_part(name, &input)?;
        validate_branch_part(branch, &input)?;
        Ok(Self::new_unchecked(name, branch))
    }

    /// Accept only an already-canonical `name:branch` string.
    ///
    /// For seams that receive ids as strings from code that should already
    /// have parsed them. `seam` names the receiving site so the error points
    /// at the path that skipped normalization.
    pub fn expect_canonical(input: &str, seam: &str) -> Result<Self, LedgerIdParseError> {
        if !input.contains(':') {
            return Err(LedgerIdParseError::new(format!(
                "Non-canonical ledger id '{input}' reached {seam}: expected 'name:branch'. \
                 Parse ids at the edge with LedgerId::parse / LedgerRef::parse"
            )));
        }
        Self::parse(input).map_err(|e| {
            LedgerIdParseError::new(format!("{e} (reached {seam} without edge validation)"))
        })
    }

    fn new_unchecked(name: &str, branch: &str) -> Self {
        let full: Arc<str> = format!("{name}:{branch}").into();
        Self {
            full,
            sep: name.len(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.full
    }

    pub fn name(&self) -> &str {
        &self.full[..self.sep]
    }

    pub fn branch(&self) -> &str {
        &self.full[self.sep + 1..]
    }

    pub fn ledger_name(&self) -> LedgerName {
        LedgerName(self.name().into())
    }

    /// Sibling id on another branch of the same ledger.
    pub fn with_branch(&self, branch: &str) -> Result<Self, LedgerIdParseError> {
        Self::from_parts(self.name(), branch)
    }

    /// Portable storage path prefix `name/branch` (no `:` in storage paths).
    pub fn path_prefix(&self) -> String {
        format!("{}/{}", self.name(), self.branch())
    }

    /// Storage path prefix for content shared across all branches.
    pub fn shared_prefix(&self) -> String {
        format!("{}/{}", self.name(), crate::address_path::SHARED_NAMESPACE)
    }
}

impl std::ops::Deref for LedgerId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.full
    }
}

impl AsRef<str> for LedgerId {
    fn as_ref(&self) -> &str {
        &self.full
    }
}

impl fmt::Display for LedgerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.full)
    }
}

impl fmt::Debug for LedgerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.full, f)
    }
}

impl FromStr for LedgerId {
    type Err = LedgerIdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl PartialEq<str> for LedgerId {
    fn eq(&self, other: &str) -> bool {
        &*self.full == other
    }
}

impl PartialEq<&str> for LedgerId {
    fn eq(&self, other: &&str) -> bool {
        &*self.full == *other
    }
}

impl PartialEq<String> for LedgerId {
    fn eq(&self, other: &String) -> bool {
        &*self.full == other.as_str()
    }
}

impl PartialEq<LedgerId> for str {
    fn eq(&self, other: &LedgerId) -> bool {
        self == &*other.full
    }
}

impl PartialEq<LedgerId> for &str {
    fn eq(&self, other: &LedgerId) -> bool {
        *self == &*other.full
    }
}

impl PartialEq<LedgerId> for String {
    fn eq(&self, other: &LedgerId) -> bool {
        self.as_str() == &*other.full
    }
}

impl From<LedgerId> for String {
    fn from(id: LedgerId) -> String {
        id.full.to_string()
    }
}

impl From<&LedgerId> for Arc<str> {
    fn from(id: &LedgerId) -> Arc<str> {
        Arc::clone(&id.full)
    }
}

impl From<&LedgerId> for String {
    fn from(id: &LedgerId) -> String {
        id.full.to_string()
    }
}

impl Serialize for LedgerId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.full)
    }
}

/// Lenient: persisted records written before canonicalization may carry a
/// branchless id, which gets the default branch.
impl<'de> Deserialize<'de> for LedgerId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Argument conversion for constructors that take a ledger id.
///
/// A `LedgerId` passes through. A `&str` is for already-canonical literals
/// (fixtures, constants): it trips in debug builds when not canonical and
/// panics when it does not parse at all, since an unparseable id cannot have
/// been created.
pub trait IntoLedgerId {
    fn into_ledger_id(self) -> LedgerId;
}

impl IntoLedgerId for LedgerId {
    fn into_ledger_id(self) -> LedgerId {
        self
    }
}

impl IntoLedgerId for &LedgerId {
    fn into_ledger_id(self) -> LedgerId {
        self.clone()
    }
}

impl IntoLedgerId for &str {
    fn into_ledger_id(self) -> LedgerId {
        crate::address_path::storage_ledger_id(self, "a ledger id argument")
            .unwrap_or_else(|e| panic!("{e}"))
    }
}

impl IntoLedgerId for &String {
    fn into_ledger_id(self) -> LedgerId {
        self.as_str().into_ledger_id()
    }
}

/// A full ledger address: id plus optional time-travel suffix and fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRef {
    pub id: LedgerId,
    /// Raw spec after `@` (`t:5`, `time:…`). Each layer interprets it with its
    /// own grammar; [`LedgerRef::time_spec`] applies the core one.
    pub at: Option<String>,
    /// Graph selector after `#` (`txn-meta`, `config`, or an IRI).
    pub fragment: Option<String>,
}

impl LedgerRef {
    /// Parse `[urn:fluree:]name[:branch][@spec][#fragment]`.
    ///
    /// Because names and branches cannot contain `@` or `#`, the first `#`
    /// always starts the fragment and the first `@` before it always starts
    /// the time spec.
    pub fn parse(input: &str) -> Result<Self, LedgerIdParseError> {
        let body = input.strip_prefix(LEDGER_URN_PREFIX).unwrap_or(input);
        let (before_fragment, fragment) = match body.split_once('#') {
            Some((_, "")) => {
                return Err(LedgerIdParseError::new(format!(
                    "Invalid ledger address '{input}': missing graph after '#'"
                )))
            }
            Some((left, right)) => (left, Some(right.to_string())),
            None => (body, None),
        };
        let (base, at) = match before_fragment.split_once('@') {
            Some(("", _)) => {
                return Err(LedgerIdParseError::new(format!(
                    "Invalid ledger address '{input}': ledger id cannot be empty before '@'"
                )))
            }
            Some((_, "")) => {
                return Err(LedgerIdParseError::new(format!(
                    "Invalid ledger address '{input}': missing time spec after '@'"
                )))
            }
            Some((base, spec)) => (base, Some(spec.to_string())),
            None => (before_fragment, None),
        };
        Ok(Self {
            id: LedgerId::parse(base)?,
            at,
            fragment,
        })
    }

    /// Interpret the `@` suffix with the core time-travel grammar.
    pub fn time_spec(&self) -> Result<Option<LedgerIdTimeSpec>, LedgerIdParseError> {
        self.at
            .as_deref()
            .map(|spec| parse_time_travel_spec(spec, "@"))
            .transpose()
    }
}

/// Split a `name[:branch]` ledger ID into (name, branch), applying the default branch.
pub fn split_ledger_id(ledger_id: &str) -> Result<(String, String), LedgerIdParseError> {
    let id = LedgerId::parse(ledger_id)?;
    Ok((id.name().to_string(), id.branch().to_string()))
}

/// Normalize a ledger ID to `name:branch` form using the default branch.
pub fn normalize_ledger_id(ledger_id: &str) -> Result<String, LedgerIdParseError> {
    Ok(LedgerId::parse(ledger_id)?.to_string())
}

/// Format a canonical `name:branch` ledger ID string from already-validated parts.
pub fn format_ledger_id(name: &str, branch: &str) -> String {
    format!("{name}:{branch}")
}

/// Parse a ledger ID with optional `@t:`, `@time:`, `@recorded:`, `@commit:`, or
/// `@snapshot:` time-travel suffix.
pub fn parse_ledger_id_with_time(ledger_id: &str) -> Result<ParsedLedgerId, LedgerIdParseError> {
    let (base, time) = split_time_travel_suffix(ledger_id)?;
    let id = LedgerId::parse(&base)?;
    Ok(ParsedLedgerId {
        name: id.name().to_string(),
        branch: id.branch().to_string(),
        time,
    })
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
pub const TIME_TRAVEL_TAGS: [&str; 6] =
    ["t:", "time:", "iso:", "commit:", "recorded:", "snapshot:"];

/// Parse a time-travel spec: the part of a ledger address after `@`, or a bare
/// spec such as a CLI `--at` argument.
///
/// Accepts `t:<N>`, `time:<timestamp>`, `recorded:<timestamp>`,
/// `commit:<prefix>` (at least [`COMMIT_PREFIX_MIN_LEN`] characters) and
/// `snapshot:<id>`. `iso:<timestamp>` is the original spelling of `time:` and
/// stays accepted as an alias. `t:latest` is deliberately *not*
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
    } else if let Some((tag, val)) = ["time:", "iso:"]
        .into_iter()
        .find_map(|tag| spec.strip_prefix(tag).map(|val| (tag, val)))
    {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}{tag}'"
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
    } else if let Some(val) = spec.strip_prefix("snapshot:") {
        if val.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Missing value after '{sigil}snapshot:'"
            )));
        }
        let id: i64 = val.parse().map_err(|_| {
            LedgerIdParseError::new(format!("Invalid integer for {sigil}snapshot: '{val}'"))
        })?;
        Ok(LedgerIdTimeSpec::AtSnapshot(id))
    } else {
        Err(LedgerIdParseError::new(format!(
            "Invalid time travel format: '{spec}'. Expected {sigil}t:, {sigil}time:, {sigil}recorded:, {sigil}commit:, or {sigil}snapshot: prefix"
        )))
    }
}

/// Split a ledger ID string into its base and optional time-travel suffix.
///
/// This does not interpret `:`; it only handles `@t:`, `@time:` (alias `@iso:`),
/// `@recorded:`, `@commit:`, and `@snapshot:`.
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

/// Directory names the storage layout puts directly under `name/branch/`
/// (plus the generic `blob/`). A branch or ledger-name segment spelled like one
/// would place one ledger's files inside another's destructive prefix:
/// ledger `x/main` branch `index` lives at `x/main/index/`, which is
/// `x:main`'s index directory.
const RESERVED_LAYOUT_SEGMENTS: [&str; 5] = ["commit", "txn", "index", "config", "blob"];

/// Validate a ledger name for creating a new ledger.
///
/// Stricter than parsing: existing ledgers whose names break these rules stay
/// readable, but no new one can be created.
pub fn validate_ledger_name(name: &str) -> Result<(), LedgerIdParseError> {
    LedgerName::parse(name)?;
    if let Some(seg) = name
        .split('/')
        .find(|seg| RESERVED_LAYOUT_SEGMENTS.contains(seg))
    {
        return Err(LedgerIdParseError::new(format!(
            "Invalid ledger name '{name}': segment '{seg}' is reserved by the storage layout"
        )));
    }
    if name.split('/').next() == Some(crate::storage::GRAPH_SOURCES_PATH_SEGMENT) {
        return Err(LedgerIdParseError::new(format!(
            "Invalid ledger name '{name}': '{}' is reserved for graph-source storage",
            crate::storage::GRAPH_SOURCES_PATH_SEGMENT
        )));
    }
    Ok(())
}

/// Maximum allowed length for a branch name.
const MAX_BRANCH_NAME_LEN: usize = 128;

/// Validate a branch name for use in `create_branch`.
///
/// Branch names must:
/// - Not be empty or purely whitespace
/// - Not contain `:` (reserved as ledger ID separator)
/// - Not contain `@` (reserved for time-travel suffixes)
/// - Not contain `#` (reserved for graph fragments)
/// - Not contain `/`: storage paths are `name/branch`, and ledger names may
///   contain `/`, so `mydb:release/v1` and `mydb/release:v1` would share one
///   path. Existing branches with `/` stay readable.
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
    if name.contains('#') {
        return Err(LedgerIdParseError::new("Branch name cannot contain '#'"));
    }
    if name.contains('/') {
        return Err(LedgerIdParseError::new("Branch name cannot contain '/'"));
    }
    // `ns@v2/{name}/{branch}.index.json` is the index sidecar of branch
    // `{branch}`; a branch named `main.index` would share it with `main`.
    if name.ends_with(".index") || name.ends_with(".snapshots") {
        return Err(LedgerIdParseError::new(
            "Branch name cannot end with '.index' or '.snapshots' (reserved for nameservice sidecar records)",
        ));
    }
    if RESERVED_LAYOUT_SEGMENTS.contains(&name) {
        return Err(LedgerIdParseError::new(format!(
            "Branch name '{name}' is reserved by the storage layout"
        )));
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

    /// `time:` is the canonical spelling; `iso:` is its alias. Both reach the
    /// same value, and each reports its own tag when the value is missing.
    #[test]
    fn time_and_iso_tags_are_one_spec() {
        assert_eq!(
            parse_time_travel_spec("time:2025-01-01T00:00:00Z", "@").unwrap(),
            parse_time_travel_spec("iso:2025-01-01T00:00:00Z", "@").unwrap(),
        );
        assert_eq!(
            parse_time_travel_spec("time:2025-01-01T00:00:00Z", "@").unwrap(),
            LedgerIdTimeSpec::AtIso("2025-01-01T00:00:00Z".to_string()),
        );
        for tag in ["time:", "iso:"] {
            let err = parse_time_travel_spec(tag, "@").unwrap_err().to_string();
            assert_eq!(err, format!("Missing value after '@{tag}'"));
        }
        assert!(TIME_TRAVEL_TAGS.contains(&"time:") && TIME_TRAVEL_TAGS.contains(&"iso:"));
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
    fn ledger_id_defaults_branch_and_is_canonical() {
        let id = LedgerId::parse("mydb").unwrap();
        assert_eq!(id, "mydb:main");
        assert_eq!((id.name(), id.branch()), ("mydb", "main"));
        assert_eq!(LedgerId::parse("mydb:main").unwrap(), id);
        assert_eq!(id.path_prefix(), "mydb/main");
    }

    /// A branchless nested name is a name, never a `name/branch` path: the
    /// old path-form heuristic sent `acme/inventory` to branch `inventory`
    /// (#1540) and `test/db`'s `@shared` to ledger `test`'s.
    #[test]
    fn nested_names_keep_every_segment() {
        let id = LedgerId::parse("acme/inventory").unwrap();
        assert_eq!(id, "acme/inventory:main");
        assert_eq!(id.path_prefix(), "acme/inventory/main");
        assert_eq!(id.shared_prefix(), "acme/inventory/@shared");
        assert_eq!(
            LedgerName::parse("test/db").unwrap().shared_prefix(),
            "test/db/@shared"
        );
        assert_eq!(
            crate::address_path::ledger_id_to_path_prefix("acme/inventory").unwrap(),
            "acme/inventory/main"
        );
    }

    #[test]
    fn ledger_id_rejects_reserved_characters_with_reason() {
        for (input, needle) in [
            ("a:b:c", "branch cannot contain ':'"),
            ("mydb@t:5", "ledger name cannot contain '@'"),
            ("mydb:main@t:5", "branch cannot contain '@'"),
            ("mydb#txn-meta", "ledger name cannot contain '#'"),
            ("", "ledger name cannot be empty"),
            (":main", "ledger name cannot be empty"),
            ("mydb:", "branch cannot be empty"),
            ("/mydb", "empty '/' segments"),
            ("a//b", "empty '/' segments"),
            ("a/../b", "'.' or '..' segments"),
            ("mydb:../x", "path traversal"),
        ] {
            let err = LedgerId::parse(input).unwrap_err().to_string();
            assert!(err.contains(needle), "{input:?}: {err}");
            assert!(err.contains(&format!("'{input}'")), "{input:?}: {err}");
        }
    }

    #[test]
    fn expect_canonical_names_the_seam() {
        let err = LedgerId::expect_canonical("mydb", "LedgerManager::notify")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'mydb'") && err.contains("LedgerManager::notify"),
            "{err}"
        );
        assert!(LedgerId::expect_canonical("mydb:main", "x").is_ok());
    }

    #[test]
    fn ledger_ref_splits_every_part_once() {
        let r = LedgerRef::parse("urn:fluree:mydb:dev@t:5#txn-meta").unwrap();
        assert_eq!(r.id, "mydb:dev");
        assert_eq!(r.at.as_deref(), Some("t:5"));
        assert_eq!(r.fragment.as_deref(), Some("txn-meta"));
        assert_eq!(r.time_spec().unwrap(), Some(LedgerIdTimeSpec::AtT(5)));

        let r = LedgerRef::parse("mydb").unwrap();
        assert_eq!((r.id.as_str(), r.at, r.fragment), ("mydb:main", None, None));

        // A fragment IRI may itself contain ':' and '@'.
        let r = LedgerRef::parse("mydb#http://ex.org/g@1").unwrap();
        assert_eq!(r.fragment.as_deref(), Some("http://ex.org/g@1"));

        assert!(LedgerRef::parse("mydb#").is_err());
        assert!(LedgerRef::parse("@t:5").is_err());
        assert!(LedgerRef::parse("mydb@").is_err());
    }

    /// `split_ledger_id` used to accept `mydb@t:5` as name `mydb@t`, branch `5`.
    #[test]
    fn split_rejects_time_suffix() {
        assert!(split_ledger_id("mydb@t:5").is_err());
        assert!(normalize_ledger_id("mydb@t:5").is_err());
    }

    #[test]
    fn ledger_name_rejects_branch() {
        assert!(LedgerName::parse("mydb:main").is_err());
        assert_eq!(
            LedgerName::parse("mydb")
                .unwrap()
                .with_branch("dev")
                .unwrap(),
            "mydb:dev"
        );
    }

    #[test]
    fn deserialize_is_lenient_serialize_is_canonical() {
        let id: LedgerId = serde_json::from_str("\"mydb\"").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"mydb:main\"");
        assert!(serde_json::from_str::<LedgerId>("\"a:b:c\"").is_err());
    }

    #[test]
    fn new_names_and_branches_cannot_shadow_layout_dirs() {
        for b in ["index", "commit", "txn", "config", "blob", "main.index"] {
            assert!(validate_branch_name(b).is_err(), "{b}");
        }
        assert!(validate_branch_name("main.json").is_ok());
        assert!(validate_ledger_name("a/main/index/child").is_err());
        assert!(validate_ledger_name("graph-sources/x").is_err());
        assert!(validate_ledger_name("acme/inventory").is_ok());
        assert!(validate_ledger_name("mydb:main").is_err());
        // Existing ledgers with such names still parse.
        assert!(LedgerId::parse("a/main/index/child:main").is_ok());
    }

    #[test]
    fn fragment_keeps_later_hash_and_at() {
        let r = LedgerRef::parse("mydb#http://ex.org/g#frag@2").unwrap();
        assert_eq!(r.id, "mydb:main");
        assert_eq!(r.at, None);
        assert_eq!(r.fragment.as_deref(), Some("http://ex.org/g#frag@2"));
    }

    #[test]
    fn new_branches_cannot_contain_slash_or_hash() {
        assert!(validate_branch_name("release/v1.0").is_err());
        assert!(validate_branch_name("a#b").is_err());
        // ...but existing ones still parse.
        assert_eq!(
            LedgerId::parse("mydb:release/v1.0").unwrap().branch(),
            "release/v1.0"
        );
    }

    #[test]
    fn test_validate_branch_name_too_long() {
        let long = "a".repeat(MAX_BRANCH_NAME_LEN + 1);
        assert!(validate_branch_name(&long).is_err());
        let ok = "a".repeat(MAX_BRANCH_NAME_LEN);
        assert!(validate_branch_name(&ok).is_ok());
    }
}
