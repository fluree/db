//! Typed references to what a read or an update addresses.
//!
//! A user names a ledger, a time, a graph of a ledger, or a bare graph IRI as
//! text. That text is parsed exactly once, at the surface edge, into one of the
//! types here, and nothing past the edge inspects the text again to decide
//! what it names.
//!
//! Which grammar applies depends on the **position** the text appears in:
//!
//! | Position | Parser | Examples |
//! |---|---|---|
//! | a ledger position: an HTTP ledger path, `Fluree::graph()` / `db()`, a CLI `--ledger`, an MCP `ledger` argument, `SERVICE <fluree:ledger:…>` | [`LedgerRef::parse`] | `mydb`, `mydb:dev@t:5`, `urn:fluree:mydb:main#txn-meta` |
//! | a dataset position: SPARQL `FROM` / `FROM NAMED` / `TO` / `USING` / `WITH`, a JSON-LD `from` / `fromNamed` string | [`DatasetRef::parse`] | the above, or `http://ex.org/g` |
//! | a graph of an already-named ledger: a `#fragment`, a JSON-LD `graph` value | [`GraphSel::parse`] | `txn-meta`, `config`, `http://ex.org/g` |
//!
//! The address grammar is
//!
//! ```text
//! [urn:fluree:]name[:branch][@<tag>:<value>][#<graph>]
//! ```
//!
//! with two rules that keep it from swallowing graph IRIs:
//!
//! - **A branch may not begin with `/`** ([`LedgerId::parse`]). `name://…` is
//!   the authority form of an IRI, so `http://ex.org/g` is never a ledger.
//! - **`@` starts a time pin only when a known tag follows it**
//!   ([`TIME_TRAVEL_TAGS`]). In a ledger position any other `@` is an error; in
//!   a dataset position it means the text is not an address at all
//!   (`http://ex.org/@alice/g`, `mailto:a@b`).
//!
//! Resolution against a nameservice or a ledger's graph registry is not done
//! here: this module is pure syntax, and `DatasetRef::Ambiguous` carries both
//! readings of a string that is lexically both, for the resolver to choose.

use crate::graph_registry::{
    validate_absolute_graph_iri, CONFIG_GRAPH_ID, DEFAULT_GRAPH_ID, TXN_META_GRAPH_ID,
};
use crate::ids::GraphId;
use crate::ledger_id::{
    parse_time_travel_spec, LedgerId, LedgerIdParseError, LedgerIdTimeSpec, COMMIT_PREFIX_MIN_LEN,
    LEDGER_URN_PREFIX, TIME_TRAVEL_TAGS,
};
use std::fmt;
use std::sync::Arc;

/// The error every parser in this module returns: a caller mistake, reported
/// as a bad request.
pub type RefError = LedgerIdParseError;

// ============================================================================
// GraphIri
// ============================================================================

/// An absolute IRI naming a graph.
///
/// Constructed only through [`GraphIri::parse`], so a relative or empty IRI
/// cannot name a graph (`validate_absolute_graph_iri` is the rule).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GraphIri(Arc<str>);

impl GraphIri {
    /// Accept `s` if it is an absolute IRI.
    pub fn parse(s: &str) -> Result<Self, RefError> {
        validate_absolute_graph_iri(s).map_err(RefError::new)?;
        Ok(Self(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The IRI as a shared string, for runtime maps keyed by graph name.
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }
}

impl std::ops::Deref for GraphIri {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for GraphIri {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GraphIri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for GraphIri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

// ============================================================================
// GraphSel
// ============================================================================

/// Which graph of one ledger (or graph source). The only graph-name keyword
/// table: `default`, `txn-meta` and `config` are spelled here and nowhere else.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GraphSel {
    /// The ledger's default graph (g_id 0).
    Default,
    /// The ledger's transaction-metadata graph (g_id 1).
    TxnMeta,
    /// The ledger's config graph (g_id 2).
    Config,
    /// A graph named by an absolute IRI, resolved against the ledger's graph
    /// registry. It may name a reserved slot too: a branch inherits its
    /// source's `urn:fluree:<source>#config` registration verbatim.
    Named(GraphIri),
}

impl GraphSel {
    /// The keyword spellings, in the order [`GraphSel::keyword`] matches them.
    pub const KEYWORDS: [&'static str; 3] = ["default", "txn-meta", "config"];

    /// The keyword `s` spells, if it is one.
    pub fn keyword(s: &str) -> Option<Self> {
        match s {
            "default" => Some(Self::Default),
            "txn-meta" => Some(Self::TxnMeta),
            "config" => Some(Self::Config),
            _ => None,
        }
    }

    /// A graph of an already-named ledger: a keyword or an absolute IRI.
    ///
    /// A relative name (`#products`) is refused rather than looked up as the
    /// literal string: the registry holds absolute IRIs, so it could only miss.
    pub fn parse(s: &str) -> Result<Self, RefError> {
        if let Some(sel) = Self::keyword(s) {
            return Ok(sel);
        }
        GraphIri::parse(s).map(Self::Named).map_err(|e| {
            RefError::new(format!(
                "Invalid graph '{s}': expected 'default', 'txn-meta', 'config' or an \
                 absolute graph IRI ({e})"
            ))
        })
    }

    /// The fixed graph id of a keyword; `None` for a named graph, whose id
    /// comes from the registry.
    pub fn reserved_g_id(&self) -> Option<GraphId> {
        match self {
            Self::Default => Some(DEFAULT_GRAPH_ID),
            Self::TxnMeta => Some(TXN_META_GRAPH_ID),
            Self::Config => Some(CONFIG_GRAPH_ID),
            Self::Named(_) => None,
        }
    }

    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }
}

impl fmt::Display for GraphSel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("default"),
            Self::TxnMeta => f.write_str("txn-meta"),
            Self::Config => f.write_str("config"),
            Self::Named(iri) => f.write_str(iri),
        }
    }
}

// ============================================================================
// TimeSpec
// ============================================================================

/// A point in a ledger's (or a graph source's) history.
///
/// Also reachable as `fluree_db_api::TimeSpec` and
/// `fluree_db_api::dataset::TimeSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimeSpec {
    /// At a specific transaction number
    AtT(i64),
    /// At a specific commit hash
    AtCommit(String),
    /// At a specific ISO 8601 timestamp, resolved against commit *event
    /// time* (`db:time` — user-suppliable for backdated historical loads)
    AtTime(String),
    /// At a specific ISO 8601 timestamp, resolved against the wall-clock
    /// time commits were *recorded* (`db:receivedAt`, audit axis).
    /// Identical to `AtTime` on ledgers that never used caller-supplied
    /// event times.
    AtRecorded(String),
    /// At a table format's own snapshot id (`@snapshot:<id>`). Resolvable only
    /// by a graph source backed by a snapshotted table; native ledgers reject it.
    AtSnapshot(i64),
    /// "latest" keyword - resolves to current ledger t
    Latest,
}

impl TimeSpec {
    /// Create at-t specification
    pub fn at_t(t: i64) -> Self {
        Self::AtT(t)
    }

    /// Create at-commit specification
    pub fn at_commit(commit: impl Into<String>) -> Self {
        Self::AtCommit(commit.into())
    }

    /// Create at-time specification
    pub fn at_time(time: impl Into<String>) -> Self {
        Self::AtTime(time.into())
    }

    /// Create at-recorded specification (audit axis)
    pub fn at_recorded(time: impl Into<String>) -> Self {
        Self::AtRecorded(time.into())
    }

    /// Create latest specification
    pub fn latest() -> Self {
        Self::Latest
    }

    /// Parse the canonical time-travel grammar: a ledger address's `@` suffix
    /// (`mydb:main@t:5`) with the `@` removed.
    ///
    /// | spelling | meaning |
    /// |---|---|
    /// | `t:<N>` | transaction number `N` |
    /// | `t:latest` | the ledger's current head |
    /// | `time:<timestamp>` (alias `iso:`) | commit *event* time (`db:time`) |
    /// | `recorded:<timestamp>` | wall-clock time the commit was recorded (`db:receivedAt`) |
    /// | `commit:<prefix>` | commit hex-digest prefix, at least 6 characters |
    ///
    /// This is an address grammar, so it is deliberately strict — widening it
    /// here widens what `ledger@<spec>` accepts on every query surface. User
    /// typed `--at` / `at=` arguments go through [`TimeSpec::parse_at`], which
    /// layers the CLI's older bare spellings on top without touching this one.
    pub fn parse(spec: &str) -> Result<Self, LedgerIdParseError> {
        Self::parse_with_sigil(spec, "")
    }

    /// [`TimeSpec::parse`] for the suffix of a ledger address — identical
    /// grammar, but errors quote the tag with the `@` the user actually typed
    /// (`Missing value after '@t:'`, not `'t:'`).
    pub fn parse_address_suffix(spec: &str) -> Result<Self, LedgerIdParseError> {
        Self::parse_with_sigil(spec, "@")
    }

    fn parse_with_sigil(spec: &str, sigil: &str) -> Result<Self, LedgerIdParseError> {
        // `LedgerIdTimeSpec` has no `Latest`: resolving it needs the ledger's
        // current `t`, which the core parser has no access to. Taking it here
        // is what makes `@t:latest` work.
        if spec == "t:latest" {
            return Ok(TimeSpec::Latest);
        }
        parse_time_travel_spec(spec, sigil).map(TimeSpec::from)
    }

    /// Parse an `--at` / `at=` argument: [`TimeSpec::parse`]'s grammar plus the
    /// three bare spellings the CLI and `POST /export` accepted before the
    /// canonical tags were wired up.
    ///
    /// Beyond the tagged forms it accepts, in this order:
    ///
    /// - `latest` — the untagged spelling of `t:latest`, which `fluree history
    ///   --to` has always taken.
    /// - a bare integer → `AtT`
    /// - a string containing `-` → `AtTime` (an ISO-8601 timestamp or date; a
    ///   hex digest never contains one, so a date-only value fails as a bad
    ///   timestamp rather than as an unknown commit)
    /// - anything else → `AtCommit` (a bare hex-digest prefix)
    ///
    /// **A bare integer is a `t`, not a commit prefix.** `123456` is both a
    /// valid `t` and a valid 6-character hex prefix; this grammar resolves the
    /// ambiguity in favour of `t` because that is what `--at` has always done.
    /// `commit:123456` forces the prefix reading and `t:123456` forces the
    /// other — which is the point of having the tags.
    ///
    /// A spec that *starts with* a canonical tag is never reinterpreted as one
    /// of the bare forms: `--at t:abc` is a malformed `t:`, not a commit prefix
    /// named `t:abc`. Silently falling through was the whole of #1805.
    pub fn parse_at(spec: &str) -> Result<Self, LedgerIdParseError> {
        if spec == "latest" {
            return Ok(TimeSpec::Latest);
        }
        if TIME_TRAVEL_TAGS.iter().any(|tag| spec.starts_with(tag)) {
            return Self::parse(spec).map_err(|e| {
                LedgerIdParseError::new(format!("{e}. {ACCEPTED_TIME_SPEC_SPELLINGS}"))
            });
        }
        if spec.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Empty time spec. {ACCEPTED_TIME_SPEC_SPELLINGS}"
            )));
        }
        if let Ok(t) = spec.parse::<i64>() {
            return Ok(TimeSpec::AtT(t));
        }
        if spec.contains('-') {
            // Looks like ISO-8601 (e.g. "2024-01-15T10:30:00Z", or a bare date).
            return Ok(TimeSpec::AtTime(spec.to_string()));
        }
        // The same floor the `commit:` arm applies, so the two spellings of a
        // too-short prefix agree: `commit:abc` was rejected by the tag arm
        // while a bare `abc` was accepted and died at the resolver with the
        // same complaint several layers down.
        //
        // This is a fast-path rejection, not the authority. `normalize_commit_ref`
        // strips `fluree:commit:` / `sha256:` and decodes canonical CIDs before
        // measuring, so `sha256:abc` is ten characters here and three there —
        // it passes this check and is correctly rejected downstream. Measuring
        // the raw string is only sound in the direction it is used: everything
        // this rejects, the resolver would also reject.
        if spec.len() < COMMIT_PREFIX_MIN_LEN {
            return Err(LedgerIdParseError::new(format!(
                "Commit prefix must be at least {COMMIT_PREFIX_MIN_LEN} characters, got {}. \
                 {ACCEPTED_TIME_SPEC_SPELLINGS}",
                spec.len()
            )));
        }
        Ok(TimeSpec::AtCommit(spec.to_string()))
    }
}

/// The spellings [`TimeSpec::parse_at`] accepts, quoted back when a user reaches
/// for a canonical tag and mis-spells it.
pub const ACCEPTED_TIME_SPEC_SPELLINGS: &str =
    "Accepted: t:<N>, t:latest, latest, time:<ISO-8601> (or its alias iso:), \
     recorded:<ISO-8601>, commit:<hex-prefix>, snapshot:<id> (graph sources only), \
     a bare transaction number, a bare ISO-8601 timestamp, or a bare commit \
     hex-digest prefix";

impl From<LedgerIdTimeSpec> for TimeSpec {
    fn from(spec: LedgerIdTimeSpec) -> Self {
        match spec {
            LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
            LedgerIdTimeSpec::AtIso(value) => TimeSpec::AtTime(value),
            LedgerIdTimeSpec::AtCommit(value) => TimeSpec::AtCommit(value),
            LedgerIdTimeSpec::AtRecorded(value) => TimeSpec::AtRecorded(value),
            LedgerIdTimeSpec::AtSnapshot(id) => TimeSpec::AtSnapshot(id),
        }
    }
}

// ============================================================================
// LedgerRef
// ============================================================================

/// A parsed ledger (or graph-source) address: which ledger, at which time,
/// which graph.
///
/// The fields are private: a `LedgerRef` exists only by parsing an address or
/// by building one from an already-valid [`LedgerId`], and a pin is applied to
/// the value ([`LedgerRef::with_at`]), never spliced into text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerRef {
    id: LedgerId,
    at: Option<TimeSpec>,
    graph: GraphSel,
}

impl LedgerRef {
    /// Parse `[urn:fluree:]name[:branch][@<tag>:<value>][#<graph>]`, for a
    /// ledger position.
    ///
    /// Names and branches cannot contain `@` or `#`, so the first `#` starts
    /// the graph and the first `@` before it starts the pin. The graph is a
    /// keyword or an absolute IRI ([`GraphSel::parse`]); the pin is the
    /// address grammar of [`TimeSpec::parse_address_suffix`].
    ///
    /// An `@` whose tag is not a known one is a bad pin here. A dataset
    /// position reads the same text as "not an address"
    /// ([`DatasetRef::parse`]).
    pub fn parse(input: &str) -> Result<Self, RefError> {
        let body = input.strip_prefix(LEDGER_URN_PREFIX).unwrap_or(input);
        let (before_graph, graph) = match body.split_once('#') {
            Some((_, "")) => {
                return Err(RefError::new(format!(
                    "Invalid ledger address '{input}': missing graph after '#'"
                )))
            }
            Some((left, fragment)) => {
                let graph = GraphSel::parse(fragment)
                    .map_err(|e| RefError::new(format!("Invalid ledger address '{input}': {e}")))?;
                (left, graph)
            }
            None => (body, GraphSel::Default),
        };
        let (base, at) = match before_graph.split_once('@') {
            Some(("", _)) => {
                return Err(RefError::new(format!(
                    "Invalid ledger address '{input}': ledger id cannot be empty before '@'"
                )))
            }
            Some((_, "")) => {
                return Err(RefError::new(format!(
                    "Invalid ledger address '{input}': missing time spec after '@'"
                )))
            }
            Some((base, spec)) => {
                let at = TimeSpec::parse_address_suffix(spec)
                    .map_err(|e| RefError::new(format!("Invalid ledger address '{input}': {e}")))?;
                (base, Some(at))
            }
            None => (before_graph, None),
        };
        Ok(Self {
            id: LedgerId::parse(base)?,
            at,
            graph,
        })
    }

    /// The whole default graph of `id`, at head.
    pub fn new(id: LedgerId) -> Self {
        Self {
            id,
            at: None,
            graph: GraphSel::Default,
        }
    }

    pub fn id(&self) -> &LedgerId {
        &self.id
    }

    /// The pin, if the address names one.
    pub fn at(&self) -> Option<&TimeSpec> {
        self.at.as_ref()
    }

    pub fn graph(&self) -> &GraphSel {
        &self.graph
    }

    /// True when this names the whole ledger at head: no pin, the default
    /// graph.
    pub fn is_bare(&self) -> bool {
        self.at.is_none() && self.graph.is_default()
    }

    /// Whether this is `id`'s own address: `id` in any spelling (`name`,
    /// `name:branch`, `urn:fluree:…`), with no time pin and the default graph
    /// (no graph, or `#default`). The one test every surface uses for "the
    /// ledger's own address names its default graph".
    pub fn is_own_address(&self, id: &LedgerId) -> bool {
        self.is_bare() && self.id == *id
    }

    /// Pin the address. Replaces any pin the text carried.
    pub fn with_at(mut self, at: TimeSpec) -> Self {
        self.at = Some(at);
        self
    }

    /// Drop the pin.
    pub fn without_at(mut self) -> Self {
        self.at = None;
        self
    }

    /// Select a graph of the addressed ledger.
    pub fn with_graph(mut self, graph: GraphSel) -> Self {
        self.graph = graph;
        self
    }

    pub fn into_id(self) -> LedgerId {
        self.id
    }

    pub fn into_parts(self) -> (LedgerId, Option<TimeSpec>, GraphSel) {
        (self.id, self.at, self.graph)
    }
}

impl std::str::FromStr for LedgerRef {
    type Err = RefError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

// ============================================================================
// DatasetRef
// ============================================================================

/// What a dataset-position string names, before resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatasetRef {
    /// Lexically only a ledger address (`mydb`, `mydb@t:5`).
    Address(LedgerRef),
    /// Lexically only a graph IRI (`http://ex.org/g`, `urn:ex:doc:1`).
    GraphIri(GraphIri),
    /// Both: an absolute IRI that also parses as an address (`mydb:main`,
    /// `urn:fluree:mydb:main#config`). Only a resolver holding the target
    /// ledger's graph registry can choose; with no target, the address wins.
    Ambiguous { address: LedgerRef, iri: GraphIri },
}

impl DatasetRef {
    /// Classify a dataset-position string (a SPARQL `FROM` / `FROM NAMED` /
    /// `TO` / `USING` / `WITH` IRI after prefix and BASE expansion, or a
    /// JSON-LD `from` / `fromNamed` string that is not a graph keyword).
    ///
    /// 1. `urn:fluree:…` that parses as an address: `Address` when pinned,
    ///    else `Ambiguous` (a ledger's registry may hold such an IRI verbatim).
    ///    One that does not parse (`#resolution-config`) is a `GraphIri`.
    /// 2. An authority form (`scheme://`) or two or more `:` before any `#`
    ///    or `@`: a `GraphIri`, never an address.
    /// 3. Parses as a pinned address: `Address`.
    /// 4. Parses as an address and is an absolute IRI too (`mydb:main`):
    ///    `Ambiguous`.
    /// 5. Parses as an address with no scheme (`mydb`): `Address`.
    /// 6. Otherwise an absolute IRI is a `GraphIri`, and anything else is
    ///    refused (relative text the edge could not resolve).
    pub fn parse(s: &str) -> Result<Self, RefError> {
        let pre_graph = s.split('#').next().unwrap_or(s);
        if s.starts_with(LEDGER_URN_PREFIX) {
            // Text that is no address, an unknown `@` tag included, is
            // read as an IRI.
            return Ok(match LedgerRef::parse(s) {
                Ok(address) if address.at.is_some() => Self::Address(address),
                Ok(address) => Self::Ambiguous {
                    address,
                    iri: GraphIri::parse(s)?,
                },
                Err(_) => Self::GraphIri(GraphIri::parse(s)?),
            });
        }
        // The part an address's name and branch would occupy: before any
        // graph and any pin (a pin's tag has its own ':').
        let base = pre_graph.split('@').next().unwrap_or(pre_graph);
        if base.contains("://") || base.matches(':').count() >= 2 {
            return Self::graph_iri(s);
        }
        match LedgerRef::parse(s) {
            Ok(address) if address.at.is_some() => Ok(Self::Address(address)),
            Ok(address) if pre_graph.contains(':') => Ok(match GraphIri::parse(s) {
                Ok(iri) => Self::Ambiguous { address, iri },
                Err(_) => Self::Address(address),
            }),
            Ok(address) => Ok(Self::Address(address)),
            // A pin with a known tag and a bad value is still a pin: report it.
            Err(e) if Self::has_known_pin(pre_graph) => Err(e),
            // Text that is no address, an unknown `@` tag included, is
            // read as an IRI.
            Err(not_an_address) => Self::graph_iri(s).map_err(|neither| {
                // Text that is no IRI either but has an `@` (`mydb@foo`) is
                // most likely a mistyped pin: say that.
                if base.len() < pre_graph.len() {
                    not_an_address
                } else {
                    neither
                }
            }),
        }
    }

    fn graph_iri(s: &str) -> Result<Self, RefError> {
        GraphIri::parse(s).map(Self::GraphIri).map_err(|_| {
            RefError::new(format!(
                "'{s}' is neither a ledger address ([urn:fluree:]name[:branch][@t:<N>][#graph]) \
                 nor an absolute graph IRI"
            ))
        })
    }

    fn has_known_pin(pre_graph: &str) -> bool {
        pre_graph
            .split_once('@')
            .is_some_and(|(_, spec)| TIME_TRAVEL_TAGS.iter().any(|tag| spec.starts_with(tag)))
    }

    /// The address reading, when there is one.
    pub fn address(&self) -> Option<&LedgerRef> {
        match self {
            Self::Address(a) | Self::Ambiguous { address: a, .. } => Some(a),
            Self::GraphIri(_) => None,
        }
    }

    /// The graph-IRI reading, when there is one.
    pub fn graph_iri_reading(&self) -> Option<&GraphIri> {
        match self {
            Self::GraphIri(iri) | Self::Ambiguous { iri, .. } => Some(iri),
            Self::Address(_) => None,
        }
    }
}

// ============================================================================
// MemberRef
// ============================================================================

/// One dataset member as the edge parsed it: a dataset-position reference, or
/// a graph keyword (`default` / `txn-meta` / `config`) that names a graph of
/// the target ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberRef {
    /// `default`, `txn-meta` or `config`: a graph of whatever ledger the
    /// surface targets. There is no such ledger on a connection surface.
    Keyword(GraphSel),
    /// Anything else in a dataset position.
    Dataset(DatasetRef),
}

impl MemberRef {
    /// Parse a dataset-position string: a graph keyword, or
    /// [`DatasetRef::parse`].
    pub fn parse(s: &str) -> Result<Self, RefError> {
        match GraphSel::keyword(s) {
            Some(sel) => Ok(Self::Keyword(sel)),
            None => DatasetRef::parse(s).map(Self::Dataset),
        }
    }

    /// The address reading, when there is one.
    pub fn address(&self) -> Option<&LedgerRef> {
        match self {
            Self::Dataset(r) => r.address(),
            Self::Keyword(_) => None,
        }
    }
}

// ============================================================================
// Resolution in a target ledger
// ============================================================================

/// A graph of the target ledger that a reference resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetGraph {
    /// The graph's id in the target ledger.
    pub g_id: GraphId,
    /// The same graph as a selector: a keyword for a reserved slot, else the
    /// registered IRI.
    pub graph: GraphSel,
}

/// Why a reference does not name a graph of the target ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetError {
    /// The reference names another ledger (or graph source).
    CrossLedger {
        /// What the reference names.
        named: LedgerId,
        /// The ledger the surface reads.
        target: LedgerId,
        /// The text was also an absolute IRI (`urn:x`, `mydb:dev`), which the
        /// target's registry does not hold either.
        also_iri: bool,
    },
    /// The reference is a graph IRI the target ledger has no graph for.
    GraphNotFound(String),
}

impl fmt::Display for TargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CrossLedger {
                named,
                target,
                also_iri: false,
            } => write!(
                f,
                "'{named}' names another ledger; this endpoint reads '{target}'. \
                 Query both through the connection endpoint (/query) instead"
            ),
            Self::CrossLedger {
                named,
                target,
                also_iri: true,
            } => write!(
                f,
                "'{named}' is not a graph of '{target}', and as a ledger address it names \
                 another ledger. Query both through the connection endpoint (/query) instead"
            ),
            Self::GraphNotFound(iri) => write!(f, "Graph not found: '{iri}'"),
        }
    }
}

impl std::error::Error for TargetError {}

/// The ledger a ledger-scoped surface reads, as the one context a dataset
/// reference is resolved in: the HTTP ledger routes, the view API, the CLI's
/// local target, and an update's WHERE.
///
/// `lookup` is the ledger's graph registry: exact IRI to graph id.
pub struct TargetLedger<'a> {
    id: &'a LedgerId,
    lookup: &'a dyn Fn(&str) -> Option<GraphId>,
}

impl<'a> TargetLedger<'a> {
    pub fn new(id: &'a LedgerId, lookup: &'a dyn Fn(&str) -> Option<GraphId>) -> Self {
        Self { id, lookup }
    }

    pub fn id(&self) -> &LedgerId {
        self.id
    }

    /// Resolve one dataset member, as written and as parsed, to a graph of
    /// this ledger. `pinned` says whether the member carries a time of its own
    /// (the caller reconciles or refuses it).
    ///
    /// In order:
    ///
    /// 0. This ledger's own address ([`LedgerRef::is_own_address`]: any
    ///    spelling, no pin, the default graph): the default graph. The
    ///    ledger's name stays reserved for its default graph even if a
    ///    registered graph shares the IRI.
    /// 1. The written text as an exact registry IRI, reserved slots included:
    ///    a branch inherits its source's `urn:fluree:<source>#config`
    ///    registration, and a user graph may itself be named `urn:fluree:…`.
    /// 2. Otherwise by what the text parsed as: a keyword is that graph; an
    ///    address of this ledger selects its graph; an address of another
    ///    ledger is [`TargetError::CrossLedger`]; a graph IRI the registry does
    ///    not hold is [`TargetError::GraphNotFound`].
    pub fn resolve(
        &self,
        written: &str,
        member: &MemberRef,
        pinned: bool,
    ) -> Result<TargetGraph, TargetError> {
        if let Some(address) = member.address() {
            if !pinned && address.is_own_address(self.id) {
                return Ok(self.reserved(GraphSel::Default));
            }
        }
        if let Some(found) = self.registered(written) {
            return Ok(found);
        }
        match member {
            MemberRef::Keyword(sel) => self.graph(sel),
            MemberRef::Dataset(DatasetRef::GraphIri(iri)) => {
                Err(TargetError::GraphNotFound(iri.to_string()))
            }
            MemberRef::Dataset(
                reference @ (DatasetRef::Address(address) | DatasetRef::Ambiguous { address, .. }),
            ) => {
                if address.id() == self.id {
                    self.graph(address.graph())
                } else {
                    Err(TargetError::CrossLedger {
                        named: address.id().clone(),
                        target: self.id.clone(),
                        also_iri: matches!(reference, DatasetRef::Ambiguous { .. }),
                    })
                }
            }
        }
    }

    /// A graph of this ledger, named by a selector: a keyword is its reserved
    /// slot; an IRI is an exact registry entry (reserved slots included) or
    /// this ledger's own `urn:fluree:<id>#config` / `#txn-meta`.
    pub fn graph(&self, sel: &GraphSel) -> Result<TargetGraph, TargetError> {
        match sel {
            GraphSel::Named(iri) => self.registered(iri).map(Ok).unwrap_or_else(|| {
                if **iri == crate::graph_registry::config_graph_iri(self.id) {
                    Ok(self.reserved(GraphSel::Config))
                } else if **iri == crate::graph_registry::txn_meta_graph_iri(self.id) {
                    Ok(self.reserved(GraphSel::TxnMeta))
                } else {
                    Err(TargetError::GraphNotFound(iri.to_string()))
                }
            }),
            reserved => Ok(self.reserved(reserved.clone())),
        }
    }

    fn registered(&self, iri: &str) -> Option<TargetGraph> {
        let g_id = (self.lookup)(iri)?;
        Some(match g_id {
            DEFAULT_GRAPH_ID => self.reserved(GraphSel::Default),
            TXN_META_GRAPH_ID => self.reserved(GraphSel::TxnMeta),
            CONFIG_GRAPH_ID => self.reserved(GraphSel::Config),
            g_id => TargetGraph {
                g_id,
                graph: GraphSel::Named(GraphIri(iri.into())),
            },
        })
    }

    fn reserved(&self, graph: GraphSel) -> TargetGraph {
        TargetGraph {
            g_id: graph
                .reserved_g_id()
                .expect("a keyword names a reserved slot"),
            graph,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(s: &str) -> LedgerRef {
        match DatasetRef::parse(s).unwrap() {
            DatasetRef::Address(a) => a,
            other => panic!("{s:?}: expected Address, got {other:?}"),
        }
    }

    fn graph_iri(s: &str) -> GraphIri {
        match DatasetRef::parse(s).unwrap() {
            DatasetRef::GraphIri(iri) => iri,
            other => panic!("{s:?}: expected GraphIri, got {other:?}"),
        }
    }

    fn ambiguous(s: &str) -> (LedgerRef, GraphIri) {
        match DatasetRef::parse(s).unwrap() {
            DatasetRef::Ambiguous { address, iri } => (address, iri),
            other => panic!("{s:?}: expected Ambiguous, got {other:?}"),
        }
    }

    // --- GraphSel ---

    #[test]
    fn graph_sel_keywords_and_iris() {
        assert_eq!(GraphSel::parse("default").unwrap(), GraphSel::Default);
        assert_eq!(GraphSel::parse("txn-meta").unwrap(), GraphSel::TxnMeta);
        assert_eq!(GraphSel::parse("config").unwrap(), GraphSel::Config);
        assert_eq!(
            GraphSel::parse("http://ex.org/g").unwrap(),
            GraphSel::Named(GraphIri::parse("http://ex.org/g").unwrap())
        );
        // A user graph may itself be named with `urn:fluree:`.
        assert!(matches!(
            GraphSel::parse("urn:fluree:L:main#resolution-config").unwrap(),
            GraphSel::Named(_)
        ));
        assert_eq!(GraphSel::Default.reserved_g_id(), Some(DEFAULT_GRAPH_ID));
        assert_eq!(GraphSel::TxnMeta.reserved_g_id(), Some(TXN_META_GRAPH_ID));
        assert_eq!(GraphSel::Config.reserved_g_id(), Some(CONFIG_GRAPH_ID));
    }

    /// A relative or empty name cannot be a graph: nothing could match it.
    #[test]
    fn graph_sel_refuses_relative_names() {
        for bad in ["products", "#products", "", "Txn-Meta", "a b"] {
            assert!(GraphSel::parse(bad).is_err(), "{bad:?}");
        }
        assert!(GraphIri::parse("products").is_err());
    }

    // --- LedgerRef (ledger positions) ---

    #[test]
    fn ledger_ref_splits_every_part_once() {
        let r = LedgerRef::parse("urn:fluree:mydb:dev@t:5#txn-meta").unwrap();
        assert_eq!(r.id(), "mydb:dev");
        assert_eq!(r.at(), Some(&TimeSpec::AtT(5)));
        assert_eq!(r.graph(), &GraphSel::TxnMeta);

        let r = LedgerRef::parse("mydb").unwrap();
        assert_eq!(
            (r.id().as_str(), r.at(), r.graph()),
            ("mydb:main", None, &GraphSel::Default)
        );
        assert!(r.is_bare());

        // A fragment IRI may itself contain ':', '#' and '@'.
        let r = LedgerRef::parse("mydb#http://ex.org/g#frag@2").unwrap();
        assert_eq!(r.graph().to_string(), "http://ex.org/g#frag@2");
        assert_eq!(r.at(), None);

        let r = LedgerRef::parse("mydb@t:latest").unwrap();
        assert_eq!(r.at(), Some(&TimeSpec::Latest));
    }

    #[test]
    fn ledger_ref_refuses_malformed_addresses() {
        for bad in [
            "mydb#",
            "@t:5",
            "mydb@",
            "mydb@foo",
            "mydb@t:abc",
            "mydb#products",
            "a:b:c",
            "http://ex.org/g",
            "mydb:/x",
        ] {
            assert!(LedgerRef::parse(bad).is_err(), "{bad:?}");
        }
    }

    /// In a ledger position an unknown `@` tag is a bad pin, named as such.
    #[test]
    fn ledger_position_unknown_pin_is_an_error() {
        let err = LedgerRef::parse("mydb@foo").unwrap_err().to_string();
        assert!(err.contains("Invalid time travel format"), "{err}");
    }

    #[test]
    fn with_at_and_graph_replace_the_parsed_parts() {
        let r = LedgerRef::parse("mydb@t:1")
            .unwrap()
            .with_at(TimeSpec::AtT(2))
            .with_graph(GraphSel::Config);
        assert_eq!(r.at(), Some(&TimeSpec::AtT(2)));
        assert_eq!(r.graph(), &GraphSel::Config);
        assert_eq!(r.clone().without_at().at(), None);
        assert_eq!(r.into_id(), "mydb:main");
    }

    // --- A branch may not begin with `/` ---

    /// `name://…` is an IRI's authority form. A branch that begins with `/`
    /// is refused on input, while one merely containing `/` stays readable
    /// (legacy `feature/x` branches predate the creation-time ban).
    #[test]
    fn a_branch_cannot_begin_with_a_slash() {
        let err = LedgerId::parse("http://ex.org/g").unwrap_err().to_string();
        assert!(err.contains("cannot begin with '/'"), "{err}");
        assert!(LedgerId::parse("mydb:/x").is_err());
        assert_eq!(
            LedgerId::parse("mydb:feature/x").unwrap().branch(),
            "feature/x"
        );
        // Persisted records are not input: they keep their own grammar.
        assert_eq!(
            LedgerId::parse_persisted("mydb:feature/x")
                .unwrap()
                .branch(),
            "feature/x"
        );
    }

    // --- DatasetRef (dataset positions) ---

    #[test]
    fn bare_names_and_pinned_text_are_addresses() {
        assert_eq!(address("mydb").id(), "mydb:main");
        assert_eq!(address("acme/inventory").id(), "acme/inventory:main");
        assert_eq!(address("mydb#config").graph(), &GraphSel::Config);
        assert_eq!(address("mydb@t:3").at(), Some(&TimeSpec::AtT(3)));
        assert_eq!(address("mydb:main@t:3").at(), Some(&TimeSpec::AtT(3)));
        let r = address("mydb:dev@t:3#txn-meta");
        assert_eq!(
            (r.id().as_str(), r.graph()),
            ("mydb:dev", &GraphSel::TxnMeta)
        );
        assert_eq!(
            address("urn:fluree:mydb:main@t:3").at(),
            Some(&TimeSpec::AtT(3))
        );
    }

    #[test]
    fn a_scheme_and_a_branch_are_both_readings() {
        let (a, iri) = ambiguous("mydb:main");
        assert_eq!((a.id().as_str(), iri.as_str()), ("mydb:main", "mydb:main"));
        let (a, _) = ambiguous("mydb:main#config");
        assert_eq!(a.graph(), &GraphSel::Config);
        let (a, _) = ambiguous("ex:g");
        assert_eq!(a.id(), "ex:g");
        let (a, iri) = ambiguous("urn:fluree:mydb:main");
        assert_eq!(
            (a.id().as_str(), iri.as_str()),
            ("mydb:main", "urn:fluree:mydb:main")
        );
        let (a, _) = ambiguous("urn:fluree:mydb:main#config");
        assert_eq!(a.graph(), &GraphSel::Config);
        let (a, _) = ambiguous("urn:fluree:mydb");
        assert_eq!(a.id(), "mydb:main");
        let (a, _) = ambiguous("mydb:main#http://ex.org/g");
        assert_eq!(a.graph().to_string(), "http://ex.org/g");
    }

    /// Hierarchical IRIs, IRIs with an `@` segment, hash IRIs and multi-colon
    /// URNs are graph IRIs, never ledgers.
    #[test]
    fn graph_iris_are_never_addresses() {
        for iri in [
            "http://ex.org/g",
            "https://data.example/x",
            "http://example.org/vocab#products",
            "http://example.org/@alice/g",
            "urn:ex:doc:1",
            "urn:x:y#z",
            "file:///tmp/g",
            "mailto:a@b",
            "tag:ex.org,2026:g",
        ] {
            assert_eq!(graph_iri(iri).as_str(), iri);
        }
    }

    /// A `urn:fluree:` IRI whose fragment is not a graph name (solo's user
    /// graph `urn:fluree:L:main#resolution-config`) is a graph IRI, left for
    /// the target's registry to resolve exactly.
    #[test]
    fn a_urn_fluree_user_graph_is_a_graph_iri() {
        let iri = "urn:fluree:L:main#resolution-config";
        assert_eq!(graph_iri(iri).as_str(), iri);
    }

    /// In a dataset position an unknown `@` tag means "not an address".
    #[test]
    fn an_unknown_pin_tag_is_not_an_address() {
        assert_eq!(graph_iri("mailto:a@b").as_str(), "mailto:a@b");
        // No IRI either: the likeliest reading is a mistyped pin, so the
        // error is the pin's.
        let err = DatasetRef::parse("mydb@foo").unwrap_err().to_string();
        assert!(err.contains("Invalid time travel format"), "{err}");
        let err = DatasetRef::parse("products#x").unwrap_err().to_string();
        assert!(err.contains("neither a ledger address"), "{err}");
    }

    /// A known tag with a bad value is a malformed pin, not a graph IRI.
    #[test]
    fn a_known_tag_with_a_bad_value_is_an_error() {
        let err = DatasetRef::parse("mydb@t:abc").unwrap_err().to_string();
        assert!(err.contains("Invalid integer"), "{err}");
        assert!(DatasetRef::parse("mydb@commit:ab").is_err());
    }

    #[test]
    fn relative_and_empty_text_is_refused() {
        for bad in ["", "#products", "products#frag", "mydb#"] {
            assert!(DatasetRef::parse(bad).is_err(), "{bad:?}");
        }
    }

    /// An address whose graph is relative is not an address; with no scheme
    /// it is not an IRI either.
    #[test]
    fn an_address_with_a_relative_graph_is_refused() {
        assert!(DatasetRef::parse("mydb#products").is_err());
        // With a scheme the whole text is still an absolute IRI.
        assert_eq!(
            graph_iri("mydb:main#products").as_str(),
            "mydb:main#products"
        );
    }

    #[test]
    fn readings_are_exposed_uniformly() {
        let r = DatasetRef::parse("mydb:main").unwrap();
        assert!(r.address().is_some() && r.graph_iri_reading().is_some());
        let r = DatasetRef::parse("mydb").unwrap();
        assert!(r.address().is_some() && r.graph_iri_reading().is_none());
        let r = DatasetRef::parse("http://ex.org/g").unwrap();
        assert!(r.address().is_none() && r.graph_iri_reading().is_some());
    }

    // --- Resolution in a target ledger ---

    /// A branch `L:feature` whose registry, like every branch's, carries its
    /// source's reserved IRIs verbatim, plus user graphs, one of which is
    /// named with the branch's own address.
    fn feature_registry(iri: &str) -> Option<GraphId> {
        match iri {
            "urn:fluree:L:main#config" => Some(CONFIG_GRAPH_ID),
            "urn:fluree:L:main#txn-meta" => Some(TXN_META_GRAPH_ID),
            "http://ex.org/g" => Some(3),
            "urn:fluree:L:main#resolution-config" => Some(4),
            "L:feature" => Some(5),
            _ => None,
        }
    }

    fn resolve(written: &str) -> Result<TargetGraph, TargetError> {
        let id = LedgerId::parse("L:feature").unwrap();
        let target = TargetLedger::new(&id, &feature_registry);
        let member = MemberRef::parse(written).unwrap();
        let pinned = member.address().is_some_and(|a| a.at().is_some());
        target.resolve(written, &member, pinned)
    }

    fn g_id(written: &str) -> GraphId {
        resolve(written)
            .unwrap_or_else(|e| panic!("{written:?}: {e}"))
            .g_id
    }

    /// Every spelling of the target's own address is its default graph, even
    /// where a registered graph shares the text (`L:feature` is also a user
    /// graph's IRI here): the name stays reserved for the default graph.
    #[test]
    fn the_targets_own_address_is_its_default_graph() {
        for own in ["L:feature", "urn:fluree:L:feature", "L:feature@t:3"] {
            assert_eq!(g_id(own), DEFAULT_GRAPH_ID, "{own}");
        }
        assert_eq!(g_id("default"), DEFAULT_GRAPH_ID);
    }

    #[test]
    fn reserved_graphs_resolve_by_keyword_fragment_or_either_urn() {
        for config in [
            "config",
            "L:feature#config",
            "urn:fluree:L:feature#config",
            // The main branch's URN, which the branch inherited verbatim.
            "urn:fluree:L:main#config",
        ] {
            assert_eq!(g_id(config), CONFIG_GRAPH_ID, "{config}");
        }
        assert_eq!(g_id("txn-meta"), TXN_META_GRAPH_ID);
        assert_eq!(g_id("urn:fluree:L:feature#txn-meta"), TXN_META_GRAPH_ID);
    }

    /// A registered IRI resolves exactly, whatever it looks like: a user graph
    /// named `urn:fluree:…` is not re-read as an address.
    #[test]
    fn registered_iris_resolve_exactly() {
        assert_eq!(g_id("http://ex.org/g"), 3);
        assert_eq!(g_id("L:feature#http://ex.org/g"), 3);
        assert_eq!(g_id("urn:fluree:L:main#resolution-config"), 4);
    }

    #[test]
    fn other_ledgers_and_unknown_graphs_are_typed_errors() {
        assert!(matches!(
            resolve("other:main"),
            Err(TargetError::CrossLedger { ref named, .. }) if named == "other:main"
        ));
        // The same name on another branch is another ledger.
        assert!(matches!(
            resolve("L:main"),
            Err(TargetError::CrossLedger { .. })
        ));
        assert!(matches!(
            resolve("http://ex.org/nope"),
            Err(TargetError::GraphNotFound(ref iri)) if iri == "http://ex.org/nope"
        ));
        assert!(matches!(
            resolve("L:feature#http://ex.org/nope"),
            Err(TargetError::GraphNotFound(_))
        ));
    }

    // --- TimeSpec grammar (moved with the type from fluree-db-api) ---

    #[test]
    fn parse_accepts_every_canonical_tag() {
        assert_eq!(TimeSpec::parse("t:2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse("t:0").unwrap(), TimeSpec::AtT(0));
        assert_eq!(TimeSpec::parse("t:latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse("iso:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse("recorded:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtRecorded("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse("commit:abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );
    }

    /// The address grammar must stay strict: it is what `ledger@<spec>` accepts
    /// on every query surface, so the CLI's bare compatibility spellings must
    /// not leak into it.
    #[test]
    fn parse_rejects_the_bare_cli_spellings() {
        for bare in ["2", "latest", "2024-01-15T10:30:00Z", "abc123def"] {
            assert!(
                TimeSpec::parse(bare).is_err(),
                "address grammar must reject the bare spelling {bare:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_tagged_specs() {
        for bad in [
            "t:",
            "t:abc",
            "time:",
            "iso:",
            "commit:",
            "commit:abc",
            "recorded:",
            "snapshot:",
            "snapshot:abc",
        ] {
            assert!(
                TimeSpec::parse(bad).is_err(),
                "expected {bad:?} to be an error"
            );
        }
    }

    /// Error text quotes the tag as the calling surface spells it: a bare spec
    /// reports `'t:'`, an address suffix reports `'@t:'`.
    #[test]
    fn error_text_quotes_the_tag_as_the_calling_surface_spells_it() {
        for tag in ["t:", "time:", "iso:", "commit:", "recorded:", "snapshot:"] {
            assert_eq!(
                TimeSpec::parse(tag).unwrap_err().to_string(),
                format!("Missing value after '{tag}'")
            );
            assert_eq!(
                TimeSpec::parse_address_suffix(tag).unwrap_err().to_string(),
                format!("Missing value after '@{tag}'")
            );
        }

        let bare = TimeSpec::parse("nope").unwrap_err().to_string();
        assert!(
            bare.contains("Expected t:, time:, recorded:, commit:, or snapshot:"),
            "got: {bare}"
        );
        assert!(
            !bare.contains('@'),
            "bare-spec error must not mention '@': {bare}"
        );

        let addressed = TimeSpec::parse_address_suffix("nope")
            .unwrap_err()
            .to_string();
        assert!(
            addressed.contains("Expected @t:, @time:, @recorded:, @commit:, or @snapshot:"),
            "got: {addressed}"
        );
    }

    /// The `--at` grammar is the canonical one plus three bare spellings.
    #[test]
    fn parse_at_accepts_canonical_and_legacy_spellings() {
        assert_eq!(TimeSpec::parse_at("t:2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse_at("t:latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse_at("time:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("iso:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("recorded:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtRecorded("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("commit:abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );

        assert_eq!(TimeSpec::parse_at("2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse_at("latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse_at("2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );
    }

    #[test]
    fn parse_at_tagged_and_bare_integers_agree() {
        for t in [0_i64, 1, 42, 123_456] {
            assert_eq!(
                TimeSpec::parse_at(&t.to_string()).unwrap(),
                TimeSpec::parse_at(&format!("t:{t}")).unwrap(),
                "bare and tagged spellings of t={t} must agree"
            );
        }
    }

    /// A string that *starts with* a canonical tag is never reinterpreted as a
    /// bare form (#1805).
    #[test]
    fn parse_at_never_falls_back_to_a_prefix_for_a_tagged_spec() {
        for bad in ["t:abc", "iso:", "commit:abc", "recorded:", "t:"] {
            let err = TimeSpec::parse_at(bad).unwrap_err().to_string();
            assert!(
                err.contains("Accepted: t:<N>"),
                "{bad:?} must report the accepted spellings, got: {err}"
            );
        }
        assert!(TimeSpec::parse_at("").is_err());
    }

    /// One constant for the commit-prefix floor, quoted by every surface.
    #[test]
    fn every_surface_shares_one_commit_prefix_floor() {
        let at_floor = "a".repeat(COMMIT_PREFIX_MIN_LEN);
        let below = "a".repeat(COMMIT_PREFIX_MIN_LEN - 1);

        assert!(TimeSpec::parse_at(&at_floor).is_ok());
        assert!(TimeSpec::parse_at(&format!("commit:{at_floor}")).is_ok());
        assert!(TimeSpec::parse_address_suffix(&format!("commit:{at_floor}")).is_ok());

        let expected = format!("at least {COMMIT_PREFIX_MIN_LEN} characters");
        for err in [
            TimeSpec::parse_at(&below).unwrap_err().to_string(),
            TimeSpec::parse_at(&format!("commit:{below}"))
                .unwrap_err()
                .to_string(),
            TimeSpec::parse_address_suffix(&format!("commit:{below}"))
                .unwrap_err()
                .to_string(),
        ] {
            assert!(
                err.contains(&expected),
                "message must quote the constant: {err}"
            );
        }
    }

    #[test]
    fn parse_at_rejects_a_short_prefix_in_both_spellings() {
        for spec in ["abc", "a", "12ab", "abcde"] {
            let bare = TimeSpec::parse_at(spec).unwrap_err().to_string();
            let tagged = TimeSpec::parse_at(&format!("commit:{spec}"))
                .unwrap_err()
                .to_string();
            let expected = format!("at least {COMMIT_PREFIX_MIN_LEN} characters");
            assert!(
                bare.contains(&expected),
                "bare {spec:?} must be rejected at the boundary, got: {bare}"
            );
            assert!(tagged.contains(&expected), "got: {tagged}");
        }
        assert_eq!(
            TimeSpec::parse_at("abc123").unwrap(),
            TimeSpec::AtCommit("abc123".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("abc123").unwrap(),
            TimeSpec::parse_at("commit:abc123").unwrap()
        );
    }

    #[test]
    fn parse_at_resolves_the_digits_ambiguity_toward_t() {
        assert_eq!(
            TimeSpec::parse_at("123456").unwrap(),
            TimeSpec::AtT(123_456)
        );
        assert_eq!(
            TimeSpec::parse_at("commit:123456").unwrap(),
            TimeSpec::AtCommit("123456".to_string())
        );
    }

    /// The address path and the bare grammar agree modulo the `@`.
    #[test]
    fn address_suffix_and_bare_spec_agree_on_the_canonical_grammar() {
        for spec in [
            "t:7",
            "t:latest",
            "time:2024-01-15T10:30:00Z",
            "iso:2024-01-15T10:30:00Z",
            "recorded:2024-01-15T10:30:00Z",
            "commit:abc123def",
            "snapshot:42",
        ] {
            let r = LedgerRef::parse(&format!("mydb:main@{spec}")).unwrap();
            assert_eq!(r.id(), "mydb:main");
            assert_eq!(
                r.at(),
                Some(&TimeSpec::parse(spec).unwrap()),
                "address suffix @{spec} must mean the same as the bare spec {spec}"
            );
            assert_eq!(
                TimeSpec::parse_address_suffix(spec).unwrap(),
                TimeSpec::parse(spec).unwrap(),
                "the two entry points differ only in error text"
            );
        }
    }

    #[test]
    fn address_latest_parses_with_and_without_a_fragment() {
        let r = LedgerRef::parse("mydb:main@t:latest").unwrap();
        assert_eq!(r.at(), Some(&TimeSpec::Latest));
        let r = LedgerRef::parse("mydb:main@t:latest#txn-meta").unwrap();
        assert_eq!(
            (r.at(), r.graph()),
            (Some(&TimeSpec::Latest), &GraphSel::TxnMeta)
        );
        assert!(LedgerRef::parse("@t:latest").is_err());
    }

    #[test]
    fn from_ledger_id_time_spec_maps_every_variant() {
        assert_eq!(TimeSpec::from(LedgerIdTimeSpec::AtT(3)), TimeSpec::AtT(3));
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtIso("x".into())),
            TimeSpec::AtTime("x".into())
        );
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtRecorded("x".into())),
            TimeSpec::AtRecorded("x".into())
        );
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtCommit("abc123".into())),
            TimeSpec::AtCommit("abc123".into())
        );
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtSnapshot(9)),
            TimeSpec::AtSnapshot(9)
        );
    }
}
