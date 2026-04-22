use serde::{Deserialize, Serialize};

/// Maximum length of memory content in characters.
pub const MAX_CONTENT_LENGTH: usize = 750;

/// The kind of memory being stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryKind {
    Fact,
    Decision,
    Constraint,
}

impl MemoryKind {
    /// Short lowercase string for use in IDs and queries.
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Fact => "fact",
            MemoryKind::Decision => "decision",
            MemoryKind::Constraint => "constraint",
        }
    }

    /// RDF class IRI for this kind.
    pub fn class_iri(&self) -> &'static str {
        match self {
            MemoryKind::Fact => "mem:Fact",
            MemoryKind::Decision => "mem:Decision",
            MemoryKind::Constraint => "mem:Constraint",
        }
    }

    /// Parse from a string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "fact" => Some(MemoryKind::Fact),
            "decision" => Some(MemoryKind::Decision),
            "constraint" => Some(MemoryKind::Constraint),
            // Backwards compat: map removed kinds to closest equivalent
            "preference" => Some(MemoryKind::Fact),
            "artifact" => Some(MemoryKind::Fact),
            _ => None,
        }
    }
}

impl std::fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope of a memory — which named graph it belongs to.
///
/// Stored as IRIs in the knowledge graph (e.g., `mem:repo`, `mem:user`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Repo-wide memory, visible to all agents in this repo.
    #[default]
    Repo,
    /// User-private memory, follows the developer across repos.
    User,
}

impl Scope {
    /// The IRI used in the knowledge graph for this scope.
    pub fn iri(&self) -> &'static str {
        match self {
            Scope::Repo => "https://ns.flur.ee/memory#repo",
            Scope::User => "https://ns.flur.ee/memory#user",
        }
    }

    /// The short prefixed form (for display and SPARQL with prefix).
    pub fn prefixed(&self) -> &'static str {
        match self {
            Scope::Repo => "mem:repo",
            Scope::User => "mem:user",
        }
    }

    /// Parse from a string (accepts IRI, prefixed, or short form).
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "repo" | "mem:repo" | "https://ns.flur.ee/memory#repo" => Some(Scope::Repo),
            "user" | "mem:user" | "https://ns.flur.ee/memory#user" => Some(Scope::User),
            // Backwards compat with old string values
            "project" => Some(Scope::Repo),
            "global" => Some(Scope::User),
            _ => None,
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::Repo => f.write_str("repo"),
            Scope::User => f.write_str("user"),
        }
    }
}

/// Severity of a constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Must,
    Should,
    Prefer,
}

impl Severity {
    /// Parse from a string (case-insensitive).
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "must" => Some(Severity::Must),
            "should" => Some(Severity::Should),
            "prefer" => Some(Severity::Prefer),
            _ => None,
        }
    }
}

/// A stored memory record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    /// Unique ID (e.g., `mem:fact-01JDXYZ...`).
    pub id: String,
    /// Kind of memory.
    pub kind: MemoryKind,
    /// The content text.
    pub content: String,
    /// Tags for categorization and recall.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Scope (repo or user).
    #[serde(default)]
    pub scope: Scope,
    /// Severity (for constraints).
    pub severity: Option<Severity>,
    /// File/artifact references.
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    /// Git branch when created.
    pub branch: Option<String>,
    /// Creation timestamp.
    pub created_at: String,

    // -- Optional predicates (available on any kind) --
    /// Why this decision/constraint/fact exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// Alternatives considered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternatives: Option<String>,
}

/// Input for creating a new memory.
#[derive(Debug, Clone)]
pub struct MemoryInput {
    pub kind: MemoryKind,
    pub content: String,
    pub tags: Vec<String>,
    pub scope: Scope,
    pub severity: Option<Severity>,
    pub artifact_refs: Vec<String>,
    pub branch: Option<String>,
    pub rationale: Option<String>,
    pub alternatives: Option<String>,
}

/// Input for updating a memory in place.
#[derive(Debug, Clone)]
pub struct MemoryUpdate {
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub severity: Option<Severity>,
    pub artifact_refs: Option<Vec<String>>,
    pub rationale: Option<String>,
    pub alternatives: Option<String>,
}

/// Filter for querying memories.
#[derive(Debug, Clone, Default)]
pub struct MemoryFilter {
    pub kind: Option<MemoryKind>,
    pub tags: Vec<String>,
    pub scope: Option<Scope>,
    pub branch: Option<String>,
}

/// A memory scored by relevance for a recall query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredMemory {
    pub memory: Memory,
    pub score: f64,
}

/// Result from a recall operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallResult {
    pub query: String,
    pub memories: Vec<ScoredMemory>,
    pub total_count: usize,
}

/// Summary status of the memory store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStatus {
    pub initialized: bool,
    pub total_memories: usize,
    pub by_kind: Vec<(MemoryKind, usize)>,
    pub total_tags: usize,
    /// Preview of recent memories (content truncated) for discoverability.
    pub recent: Vec<MemoryPreview>,
    /// The memory directory path (e.g. `<repo>/.fluree-memory/`), or `None` if ledger-only.
    pub memory_dir: Option<String>,
}

/// A compact summary of a memory for status/listing output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPreview {
    pub id: String,
    pub kind: MemoryKind,
    /// Content truncated to ~100 chars.
    pub summary: String,
    pub tags: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_content_length_is_750() {
        assert_eq!(MAX_CONTENT_LENGTH, 750);
    }

    #[test]
    fn memory_kind_parse_current_kinds() {
        assert_eq!(MemoryKind::parse("fact"), Some(MemoryKind::Fact));
        assert_eq!(MemoryKind::parse("decision"), Some(MemoryKind::Decision));
        assert_eq!(
            MemoryKind::parse("constraint"),
            Some(MemoryKind::Constraint)
        );
        assert_eq!(MemoryKind::parse("FACT"), Some(MemoryKind::Fact));
    }

    #[test]
    fn memory_kind_parse_backwards_compat() {
        assert_eq!(MemoryKind::parse("preference"), Some(MemoryKind::Fact));
        assert_eq!(MemoryKind::parse("artifact"), Some(MemoryKind::Fact));
    }

    #[test]
    fn memory_kind_parse_invalid() {
        assert_eq!(MemoryKind::parse("unknown"), None);
        assert_eq!(MemoryKind::parse(""), None);
    }

    #[test]
    fn scope_parse_variants() {
        assert_eq!(Scope::parse_str("repo"), Some(Scope::Repo));
        assert_eq!(Scope::parse_str("user"), Some(Scope::User));
        assert_eq!(Scope::parse_str("mem:repo"), Some(Scope::Repo));
        assert_eq!(Scope::parse_str("project"), Some(Scope::Repo));
        assert_eq!(Scope::parse_str("global"), Some(Scope::User));
    }

    #[test]
    fn severity_parse() {
        assert_eq!(Severity::parse_str("must"), Some(Severity::Must));
        assert_eq!(Severity::parse_str("should"), Some(Severity::Should));
        assert_eq!(Severity::parse_str("prefer"), Some(Severity::Prefer));
        assert_eq!(Severity::parse_str("invalid"), None);
    }
}
