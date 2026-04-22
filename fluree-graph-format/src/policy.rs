//! Formatting policies
//!
//! These enums control how graphs are formatted to various output formats.

use serde_json::Value as JsonValue;

/// Policy for @context handling in JSON-LD output
#[derive(Clone, Debug, Default)]
pub enum ContextPolicy {
    /// Use an original context from parsing (for CONSTRUCT parity)
    ///
    /// The formatter will include this context verbatim in the output.
    UseOriginal(JsonValue),

    /// Use a caller-provided context
    ///
    /// Useful when you want to override the original context.
    UseProvided(JsonValue),

    /// Generate a minimal context from used prefixes (future)
    ///
    /// The formatter will analyze which prefixes are actually used
    /// and generate a minimal context.
    AutoGenerateMinimal,

    /// No context - output expanded form only
    ///
    /// All IRIs will be full expanded IRIs, no prefixes.
    #[default]
    None,
}

impl ContextPolicy {
    /// Create a policy that uses the original context
    pub fn use_original(context: JsonValue) -> Self {
        ContextPolicy::UseOriginal(context)
    }

    /// Create a policy that uses a provided context
    pub fn use_provided(context: JsonValue) -> Self {
        ContextPolicy::UseProvided(context)
    }

    /// Get the context value if one is set
    pub fn context(&self) -> Option<&JsonValue> {
        match self {
            ContextPolicy::UseOriginal(ctx) | ContextPolicy::UseProvided(ctx) => Some(ctx),
            ContextPolicy::AutoGenerateMinimal | ContextPolicy::None => None,
        }
    }
}

/// Policy for blank node ID formatting
#[derive(Clone, Debug, Default)]
pub enum BlankNodePolicy {
    /// Preserve labeled blank nodes from source, rename anonymous
    ///
    /// Blank nodes with explicit labels (e.g., `_:person1`) keep their labels.
    /// Anonymous blank nodes may be renamed for determinism.
    #[default]
    PreserveLabeled,

    /// Rename all blank nodes deterministically
    ///
    /// All blank nodes get sequential IDs: `_:b0`, `_:b1`, etc.
    /// This ensures stable output regardless of input ordering.
    Deterministic,

    /// Use Fluree-style IDs
    ///
    /// Blank nodes use the format `_:fdb-<ulid>`.
    FlureeStyle,
}

/// How to handle rdf:type predicate in output
#[derive(Clone, Debug, Default)]
pub enum TypeHandling {
    /// Output as @type with string values (JSON-LD native)
    ///
    /// This is the conventional JSON-LD representation:
    /// ```json
    /// {"@id": "...", "@type": "Person"}
    /// ```
    /// or with multiple types:
    /// ```json
    /// {"@id": "...", "@type": ["Person", "Agent"]}
    /// ```
    #[default]
    AsAtType,

    /// Output as rdf:type with {"@id": ...} values (raw RDF)
    ///
    /// This preserves the RDF representation:
    /// ```json
    /// {"@id": "...", "rdf:type": {"@id": "Person"}}
    /// ```
    AsRdfType,
}

impl TypeHandling {
    /// Check if types should be output as @type
    pub fn use_at_type(&self) -> bool {
        matches!(self, TypeHandling::AsAtType)
    }
}
