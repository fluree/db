/// Memory namespace IRI prefix.
pub const MEM_NS: &str = "https://ns.flur.ee/memory#";

// Classes
pub const CLASS_FACT: &str = "https://ns.flur.ee/memory#Fact";
pub const CLASS_DECISION: &str = "https://ns.flur.ee/memory#Decision";
pub const CLASS_CONSTRAINT: &str = "https://ns.flur.ee/memory#Constraint";

// Properties
pub const PROP_CONTENT: &str = "https://ns.flur.ee/memory#content";
pub const PROP_TAG: &str = "https://ns.flur.ee/memory#tag";
pub const PROP_SCOPE: &str = "https://ns.flur.ee/memory#scope";
pub const PROP_SEVERITY: &str = "https://ns.flur.ee/memory#severity";
pub const PROP_ARTIFACT_REF: &str = "https://ns.flur.ee/memory#artifactRef";
pub const PROP_BRANCH: &str = "https://ns.flur.ee/memory#branch";
pub const PROP_CREATED_AT: &str = "https://ns.flur.ee/memory#createdAt";
pub const PROP_RATIONALE: &str = "https://ns.flur.ee/memory#rationale";
pub const PROP_ALTERNATIVES: &str = "https://ns.flur.ee/memory#alternatives";

// Scope IRIs (named graph identifiers)
pub const SCOPE_REPO: &str = "https://ns.flur.ee/memory#repo";
pub const SCOPE_USER: &str = "https://ns.flur.ee/memory#user";

/// Properties that are OPTIONAL in most SPARQL projections for Memory rows.
///
/// Each tuple is `(property_iri, var_name)` where `var_name` is used as `?{var_name}`.
pub const OPTIONAL_PROPS: [(&str, &str); 7] = [
    (PROP_SCOPE, "scope"),
    (PROP_SEVERITY, "severity"),
    (PROP_TAG, "tag"),
    (PROP_ARTIFACT_REF, "artifactRef"),
    (PROP_BRANCH, "branch"),
    (PROP_RATIONALE, "rationale"),
    (PROP_ALTERNATIVES, "alternatives"),
];
