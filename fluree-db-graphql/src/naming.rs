//! IRI → GraphQL identifier.
//!
//! GraphQL names are `/[_A-Za-z][_0-9A-Za-z]*/` and unique within a scope, while
//! IRIs are neither. The rules, in order:
//!
//! 1. `graphql:name` on the shape, if the caller supplies one (tier 3).
//! 2. The IRI's local part: the remainder after the ledger's `@vocab` or a
//!    `@context` prefix, otherwise after the last `#`, `/` or `:`.
//! 3. On collision within the scope, qualify with the context prefix (`foaf_name`).
//! 4. Still colliding, append `_2`, `_3`, … — reached only when two IRIs share both
//!    a local name and a prefix short name, which the sorted iteration makes stable.
//!
//! Assignment order therefore decides who keeps the unqualified name. Callers must
//! feed IRIs in a deterministic order (sorted) so a schema is reproducible.

use std::collections::{HashMap, HashSet};

/// GraphQL type names that the mapping itself owns; a class named `String` or
/// `Query` has to be renamed out of the way.
pub const RESERVED_TYPE_NAMES: &[&str] = &[
    "Query",
    "Mutation",
    "Subscription",
    "Node",
    "String",
    "Int",
    "Long",
    "Float",
    "Decimal",
    "Boolean",
    "ID",
    "DateTime",
    "Date",
    "Time",
    "JSON",
    "SortDirection",
];

/// Field names the mapping itself owns.
pub const RESERVED_FIELD_NAMES: &[&str] = &["id"];

/// Turns IRIs into local names using the ledger's context.
#[derive(Debug, Clone, Default)]
pub struct Namer {
    /// `(namespace IRI, short prefix)`, longest namespace first so the most
    /// specific prefix wins.
    prefixes: Vec<(String, String)>,
    /// The default vocabulary, stripped with no prefix qualifier available.
    vocab: Option<String>,
}

impl Namer {
    /// `context` maps short prefix → namespace IRI, as a JSON-LD `@context` does.
    pub fn new<I>(context: I, vocab: Option<String>) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut prefixes: Vec<(String, String)> = context
            .into_iter()
            .filter(|(short, ns)| !short.is_empty() && !ns.is_empty())
            .map(|(short, ns)| (ns, short))
            .collect();
        // Longest namespace first; ties broken by prefix for determinism.
        prefixes.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.1.cmp(&b.1)));
        prefixes.dedup_by(|a, b| a.0 == b.0);
        Namer { prefixes, vocab }
    }

    /// Split an IRI into `(prefix short name, local part)`.
    ///
    /// The prefix is `None` when the IRI matched `@vocab` or no declared
    /// namespace, in which case rule 3 has nothing to qualify with.
    pub fn split(&self, iri: &str) -> (Option<&str>, String) {
        if let Some(vocab) = &self.vocab {
            if let Some(rest) = iri.strip_prefix(vocab.as_str()) {
                if !rest.is_empty() {
                    return (None, rest.to_string());
                }
            }
        }
        for (ns, short) in &self.prefixes {
            if let Some(rest) = iri.strip_prefix(ns.as_str()) {
                if !rest.is_empty() {
                    return (Some(short.as_str()), rest.to_string());
                }
            }
        }
        (None, last_segment(iri).to_string())
    }
}

impl Namer {
    /// Shorten an IRI to `prefix:local` where a declared prefix matches.
    ///
    /// `@vocab` is deliberately *not* applied here: a bare local name is not
    /// round-trippable without the context, and this is what an `id` field hands
    /// to clients that will hand it back.
    pub fn compact(&self, iri: &str) -> String {
        for (ns, short) in &self.prefixes {
            if let Some(rest) = iri.strip_prefix(ns.as_str()) {
                if !rest.is_empty() && !rest.contains(['/', '#']) {
                    return format!("{short}:{rest}");
                }
            }
        }
        iri.to_string()
    }

    /// Expand `prefix:local` back to an IRI. A value that names no declared
    /// prefix is returned unchanged, so full IRIs pass straight through.
    pub fn expand(&self, value: &str) -> String {
        let Some((prefix, local)) = value.split_once(':') else {
            return value.to_string();
        };
        for (ns, short) in &self.prefixes {
            if short == prefix {
                return format!("{ns}{local}");
            }
        }
        value.to_string()
    }
}

/// The part of an IRI after its last delimiter. Falls back to the whole string,
/// which `sanitize` then makes safe.
fn last_segment(iri: &str) -> &str {
    iri.rsplit_once(['#', '/']).map_or(iri, |(_, rest)| rest)
}

/// Coerce arbitrary text into a GraphQL name, or `None` if nothing usable remains.
pub fn sanitize(raw: &str) -> Option<String> {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    // A leading digit is not a valid name start.
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    // `__` is reserved for introspection.
    while out.starts_with("__") {
        out.remove(0);
    }
    if out.is_empty() || out.chars().all(|c| c == '_') {
        return None;
    }
    Some(out)
}

/// Naive English pluralisation for root list fields.
///
/// Deliberately naive: `graphql:pluralName` overrides it in tier 3, and a wrong
/// guess here costs a slightly odd field name, not correctness.
pub fn pluralize(name: &str) -> String {
    if name.is_empty() {
        return name.to_string();
    }
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("s")
        || lower.ends_with("x")
        || lower.ends_with("z")
        || lower.ends_with("ch")
        || lower.ends_with("sh")
    {
        return format!("{name}es");
    }
    if lower.ends_with("y") && !ends_with_vowel_before_y(&lower) {
        return format!("{}ies", &name[..name.len() - 1]);
    }
    format!("{name}s")
}

fn ends_with_vowel_before_y(lower: &str) -> bool {
    let bytes = lower.as_bytes();
    bytes
        .len()
        .checked_sub(2)
        .is_some_and(|i| matches!(bytes[i], b'a' | b'e' | b'i' | b'o' | b'u'))
}

/// Allocates unique names within one scope (all type names, or one type's fields).
#[derive(Debug)]
pub struct NameScope<'a> {
    namer: &'a Namer,
    taken: HashSet<String>,
    by_iri: HashMap<String, String>,
}

impl<'a> NameScope<'a> {
    pub fn new(namer: &'a Namer, reserved: &[&str]) -> Self {
        NameScope {
            namer,
            taken: reserved.iter().map(|s| (*s).to_string()).collect(),
            by_iri: HashMap::new(),
        }
    }

    /// Claim a name outright (an explicit `graphql:name`, or a synthetic field).
    ///
    /// Returns `false` if it was already taken, leaving the scope unchanged so the
    /// caller can report the conflict rather than silently renaming a declared name.
    pub fn claim(&mut self, iri: &str, name: &str) -> bool {
        if !self.taken.insert(name.to_string()) {
            return false;
        }
        self.by_iri.insert(iri.to_string(), name.to_string());
        true
    }

    /// The name already assigned to `iri`, if any.
    pub fn get(&self, iri: &str) -> Option<&str> {
        self.by_iri.get(iri).map(String::as_str)
    }

    /// Assign a name for `iri`, applying rules 2–4. Idempotent per IRI.
    pub fn assign(&mut self, iri: &str) -> String {
        if let Some(existing) = self.by_iri.get(iri) {
            return existing.clone();
        }
        let (prefix, local) = self.namer.split(iri);
        let base = sanitize(&local)
            .or_else(|| sanitize(iri))
            .unwrap_or_else(|| "_unnamed".to_string());

        let mut candidate = base.clone();
        if self.taken.contains(&candidate) {
            if let Some(qualified) = prefix
                .and_then(sanitize)
                .map(|p| format!("{p}_{base}"))
                .filter(|q| !self.taken.contains(q))
            {
                candidate = qualified;
            } else {
                let mut n = 2;
                while self.taken.contains(&format!("{base}_{n}")) {
                    n += 1;
                }
                candidate = format!("{base}_{n}");
            }
        }
        self.taken.insert(candidate.clone());
        self.by_iri.insert(iri.to_string(), candidate.clone());
        candidate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namer() -> Namer {
        Namer::new(
            [
                ("foaf".to_string(), "http://xmlns.com/foaf/0.1/".to_string()),
                ("ex".to_string(), "http://example.org/".to_string()),
                ("exd".to_string(), "http://example.org/deep/".to_string()),
            ],
            Some("http://example.org/vocab/".to_string()),
        )
    }

    #[test]
    fn splits_on_vocab_then_longest_prefix_then_delimiter() {
        let n = namer();
        assert_eq!(
            n.split("http://example.org/vocab/Person"),
            (None, "Person".into())
        );
        assert_eq!(
            n.split("http://xmlns.com/foaf/0.1/name"),
            (Some("foaf"), "name".into())
        );
        // The deeper namespace wins over the one that is its prefix.
        assert_eq!(
            n.split("http://example.org/deep/thing"),
            (Some("exd"), "thing".into())
        );
        assert_eq!(
            n.split("http://example.org/Widget"),
            (Some("ex"), "Widget".into())
        );
        assert_eq!(
            n.split("http://unknown.example/ns#Gadget"),
            (None, "Gadget".into())
        );
    }

    #[test]
    fn compact_and_expand_round_trip() {
        let n = namer();
        assert_eq!(n.compact("http://xmlns.com/foaf/0.1/name"), "foaf:name");
        assert_eq!(n.compact("http://example.org/Widget"), "ex:Widget");
        // The deeper namespace wins.
        assert_eq!(n.compact("http://example.org/deep/thing"), "exd:thing");
        // `@vocab` is not used for compaction: the result must round-trip.
        assert_eq!(
            n.compact("http://example.org/vocab/Person"),
            "http://example.org/vocab/Person"
        );
        // No declared prefix: unchanged.
        assert_eq!(
            n.compact("http://other.example/x"),
            "http://other.example/x"
        );

        assert_eq!(n.expand("foaf:name"), "http://xmlns.com/foaf/0.1/name");
        assert_eq!(n.expand("http://other.example/x"), "http://other.example/x");
        assert_eq!(n.expand("bare"), "bare");
        for iri in [
            "http://xmlns.com/foaf/0.1/name",
            "http://example.org/Widget",
            "http://example.org/deep/thing",
            "http://other.example/x",
        ] {
            assert_eq!(
                n.expand(&n.compact(iri)),
                iri,
                "round trip failed for {iri}"
            );
        }
    }

    #[test]
    fn sanitize_produces_valid_graphql_names() {
        assert_eq!(sanitize("has-name").as_deref(), Some("has_name"));
        assert_eq!(sanitize("dc.title").as_deref(), Some("dc_title"));
        assert_eq!(sanitize("2nd").as_deref(), Some("_2nd"));
        assert_eq!(sanitize("__typename").as_deref(), Some("_typename"));
        assert_eq!(sanitize(""), None);
        assert_eq!(sanitize("///"), None);
    }

    #[test]
    fn collisions_qualify_with_the_prefix_then_a_counter() {
        let n = namer();
        let mut scope = NameScope::new(&n, RESERVED_FIELD_NAMES);
        assert_eq!(scope.assign("http://example.org/vocab/name"), "name");
        // Same local name, different namespace: qualified with its prefix.
        assert_eq!(scope.assign("http://xmlns.com/foaf/0.1/name"), "foaf_name");
        // No prefix to qualify with, so it falls through to the counter.
        assert_eq!(scope.assign("http://unknown.example/ns#name"), "name_2");
        // A property literally called `id` cannot take the synthetic field's name.
        assert_eq!(scope.assign("http://example.org/vocab/id"), "id_2");
        // Idempotent.
        assert_eq!(scope.assign("http://xmlns.com/foaf/0.1/name"), "foaf_name");
    }

    #[test]
    fn reserved_type_names_are_not_handed_out() {
        let n = namer();
        let mut scope = NameScope::new(&n, RESERVED_TYPE_NAMES);
        // A declared prefix qualifies the clash away.
        assert_eq!(scope.assign("http://example.org/String"), "ex_String");
        // Under `@vocab` there is no prefix to qualify with, so the counter runs.
        assert_eq!(scope.assign("http://example.org/vocab/Query"), "Query_2");
    }

    /// A class named after a scalar has to be renamed, so every scalar's GraphQL
    /// name must be in the reserved list — including any added later.
    #[test]
    fn every_scalar_name_is_reserved() {
        for s in crate::schema::model::Scalar::ALL {
            assert!(
                RESERVED_TYPE_NAMES.contains(&s.type_name()),
                "`{}` is missing from RESERVED_TYPE_NAMES",
                s.type_name()
            );
        }
    }

    #[test]
    fn pluralisation_covers_the_common_endings() {
        assert_eq!(pluralize("person"), "persons");
        assert_eq!(pluralize("company"), "companies");
        assert_eq!(pluralize("day"), "days");
        assert_eq!(pluralize("address"), "addresses");
        assert_eq!(pluralize("box"), "boxes");
        assert_eq!(pluralize("branch"), "branches");
        assert_eq!(pluralize("dish"), "dishes");
        assert_eq!(pluralize(""), "");
    }

    #[test]
    fn claim_reports_conflicts_rather_than_renaming() {
        let n = namer();
        let mut scope = NameScope::new(&n, &[]);
        assert!(scope.claim("http://example.org/A", "Thing"));
        assert!(!scope.claim("http://example.org/B", "Thing"));
        assert_eq!(scope.get("http://example.org/B"), None);
    }
}
