//! Reserved JSON-LD keywords, which are not Cypher property keys.
//!
//! Fluree's Cypher surface sits on an RDF model whose JSON-LD serialization
//! uses `@`-prefixed keywords for structure: `@id` is a node's identity,
//! `@type` its classes, `@value`/`@language` the parts of a literal. None of
//! them is a property in the Cypher sense — a Cypher node variable *is* the
//! node, and its labels are `labels(n)`.
//!
//! Left unchecked they lower as ordinary predicates, which fails silently in
//! both directions: a read returns `null` (the property is simply absent) and
//! a write stores a literal predicate spelled `@id`, leaving the node's real
//! identity untouched. Rejecting is safe because a bare `@id` is not a legal
//! Cypher identifier — it has to be backticked — so no ordinary property name
//! can collide with this set.
//!
//! Shared by the read lowering (`fluree-db-cypher`) and the write lowering
//! (`fluree-db-transact`), which resolve predicates independently.

/// The message for a reserved JSON-LD keyword used where Cypher expects a
/// property key, label, or relationship type — `None` for any ordinary name.
///
/// Each message names the working accessor where one exists, which is the
/// whole point: the accessors are documented, but a user reaching for
/// `n.`@id`` has no way to discover them from a `null`.
pub fn reserved_keyword_message(key: &str) -> Option<String> {
    // Every reserved name starts with `@`, which no ordinary Cypher property
    // key can. One byte compare keeps this off the lowering path's cost for
    // the overwhelmingly common case; the match below only runs for a key
    // that is already unusual.
    if !key.starts_with('@') {
        return None;
    }
    let advice = match key {
        "@id" => {
            "a node variable already *is* the node; read its identity with `id(n)` or \
             `elementId(n)`"
        }
        "@type" => {
            "read a node's types with `labels(n)`, match on one with `MATCH (n:Label)`, and \
             add one with `SET n:Label`"
        }
        "@value" | "@language" | "@direction" | "@json" => {
            "a literal's value and language tag are not separately addressable in Cypher — \
             read the property itself"
        }
        "@graph" | "@context" | "@list" | "@set" | "@none" | "@reverse" | "@index" | "@nest"
        | "@base" | "@vocab" | "@container" | "@included" | "@prefix" | "@propagate"
        | "@protected" | "@version" => "it describes JSON-LD document structure, not data",
        _ => return None,
    };
    Some(format!(
        "`{key}` is a reserved JSON-LD keyword, not a Cypher property — {advice}."
    ))
}

#[cfg(test)]
mod tests {
    use super::reserved_keyword_message;

    #[test]
    fn names_the_working_accessor() {
        let m = reserved_keyword_message("@id").expect("@id is reserved");
        assert!(m.contains("id(n)"), "{m}");
        assert!(m.contains("elementId(n)"), "{m}");
        let m = reserved_keyword_message("@type").expect("@type is reserved");
        assert!(m.contains("labels(n)"), "{m}");
    }

    #[test]
    fn ordinary_names_pass() {
        // Including names that merely look adjacent — only the exact
        // keyword set is reserved.
        for key in ["id", "type", "name", "value", "graph", "atid", "id@", "@"] {
            assert!(
                reserved_keyword_message(key).is_none(),
                "`{key}` must not be treated as reserved"
            );
        }
    }
}
