//! Deterministic `StructuredR2rmlMapping` → Turtle renderer.
//!
//! The Turtle is rendered FROM the authoritative IR (never the other way): one
//! `@base` + `@prefix` header, then one block per table in IR order, columns in
//! IR order. Exact column/table casing is preserved; predicate and class IRIs
//! are emitted as prefixed names against the vocab prefix. Output is a pure
//! function of the IR + options, so two runs are byte-identical.
//!
//! TriplesMap node IRIs and `rr:parentTriplesMap` references both use the same
//! `<#PascalCaseStem>` relative form, so they resolve against `@base` to the
//! same absolute IRI and the round-trip join graph reconnects.

use crate::emit::ir::{ColumnMapping, StructuredR2rmlMapping, TableMapping};
use crate::emit::naming;
use crate::emit::EmitOptions;

/// Render the mapping to a Turtle document.
pub fn render_turtle(mapping: &StructuredR2rmlMapping, opts: &EmitOptions) -> String {
    let base = &mapping.base_namespace;
    let vocab_prefix = mapping.vocab_prefix();

    // -- Header: @base then @prefix declarations, in IR order. --
    // The `@base` / `@prefix` IRIs bypass `curie()`, so escape them here too: the
    // vocab prefix's namespace IS the request-controlled `base_namespace`, and an
    // unescaped newline/`>` would otherwise inject arbitrary Turtle statements.
    let mut header = format!("@base <{}> .\n", escape_iri(&opts.map_document_base));
    for p in &mapping.prefixes {
        header.push_str(&format!(
            "@prefix {}: <{}> .\n",
            p.prefix,
            escape_iri(&p.namespace)
        ));
    }

    // -- One block per table. --
    let blocks: Vec<String> = mapping
        .table_mappings
        .iter()
        .map(|tm| render_table(tm, base, vocab_prefix))
        .collect();

    format!("{}\n{}\n", header, blocks.join("\n\n"))
}

/// Render a single TriplesMap block.
fn render_table(tm: &TableMapping, base: &str, vocab_prefix: &str) -> String {
    let node = tm_node_for_table(&tm.table_name);

    let mut stmts: Vec<String> = Vec::with_capacity(tm.columns.len() + 2);
    stmts.push(format!(
        "rr:logicalTable [ rr:tableName \"{}\" ]",
        escape_turtle_string(&tm.table_name)
    ));
    stmts.push(render_subject(tm, base, vocab_prefix));
    for col in &tm.columns {
        stmts.push(render_pom(col, base, vocab_prefix));
    }

    format!(
        "<#{}> a rr:TriplesMap ;\n  {} .",
        escape_iri(&node),
        stmts.join(" ;\n  ")
    )
}

/// Render the `rr:subjectMap`. Falls back to a class-only subject map when no
/// safe subject key was found (an edge case flagged by `NoSafeSubjectKey`).
fn render_subject(tm: &TableMapping, base: &str, vocab_prefix: &str) -> String {
    let class = curie(&tm.class_iri, base, vocab_prefix);
    if tm.subject_template.is_empty() {
        format!("rr:subjectMap [ rr:class {class} ]")
    } else {
        format!(
            "rr:subjectMap [ rr:template \"{}\" ; rr:class {class} ]",
            escape_turtle_string(&tm.subject_template)
        )
    }
}

/// Render one predicate-object mapping — a join when `foreign_key` is set, a
/// literal otherwise.
fn render_pom(col: &ColumnMapping, base: &str, vocab_prefix: &str) -> String {
    let predicate = curie(&col.predicate_iri, base, vocab_prefix);

    if let Some(fk) = &col.foreign_key {
        let parent_node = tm_node_for_table(&fk.target_table);
        format!(
            "rr:predicateObjectMap [ rr:predicate {predicate} ;\n    rr:objectMap [ \
             rr:parentTriplesMap <#{}> ; rr:joinCondition [ rr:child \"{}\" ; \
             rr:parent \"{}\" ] ] ]",
            escape_iri(&parent_node),
            escape_turtle_string(&fk.child_column),
            escape_turtle_string(&fk.parent_column)
        )
    } else {
        let datatype_clause = col
            .datatype
            .as_ref()
            .map(|dt| format!(" ; rr:datatype {dt}"))
            .unwrap_or_default();
        format!(
            "rr:predicateObjectMap [ rr:predicate {predicate} ; rr:objectMap [ rr:column \"{}\"{} ] ]",
            escape_turtle_string(&col.column_name), datatype_clause
        )
    }
}

/// The `<#PascalCaseStem>` TriplesMap node local name for a logical table name.
fn tm_node_for_table(table_name: &str) -> String {
    let stem = table_name.rsplit('.').next().unwrap_or(table_name);
    naming::triples_map_node(stem)
}

/// Render a full IRI as a prefixed name against the vocab base, or as an
/// absolute `<IRI>` otherwise.
///
/// A prefixed name is emitted ONLY when the local part is a safe `PN_LOCAL`
/// (see [`is_safe_pn_local`]); anything else — an IRI outside the vocab base, or
/// a base-relative local carrying characters that are illegal or injection-prone
/// in a prefixed name (e.g. a class-name override with a quote) — falls back to
/// the escaped absolute `<IRI>` form, which is always valid Turtle.
fn curie(iri: &str, base: &str, vocab_prefix: &str) -> String {
    if let Some(local) = iri.strip_prefix(base) {
        if is_safe_pn_local(local) {
            return format!("{vocab_prefix}:{local}");
        }
    }
    format!("<{}>", escape_iri(iri))
}

/// Escape a string for use inside a Turtle `"..."` (`STRING_LITERAL_QUOTE`)
/// literal: backslash, double-quote, and C0/DEL control characters. This is what
/// keeps an adversarial catalog identifier from either breaking the parse or
/// injecting extra predicate-object maps (a bare `"` would close `rr:column`).
/// A string free of these characters is returned unchanged, so ordinary
/// identifiers render byte-for-byte as before.
fn escape_turtle_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    fluree_graph_ir::syntax::push_string(&mut out, s);
    out
}

/// Escape a string for use inside a Turtle `<...>` `IRIREF`: every character the
/// grammar forbids raw (`< > " { } | ^ \` `` ` ``, space, and control chars)
/// becomes a `\uXXXX` escape (see [`fluree_graph_ir::syntax::escape_iri`]). An
/// IRI free of these is returned unchanged.
fn escape_iri(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    fluree_graph_ir::syntax::push_iri(&mut out, s);
    out
}

/// Whether `local` is safe to emit verbatim as a prefixed-name local part.
///
/// A deliberately conservative subset of `PN_LOCAL` — ASCII letters, digits and
/// `_`, with a letter/`_` first character — every member of which is a valid
/// `PN_LOCAL`. Anything it rejects (punctuation, quotes, leading digits, empty)
/// falls back to the always-valid escaped `<IRI>` form in [`curie`]. This admits
/// every deterministic camelCase/PascalCase local the emitter produces, so the
/// prefixed output is unchanged for normal input.
fn is_safe_pn_local(local: &str) -> bool {
    let mut chars = local.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    local.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_turtle_string_is_noop_for_ordinary_identifiers() {
        assert_eq!(escape_turtle_string("DW.DIM_DATE"), "DW.DIM_DATE");
        assert_eq!(escape_turtle_string("GEOGRAPHY_KEY"), "GEOGRAPHY_KEY");
    }

    #[test]
    fn escape_turtle_string_escapes_quotes_backslash_and_controls() {
        assert_eq!(escape_turtle_string("a\"b"), "a\\\"b");
        assert_eq!(escape_turtle_string("a\\b"), "a\\\\b");
        assert_eq!(escape_turtle_string("a\nb\tc\rd"), "a\\nb\\tc\\rd");
        // A bare NUL becomes a \u escape, not a raw control byte.
        assert_eq!(escape_turtle_string("a\u{0}b"), "a\\u0000b");
    }

    #[test]
    fn escape_iri_is_noop_for_ordinary_iris() {
        let iri = "http://ns.fluree.dev/edw#geographyKey";
        assert_eq!(escape_iri(iri), iri);
    }

    #[test]
    fn escape_iri_escapes_delimiters_and_spaces() {
        assert_eq!(escape_iri("a>b"), "a\\u003Eb");
        assert_eq!(escape_iri("a b"), "a\\u0020b");
        assert_eq!(escape_iri("a\"b"), "a\\u0022b");
        assert_eq!(escape_iri("a\\b"), "a\\u005Cb");
    }

    #[test]
    fn safe_pn_local_accepts_camel_and_pascal_case() {
        assert!(is_safe_pn_local("geographyKey"));
        assert!(is_safe_pn_local("DimDate"));
        assert!(is_safe_pn_local("Order"));
        assert!(is_safe_pn_local("_private"));
    }

    #[test]
    fn safe_pn_local_rejects_injection_and_leading_digit() {
        assert!(!is_safe_pn_local(""));
        assert!(!is_safe_pn_local("2023Revenue")); // leading digit → full-IRI form
        assert!(!is_safe_pn_local("has space"));
        assert!(!is_safe_pn_local("has\"quote"));
        assert!(!is_safe_pn_local("a.b")); // dot → full-IRI form
    }

    #[test]
    fn curie_falls_back_to_escaped_iri_for_unsafe_local() {
        let base = "http://ns.fluree.dev/edw#";
        // Safe local → prefixed name (byte-for-byte as before).
        assert_eq!(curie(&format!("{base}Geography"), base, "v"), "v:Geography");
        // Unsafe local (injected quote) → escaped absolute IRI, never `v:..."`.
        let hostile = format!("{base}Ev\"il");
        let rendered = curie(&hostile, base, "v");
        assert_eq!(rendered, "<http://ns.fluree.dev/edw#Ev\\u0022il>");
        assert!(!rendered.contains('"'));
    }
}
