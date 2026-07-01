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
    let mut header = format!("@base <{}> .\n", opts.map_document_base);
    for p in &mapping.prefixes {
        header.push_str(&format!("@prefix {}: <{}> .\n", p.prefix, p.namespace));
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
        tm.table_name
    ));
    stmts.push(render_subject(tm, base, vocab_prefix));
    for col in &tm.columns {
        stmts.push(render_pom(col, base, vocab_prefix));
    }

    format!("<#{node}> a rr:TriplesMap ;\n  {} .", stmts.join(" ;\n  "))
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
            tm.subject_template
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
             rr:parentTriplesMap <#{parent_node}> ; rr:joinCondition [ rr:child \"{}\" ; \
             rr:parent \"{}\" ] ] ]",
            fk.child_column, fk.parent_column
        )
    } else {
        let datatype_clause = col
            .datatype
            .as_ref()
            .map(|dt| format!(" ; rr:datatype {dt}"))
            .unwrap_or_default();
        format!(
            "rr:predicateObjectMap [ rr:predicate {predicate} ; rr:objectMap [ rr:column \"{}\"{} ] ]",
            col.column_name, datatype_clause
        )
    }
}

/// The `<#PascalCaseStem>` TriplesMap node local name for a logical table name.
fn tm_node_for_table(table_name: &str) -> String {
    let stem = table_name.rsplit('.').next().unwrap_or(table_name);
    naming::triples_map_node(stem)
}

/// Render a full IRI as a prefixed name against the vocab base, or as an
/// absolute `<IRI>` if it does not sit under that base.
fn curie(iri: &str, base: &str, vocab_prefix: &str) -> String {
    match iri.strip_prefix(base) {
        Some(local) => format!("{vocab_prefix}:{local}"),
        None => format!("<{iri}>"),
    }
}
