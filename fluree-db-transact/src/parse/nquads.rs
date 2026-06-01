//! N-Quads → TriG conversion.
//!
//! N-Quads (`.nq`) is N-Triples plus an optional 4th *graph label* term per
//! statement: `<s> <p> <o> [<g>] .`. It is **not** a syntactic subset of TriG
//! (which groups named graphs in `GRAPH <g> { ... }` blocks), so the
//! Turtle/TriG parser cannot read it directly. This converter regroups quads
//! into TriG so the existing TriG import path ([`crate::import_trig_commit`])
//! handles them: default-graph triples stay bare, named-graph triples are
//! wrapped in a `GRAPH <g> { ... }` block per distinct graph label.
//!
//! N-Quads uses only absolute IRIs (`<...>`), blank nodes (`_:b`), and literals
//! — there are no `@prefix` declarations — so the conversion is a pure
//! regrouping of byte spans from the original text (no term re-serialization,
//! hence no escape/round-trip hazards).

use crate::error::{Result, TransactError};
use fluree_graph_turtle::{tokenize, Token, TokenKind};

/// Convert an N-Quads document into equivalent TriG text.
///
/// Default-graph quads (3 terms) are emitted as bare triples; quads with a
/// graph label (4 terms) are grouped under `GRAPH <label> { ... }`, preserving
/// first-seen order of graph labels.
pub fn nquads_to_trig(content: &str) -> Result<String> {
    let tokens =
        tokenize(content).map_err(|e| TransactError::Parse(format!("n-quads tokenize: {e}")))?;

    let mut default_triples: Vec<&str> = Vec::new();
    // (graph label text incl. delimiters, triples). Vec preserves first-seen order.
    let mut named: Vec<(&str, Vec<&str>)> = Vec::new();

    // Walk statements: accumulate term tokens until each `.`, then regroup.
    let mut stmt: Vec<&Token> = Vec::new();
    for tok in &tokens {
        match tok.kind {
            TokenKind::Eof => break,
            TokenKind::Dot => {
                if !stmt.is_empty() {
                    classify_statement(content, &stmt, &mut default_triples, &mut named)?;
                    stmt.clear();
                }
            }
            _ => stmt.push(tok),
        }
    }
    if !stmt.is_empty() {
        return Err(TransactError::Parse(
            "n-quads: trailing statement not terminated by '.'".to_string(),
        ));
    }

    // Emit TriG.
    let mut out = String::with_capacity(content.len() + named.len() * 16);
    for triple in default_triples {
        out.push_str(triple);
        out.push_str(" .\n");
    }
    for (label, triples) in named {
        out.push_str("GRAPH ");
        out.push_str(label);
        out.push_str(" {\n");
        for triple in triples {
            out.push_str(triple);
            out.push_str(" .\n");
        }
        out.push_str("}\n");
    }
    Ok(out)
}

/// Split a statement's term tokens into "term groups" and route the triple
/// portion to either the default graph or a named graph.
fn classify_statement<'a>(
    content: &'a str,
    stmt: &[&Token],
    default_triples: &mut Vec<&'a str>,
    named: &mut Vec<(&'a str, Vec<&'a str>)>,
) -> Result<()> {
    let terms = group_terms(stmt)?;
    // subject, predicate, object, [graph]
    if terms.len() != 3 && terms.len() != 4 {
        return Err(TransactError::Parse(format!(
            "n-quads: expected 3 or 4 terms per statement, found {}",
            terms.len()
        )));
    }

    // Triple text spans from the subject's start to the object's end.
    let (triple_start, _) = terms[0];
    let (_, triple_end) = terms[2];
    let triple = &content[triple_start..triple_end];

    if terms.len() == 3 {
        default_triples.push(triple);
    } else {
        // 4th term is the graph label; reuse its original text (`<iri>`).
        let (g_start, g_end) = terms[3];
        let label = &content[g_start..g_end];
        // The TriG parser only accepts an IRI graph label, not a blank node.
        // Blank-node graph labels are valid N-Quads but unsupported on import;
        // reject with a clear error rather than emitting un-parseable TriG.
        if label.starts_with("_:") {
            return Err(TransactError::Parse(format!(
                "n-quads: blank-node graph labels are not supported on import \
                 (found `{label}`); use an IRI graph label"
            )));
        }
        if let Some((_, triples)) = named.iter_mut().find(|(l, _)| *l == label) {
            triples.push(triple);
        } else {
            named.push((label, vec![triple]));
        }
    }
    Ok(())
}

/// Group a statement's tokens into term spans `(start_byte, end_byte)`.
///
/// Most terms are a single token; a string literal may be followed by a
/// datatype (`^^ <iri>`) or a language tag (`@lang`), which are folded into the
/// same term.
fn group_terms(stmt: &[&Token]) -> Result<Vec<(usize, usize)>> {
    let mut terms = Vec::with_capacity(4);
    let mut i = 0;
    while i < stmt.len() {
        let start = stmt[i].start as usize;
        let mut end = stmt[i].end as usize;
        // `StringEscaped` is the lexer's slow-path string token (literals
        // containing `\` escapes); it must fold a following `^^<dt>` / `@lang`
        // into the term exactly like plain `String`/`LongString`. Omitting it
        // mis-groups escaped literals that carry a datatype or language tag.
        let is_string = matches!(
            stmt[i].kind,
            TokenKind::String | TokenKind::LongString | TokenKind::StringEscaped(_)
        );
        i += 1;
        if is_string && i < stmt.len() {
            match stmt[i].kind {
                TokenKind::DoubleCaret => {
                    // datatype: ^^ <iri>
                    i += 1;
                    let dt = stmt.get(i).ok_or_else(|| {
                        TransactError::Parse("n-quads: '^^' not followed by a datatype".to_string())
                    })?;
                    end = dt.end as usize;
                    i += 1;
                }
                TokenKind::LangTag => {
                    end = stmt[i].end as usize;
                    i += 1;
                }
                _ => {}
            }
        }
        terms.push((start, end));
    }
    Ok(terms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_graph_only_is_pass_through_triples() {
        let nq = "<http://ex/a> <http://ex/p> <http://ex/b> .\n\
                  <http://ex/a> <http://ex/name> \"Alice\" .\n";
        let trig = nquads_to_trig(nq).unwrap();
        assert!(trig.contains("<http://ex/a> <http://ex/p> <http://ex/b> ."));
        assert!(trig.contains("<http://ex/a> <http://ex/name> \"Alice\" ."));
        assert!(!trig.contains("GRAPH"));
    }

    #[test]
    fn named_graph_quads_grouped() {
        let nq = "<http://ex/a> <http://ex/name> \"Alice\" .\n\
                  <http://ex/e1> <http://ex/desc> \"login\" <http://ex/g/audit> .\n\
                  <http://ex/e2> <http://ex/desc> \"logout\" <http://ex/g/audit> .\n";
        let trig = nquads_to_trig(nq).unwrap();
        assert!(trig.contains("GRAPH <http://ex/g/audit> {"));
        assert!(trig.contains("<http://ex/e1> <http://ex/desc> \"login\" ."));
        assert!(trig.contains("<http://ex/e2> <http://ex/desc> \"logout\" ."));
        // The default-graph triple stays outside the GRAPH block.
        let graph_pos = trig.find("GRAPH").unwrap();
        assert!(trig[..graph_pos].contains("\"Alice\" ."));
    }

    #[test]
    fn typed_and_lang_literals_keep_their_suffix() {
        let nq = "<http://ex/a> <http://ex/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> <http://ex/g> .\n\
                  <http://ex/a> <http://ex/label> \"hi\"@en <http://ex/g> .\n";
        let trig = nquads_to_trig(nq).unwrap();
        assert!(trig.contains("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer> ."));
        assert!(trig.contains("\"hi\"@en ."));
        assert!(trig.contains("GRAPH <http://ex/g> {"));
    }

    #[test]
    fn escaped_string_literals_keep_datatype_and_lang() {
        // Literals containing `\` escapes lex as `TokenKind::StringEscaped`
        // (the lexer slow path), NOT `String`. They must still fold a trailing
        // `^^<dt>` / `@lang` into the term. Regression: `group_terms` previously
        // omitted `StringEscaped`, so an escaped literal carrying a datatype or
        // language tag mis-grouped (the suffix split into a separate term).
        let nq = "<http://ex/a> <http://ex/note> \"line1\\nline2\"^^<http://www.w3.org/2001/XMLSchema#string> <http://ex/g> .\n\
                  <http://ex/a> <http://ex/label> \"caf\\u00e9\"@fr <http://ex/g> .\n";
        let trig = nquads_to_trig(nq).unwrap();
        assert!(trig.contains("GRAPH <http://ex/g> {"));
        assert!(
            trig.contains("^^<http://www.w3.org/2001/XMLSchema#string> ."),
            "escaped string must keep its datatype suffix; got: {trig}"
        );
        assert!(
            trig.contains("@fr ."),
            "escaped string must keep its language tag; got: {trig}"
        );
    }

    #[test]
    fn blank_node_graph_label_is_rejected() {
        // Blank-node graph labels are valid N-Quads but the TriG parser only
        // accepts IRI graph labels, so import rejects them with a clear error.
        let nq = "<http://ex/a> <http://ex/p> <http://ex/b> _:g1 .\n";
        let err = nquads_to_trig(nq).expect_err("blank-node graph label should be rejected");
        assert!(
            err.to_string()
                .contains("blank-node graph labels are not supported"),
            "expected a clear rejection message, got: {err}"
        );
    }

    #[test]
    fn unterminated_statement_errors() {
        let nq = "<http://ex/a> <http://ex/p> <http://ex/b>\n";
        assert!(nquads_to_trig(nq).is_err());
    }
}
