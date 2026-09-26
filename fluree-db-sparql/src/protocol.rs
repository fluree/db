//! SPARQL 1.1 Protocol dataset parameters, applied to the request text.
//!
//! A protocol request can name its RDF dataset outside the SPARQL string:
//! `default-graph-uri` / `named-graph-uri` for a query (Protocol §2.1.4) and
//! `using-graph-uri` / `using-named-graph-uri` for an update (§2.2.3). These are
//! the same dataset a `FROM` / `FROM NAMED` or `USING` / `USING NAMED` clause
//! would name, so they are applied by rewriting those clauses into the text.
//! Everything downstream — auth scoping, `min-t` waits, ledger refresh,
//! execution, and the transaction log for an update — then sees one dataset.
//!
//! The rewrite is span-based: the text is parsed, and clauses are replaced or
//! inserted at the positions the parser recorded. A request that does not
//! parse is returned unchanged, so the normal path reports the syntax error.

use std::borrow::Cow;

use crate::ast::{QueryBody, UpdateOperation};
use crate::parse::parse_sparql;

/// Why protocol dataset parameters could not be applied to a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolDatasetError {
    /// A parameter value cannot be written as an IRI reference (`<…>`).
    InvalidIri {
        /// The protocol parameter carrying the value.
        param: &'static str,
        /// The rejected value.
        value: String,
    },
    /// An update operation already names its dataset with `USING`,
    /// `USING NAMED`, or `WITH`, which the protocol parameters may not override
    /// (§2.2.3).
    UsingConflict,
    /// The request contains `DELETE WHERE`, whose grammar has no `USING`, so
    /// the parameters cannot scope it. Refused rather than run against the
    /// unscoped default graph.
    DeleteWhereUnscoped,
    /// The query's dataset carries a Fluree time-travel qualifier — a
    /// `FROM … TO …` range or an `@t:`-style pin — that replacing it with the
    /// protocol dataset would silently drop.
    TimePinnedDataset,
    /// The parser did not record where an update's `WHERE` begins, so the
    /// `USING` clauses have nowhere to go. Refused rather than run unscoped.
    UnlocatedWhere,
}

impl std::fmt::Display for ProtocolDatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIri { param, value } => {
                write!(f, "{param} value {value:?} is not a valid IRI")
            }
            Self::UsingConflict => f.write_str(
                "using-graph-uri / using-named-graph-uri may not be combined with an update \
                 operation that has its own USING, USING NAMED, or WITH clause",
            ),
            Self::DeleteWhereUnscoped => f.write_str(
                "using-graph-uri / using-named-graph-uri cannot scope a DELETE WHERE operation; \
                 write it as DELETE { … } WHERE { … }",
            ),
            Self::TimePinnedDataset => f.write_str(
                "default-graph-uri / named-graph-uri would replace a FROM clause that pins a time \
                 (FROM … TO …, or an @t:, @time:, @iso:, @recorded:, @commit: or @snapshot: \
                 suffix); put the pin on the parameter value instead",
            ),
            Self::UnlocatedWhere => f.write_str(
                "using-graph-uri / using-named-graph-uri could not be applied: the update's WHERE \
                 position was not recorded",
            ),
        }
    }
}

impl std::error::Error for ProtocolDatasetError {}

/// Apply `default-graph-uri` / `named-graph-uri` to a query.
///
/// The protocol dataset takes precedence over one in the query (§2.1.4): an
/// existing `FROM` / `FROM NAMED` clause is replaced, otherwise one is inserted.
/// Returns the text unchanged when both lists are empty, when it does not
/// parse, or when it is an update.
pub fn apply_query_dataset<'a>(
    query: &'a str,
    default_graphs: &[String],
    named_graphs: &[String],
) -> Result<Cow<'a, str>, ProtocolDatasetError> {
    if default_graphs.is_empty() && named_graphs.is_empty() {
        return Ok(Cow::Borrowed(query));
    }
    validate_iris("default-graph-uri", default_graphs)?;
    validate_iris("named-graph-uri", named_graphs)?;

    let Some(ast) = parse_sparql(query).ast else {
        return Ok(Cow::Borrowed(query));
    };
    let (dataset, insert_at) = match &ast.body {
        QueryBody::Select(q) => (q.dataset.as_ref(), q.where_clause.span.start),
        QueryBody::Construct(q) => (q.dataset.as_ref(), q.where_clause.span.start),
        QueryBody::Ask(q) => (q.dataset.as_ref(), q.where_clause.span.start),
        QueryBody::Describe(q) => (
            q.dataset.as_ref(),
            q.where_clause
                .as_ref()
                .map_or(q.dataset_offset, |w| w.span.start),
        ),
        QueryBody::Update(_) => return Ok(Cow::Borrowed(query)),
    };

    let clause = dataset_clause("FROM", default_graphs, named_graphs);
    let (start, end, replacement) = match dataset {
        Some(existing) => {
            if existing.to_graph.is_some()
                || has_time_pin(&query[existing.span.start..existing.span.end])
            {
                return Err(ProtocolDatasetError::TimePinnedDataset);
            }
            (existing.span.start, existing.span.end, clause)
        }
        None => (insert_at, insert_at, format!(" {clause} ")),
    };
    Ok(Cow::Owned(splice(query, &[(start, end, replacement)])))
}

/// Apply `using-graph-uri` / `using-named-graph-uri` to an update.
///
/// Each `DELETE`/`INSERT … WHERE` operation gets the corresponding `USING` /
/// `USING NAMED` clauses. Data operations and graph management have no WHERE
/// to scope and are left alone. Returns the text unchanged when both lists are
/// empty or when it does not parse.
pub fn apply_update_using<'a>(
    update: &'a str,
    using_graphs: &[String],
    using_named_graphs: &[String],
) -> Result<Cow<'a, str>, ProtocolDatasetError> {
    if using_graphs.is_empty() && using_named_graphs.is_empty() {
        return Ok(Cow::Borrowed(update));
    }
    validate_iris("using-graph-uri", using_graphs)?;
    validate_iris("using-named-graph-uri", using_named_graphs)?;

    let Some(ast) = parse_sparql(update).ast else {
        return Ok(Cow::Borrowed(update));
    };
    let QueryBody::Update(request) = &ast.body else {
        return Ok(Cow::Borrowed(update));
    };

    let clause = dataset_clause("USING", using_graphs, using_named_graphs);
    let mut edits = Vec::new();
    for op in &request.operations {
        match &op.operation {
            UpdateOperation::Modify(modify) => {
                if modify.using.is_some() || modify.with_iri.is_some() {
                    return Err(ProtocolDatasetError::UsingConflict);
                }
                // The parser always records it; a hand-built AST cannot reach here.
                let Some(where_keyword) = modify.where_keyword else {
                    return Err(ProtocolDatasetError::UnlocatedWhere);
                };
                edits.push((
                    where_keyword.start,
                    where_keyword.start,
                    format!("{clause} "),
                ));
            }
            UpdateOperation::DeleteWhere(_) => {
                return Err(ProtocolDatasetError::DeleteWhereUnscoped);
            }
            _ => {}
        }
    }
    if edits.is_empty() {
        return Ok(Cow::Borrowed(update));
    }
    Ok(Cow::Owned(splice(update, &edits)))
}

/// Whether dataset-clause text names a graph with a time-travel suffix
/// (the sigils `split_time_travel_suffix` accepts). `@` cannot appear in a
/// prefixed name, so only `<…>` IRIs can carry one.
fn has_time_pin(clause: &str) -> bool {
    [
        "@t:",
        "@time:",
        "@iso:",
        "@recorded:",
        "@commit:",
        "@snapshot:",
    ]
    .iter()
    .any(|sigil| clause.contains(sigil))
}

/// `FROM <a> FROM NAMED <b>` / `USING <a> USING NAMED <b>`.
fn dataset_clause(keyword: &str, defaults: &[String], named: &[String]) -> String {
    defaults
        .iter()
        .map(|iri| format!("{keyword} <{iri}>"))
        .chain(named.iter().map(|iri| format!("{keyword} NAMED <{iri}>")))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Reject values that cannot appear inside `<…>` (SPARQL `IRIREF`).
fn validate_iris(param: &'static str, values: &[String]) -> Result<(), ProtocolDatasetError> {
    for value in values {
        let invalid = value.is_empty()
            || value.chars().any(|c| {
                c <= ' ' || matches!(c, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\')
            });
        if invalid {
            return Err(ProtocolDatasetError::InvalidIri {
                param,
                value: value.clone(),
            });
        }
    }
    Ok(())
}

/// Apply non-overlapping `(start, end, replacement)` edits to `text`.
fn splice(text: &str, edits: &[(usize, usize, String)]) -> String {
    let mut sorted: Vec<&(usize, usize, String)> = edits.iter().collect();
    sorted.sort_by_key(|(start, _, _)| *start);
    let mut out = String::with_capacity(text.len() + 64);
    let mut cursor = 0;
    for (start, end, replacement) in sorted {
        out.push_str(&text[cursor..*start]);
        out.push_str(replacement);
        cursor = *end;
    }
    out.push_str(&text[cursor..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::QueryBody;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_string()).collect()
    }

    /// The rewritten text must parse, and name exactly the requested dataset.
    fn dataset_of(query: &str) -> (Vec<String>, Vec<String>) {
        let out = parse_sparql(query);
        let ast = out
            .ast
            .unwrap_or_else(|| panic!("rewritten query must parse: {query}"));
        let dataset = match &ast.body {
            QueryBody::Select(q) => q.dataset.clone(),
            QueryBody::Construct(q) => q.dataset.clone(),
            QueryBody::Ask(q) => q.dataset.clone(),
            QueryBody::Describe(q) => q.dataset.clone(),
            QueryBody::Update(_) => panic!("not a query"),
        }
        .unwrap_or_else(|| panic!("rewritten query must carry a dataset: {query}"));
        let iri = |i: &crate::ast::Iri| match &i.value {
            crate::ast::IriValue::Full(s) => s.to_string(),
            other => panic!("expected a full IRI, got {other:?}"),
        };
        (
            dataset.default_graphs.iter().map(iri).collect(),
            dataset.named_graphs.iter().map(iri).collect(),
        )
    }

    #[test]
    fn inserts_a_dataset_into_every_query_form() {
        let defaults = strings(&["http://ex.org/g1", "http://ex.org/g2"]);
        let named = strings(&["http://ex.org/n1"]);
        for query in [
            "SELECT * WHERE { ?s ?p ?o }",
            "SELECT * { ?s ?p ?o }",
            "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:p ?o } LIMIT 1",
            "ASK { ?s ?p ?o }",
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
            "CONSTRUCT WHERE { ?s ?p ?o }",
            "DESCRIBE ?s WHERE { ?s ?p ?o }",
            "DESCRIBE <http://ex.org/x>",
            "DESCRIBE *",
        ] {
            let rewritten = apply_query_dataset(query, &defaults, &named).unwrap();
            assert_eq!(
                dataset_of(&rewritten),
                (defaults.clone(), named.clone()),
                "{query} -> {rewritten}"
            );
        }
    }

    #[test]
    fn protocol_dataset_replaces_the_query_dataset() {
        let query = "SELECT * FROM <http://ex.org/a> FROM NAMED <http://ex.org/b> \
                     FROM <http://ex.org/c> WHERE { ?s ?p ?o }";
        let rewritten = apply_query_dataset(query, &strings(&["http://ex.org/g"]), &[]).unwrap();
        assert_eq!(
            dataset_of(&rewritten),
            (strings(&["http://ex.org/g"]), vec![]),
            "{rewritten}"
        );
    }

    /// Replacing a time-pinned dataset would silently turn a snapshot or
    /// history read into a current-head read, so it is refused. A pin on the
    /// parameter value itself is fine.
    #[test]
    fn a_time_pinned_dataset_is_not_replaced() {
        let g = strings(&["books:main"]);
        for query in [
            "SELECT * FROM <books:main@t:100> WHERE { ?s ?p ?o }",
            "SELECT * FROM <books:main@iso:2026-01-01T00:00:00Z> WHERE { ?s ?p ?o }",
            "SELECT * FROM NAMED <books:main@commit:bafy> WHERE { ?s ?p ?o }",
            "SELECT * FROM <books:main@t:1> TO <books:main@t:5> WHERE { ?s ?p ?o }",
        ] {
            assert_eq!(
                apply_query_dataset(query, &g, &[]),
                Err(ProtocolDatasetError::TimePinnedDataset),
                "{query}"
            );
        }

        let pinned = strings(&["books:main@t:100"]);
        let rewritten = apply_query_dataset(
            "SELECT * FROM <books:main> WHERE { ?s ?p ?o }",
            &pinned,
            &[],
        )
        .unwrap();
        assert_eq!(dataset_of(&rewritten), (pinned, vec![]), "{rewritten}");
    }

    #[test]
    fn named_graphs_alone_are_applied() {
        let rewritten = apply_query_dataset(
            "SELECT * { GRAPH ?g { ?s ?p ?o } }",
            &[],
            &strings(&["urn:n"]),
        )
        .unwrap();
        assert_eq!(dataset_of(&rewritten), (vec![], strings(&["urn:n"])));
    }

    #[test]
    fn ledger_ids_are_valid_graph_uris() {
        let rewritten =
            apply_query_dataset("SELECT * { ?s ?p ?o }", &strings(&["books:main@t:5"]), &[])
                .unwrap();
        assert_eq!(dataset_of(&rewritten).0, strings(&["books:main@t:5"]));
    }

    #[test]
    fn no_parameters_or_unparseable_text_is_returned_unchanged() {
        assert!(matches!(
            apply_query_dataset("SELECT * { ?s ?p ?o }", &[], &[]),
            Ok(Cow::Borrowed(_))
        ));
        assert!(matches!(
            apply_query_dataset("SELEKT nonsense", &strings(&["urn:g"]), &[]),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn values_that_cannot_be_iris_are_rejected() {
        for bad in ["", "urn:a b", "urn:a>", "<urn:a>", "urn:{x}"] {
            assert!(
                matches!(
                    apply_query_dataset("SELECT * { ?s ?p ?o }", &strings(&[bad]), &[]),
                    Err(ProtocolDatasetError::InvalidIri {
                        param: "default-graph-uri",
                        ..
                    })
                ),
                "{bad:?}"
            );
        }
    }

    fn using_of(update: &str) -> Vec<(Vec<String>, Vec<String>)> {
        let ast = parse_sparql(update)
            .ast
            .unwrap_or_else(|| panic!("rewritten update must parse: {update}"));
        let QueryBody::Update(request) = ast.body else {
            panic!("not an update")
        };
        let iri = |i: &crate::ast::Iri| match &i.value {
            crate::ast::IriValue::Full(s) => s.to_string(),
            other => panic!("expected a full IRI, got {other:?}"),
        };
        request
            .operations
            .iter()
            .filter_map(|op| match &op.operation {
                UpdateOperation::Modify(m) => Some(m.using.as_ref().map_or_else(
                    || (vec![], vec![]),
                    |u| {
                        (
                            u.default_graphs.iter().map(iri).collect(),
                            u.named_graphs.iter().map(iri).collect(),
                        )
                    },
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn using_is_added_to_every_modify_operation() {
        let update = "PREFIX ex: <http://ex.org/>\n\
                      DELETE { ?s ex:p ?o } WHERE { ?s ex:p ?o } ;\n\
                      INSERT DATA { ex:a ex:p 1 } ;\n\
                      DELETE { ?s ex:q ?o } INSERT { ?s ex:r ?o } WHERE { ?s ex:q ?o } ;\n\
                      INSERT { ?s ex:t 1 } WHERE { ?s ex:p ?o }";
        let rewritten =
            apply_update_using(update, &strings(&["urn:g"]), &strings(&["urn:n"])).unwrap();
        let expected = (strings(&["urn:g"]), strings(&["urn:n"]));
        assert_eq!(
            using_of(&rewritten),
            vec![expected.clone(), expected.clone(), expected],
            "{rewritten}"
        );
    }

    #[test]
    fn using_conflicts_with_an_operation_that_names_its_dataset() {
        for update in [
            "DELETE { ?s ?p ?o } USING <urn:x> WHERE { ?s ?p ?o }",
            "DELETE { ?s ?p ?o } USING NAMED <urn:x> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "WITH <urn:x> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }",
        ] {
            assert_eq!(
                apply_update_using(update, &strings(&["urn:g"]), &[]),
                Err(ProtocolDatasetError::UsingConflict),
                "{update}"
            );
        }
    }

    #[test]
    fn delete_where_cannot_be_scoped() {
        assert_eq!(
            apply_update_using("DELETE WHERE { ?s ?p ?o }", &strings(&["urn:g"]), &[]),
            Err(ProtocolDatasetError::DeleteWhereUnscoped)
        );
    }

    #[test]
    fn data_only_updates_are_unchanged() {
        assert!(matches!(
            apply_update_using(
                "INSERT DATA { <urn:a> <urn:p> 1 }",
                &strings(&["urn:g"]),
                &[]
            ),
            Ok(Cow::Borrowed(_))
        ));
    }
}
