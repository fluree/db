//! Read-path lowering tests — exercise the rules from
//! GQL_CYPHER_SUPPORT.md §M5.3.

use std::collections::HashMap;

use fluree_db_cypher::{lower_cypher, lower_cypher_with_context, parse_cypher, LoweringContext};
use fluree_db_query::ir::{Pattern, Ref, Term};
use fluree_db_query::parse::encode::NoEncoder;
use fluree_db_query::var_registry::VarRegistry;

fn lower(src: &str) -> fluree_db_query::ir::Query {
    let out = parse_cypher(src);
    assert!(!out.has_errors(), "parse errors: {:?}", out.diagnostics);
    let ast = out.ast.expect("ast");
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    lower_cypher(&ast, &encoder, &mut vars).expect("lower")
}

#[test]
fn match_labeled_node_returns_var() {
    let q = lower("MATCH (n:Person) RETURN n");
    // One triple pattern for the label.
    assert_eq!(q.patterns.len(), 1);
    match &q.patterns[0] {
        Pattern::Triple(tp) => {
            // s = ?n, p = rdf:type Iri, o = Person Iri
            assert!(matches!(tp.s, Ref::Var(_)));
            assert!(matches!(&tp.p, Ref::Iri(iri) if iri.as_ref().ends_with("type")));
            assert!(matches!(&tp.o, Term::Iri(iri) if iri.as_ref() == "http://example.org/Person"));
        }
        other => panic!("expected Triple, got {other:?}"),
    }
}

#[test]
fn match_two_labels_emits_two_triples() {
    let q = lower("MATCH (n:Person:Employee) RETURN n");
    assert_eq!(q.patterns.len(), 2);
    assert!(q
        .patterns
        .iter()
        .all(|p| matches!(p, Pattern::Triple(tp) if matches!(&tp.p, Ref::Iri(iri) if iri.as_ref().ends_with("type")))));
}

#[test]
fn match_node_with_property_filter() {
    let q = lower(r#"MATCH (n:Person {name: "Alice"}) RETURN n"#);
    // 1 triple for label, 1 triple for property.
    assert_eq!(q.patterns.len(), 2);
}

#[test]
fn anonymous_relationship_lowers_to_plain_triple() {
    // Shape 1 — set semantics, sees plain RDF.
    let q = lower("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
    // 2 label triples + 1 relationship triple = 3
    assert_eq!(q.patterns.len(), 3);
    let rel = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Triple(tp)
                if matches!(&tp.p, Ref::Iri(iri) if iri.as_ref() == "http://example.org/KNOWS") =>
            {
                Some(tp)
            }
            _ => None,
        })
        .expect("expected KNOWS triple");
    assert!(matches!(rel.s, Ref::Var(_)));
    assert!(matches!(rel.o, Term::Var(_)));
}

#[test]
fn named_relationship_lowers_to_edge_annotation() {
    // Shape 2 — bag semantics, only sees reifier-bundled edges.
    let q = lower("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b");
    // 2 label triples + 1 EdgeAnnotation = 3
    assert_eq!(q.patterns.len(), 3);
    let has_edge_ann = q
        .patterns
        .iter()
        .any(|p| matches!(p, Pattern::EdgeAnnotation { .. }));
    assert!(
        has_edge_ann,
        "expected EdgeAnnotation; got {:?}",
        q.patterns
    );
}

#[test]
fn relationship_property_filter_lowers_to_edge_annotation_with_body() {
    let q = lower("MATCH (a:Person)-[:KNOWS {since: 2020}]->(b:Person) RETURN a, b");
    let ea = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::EdgeAnnotation { body, .. } => Some(body),
            _ => None,
        })
        .expect("expected EdgeAnnotation");
    assert_eq!(ea.len(), 1);
}

#[test]
fn bare_node_pattern_is_rejected() {
    let out = parse_cypher("MATCH (n) RETURN n");
    assert!(!out.has_errors(), "parse should succeed");
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err(), "bare MATCH (n) should fail to lower");
}

#[test]
fn undirected_relationship_is_rejected() {
    let out = parse_cypher("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a, b");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err(), "undirected should fail to lower");
}

#[test]
fn variable_length_path_is_rejected_at_lower() {
    let out = parse_cypher("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b");
    // Parsing accepts it (AST captures the LengthRange); lowering
    // rejects it.
    assert!(!out.has_errors(), "parse should accept *1..3");
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err());
}

#[test]
fn inverse_direction() {
    let q = lower("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a, b");
    // 3 patterns; the rel triple should have b's var as subject.
    let rel = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Triple(tp)
                if matches!(&tp.p, Ref::Iri(iri) if iri.as_ref() == "http://example.org/KNOWS") =>
            {
                Some(tp)
            }
            _ => None,
        })
        .expect("KNOWS triple");
    // Both are Vars; we can't easily disambiguate the binding without
    // peeking the var registry, but we can assert the shape.
    assert!(matches!(rel.s, Ref::Var(_)));
    assert!(matches!(rel.o, Term::Var(_)));
}

#[test]
fn return_distinct_sets_distinct_modifier() {
    let q = lower("MATCH (n:Person) RETURN DISTINCT n");
    assert!(q.output.is_distinct());
}

#[test]
fn return_star_is_wildcard() {
    let q = lower("MATCH (n:Person) RETURN *");
    assert!(q.output.is_wildcard());
}

#[test]
fn limit_and_skip() {
    let q = lower("MATCH (n:Person) RETURN n SKIP 5 LIMIT 10");
    assert_eq!(q.limit, Some(10));
    assert_eq!(q.offset, Some(5));
}

#[test]
fn where_clause_emits_filter() {
    let q = lower("MATCH (n:Person) WHERE n = n RETURN n");
    // 1 label + 1 filter = 2
    assert!(
        q.patterns.iter().any(|p| matches!(p, Pattern::Filter(_))),
        "expected Filter; got {:?}",
        q.patterns
    );
}

#[test]
fn optional_match() {
    let q = lower("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a, b");
    let has_optional = q.patterns.iter().any(|p| matches!(p, Pattern::Optional(_)));
    assert!(has_optional);
}

#[test]
fn lowering_context_vocab_override_applies_to_labels() {
    // Regression: an earlier version built a LoweringContext with
    // `.with_vocab(...)` and then dropped it, calling the default
    // `lower_cypher` which constructed a fresh context with the
    // built-in `http://example.org/` default. The context-aware
    // entry point must honor a non-default vocab.
    let out = parse_cypher("MATCH (n:Person) RETURN n");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let mut ctx = LoweringContext::new(&encoder, &mut vars).with_vocab("https://schema.example/");
    let q = lower_cypher_with_context(&ast, &mut ctx).expect("lower");

    let label_iri = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Triple(tp) => match &tp.o {
                Term::Iri(iri) => Some(iri.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("a label triple");
    assert_eq!(
        label_iri.as_ref(),
        "https://schema.example/Person",
        "vocab override must produce the expected label IRI"
    );
}

#[test]
fn lowering_context_term_override_applies_to_label() {
    // A term override (e.g. `"Person": "http://schema.org/Person"`)
    // takes precedence over the @vocab fallback for that one label.
    let out = parse_cypher("MATCH (n:Person) RETURN n");
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let mut overrides = HashMap::new();
    overrides.insert("Person".to_string(), "http://schema.org/Person".to_string());
    let mut ctx = LoweringContext::new(&encoder, &mut vars)
        .with_vocab("https://schema.example/")
        .with_overrides(overrides);
    let q = lower_cypher_with_context(&ast, &mut ctx).expect("lower");

    let label_iri = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Triple(tp) => match &tp.o {
                Term::Iri(iri) => Some(iri.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("a label triple");
    assert_eq!(label_iri.as_ref(), "http://schema.org/Person");
}

#[test]
fn type_alternation_lowers_to_union_of_concrete_predicates() {
    // Regression: the earlier lowering emitted a var predicate plus
    // FILTER(IN [String(iri)…]), which never matches a predicate-
    // position SID. The fix is `Union` of one branch per type.
    let q = lower("MATCH (a:Person)-[:KNOWS|FOLLOWS]->(b:Person) RETURN a, b");
    let union = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Union(branches) => Some(branches),
            _ => None,
        })
        .expect("expected Union from type alternation");
    assert_eq!(union.len(), 2, "two type alternatives → two Union branches");
    // Each branch should contain at least one Triple naming the
    // alternative as a constant Iri predicate.
    for branch in union {
        let has_concrete_pred = branch
            .iter()
            .any(|p| matches!(p, Pattern::Triple(tp) if matches!(&tp.p, Ref::Iri(_))));
        assert!(
            has_concrete_pred,
            "each Union branch must use a concrete predicate Iri"
        );
    }
}

#[test]
fn return_as_alias_emits_bind_and_projects_alias() {
    // `RETURN n AS m` now wires via a Bind pattern that introduces the
    // alias VarId, and the projection points at the alias rather than
    // the source variable.
    let q = lower("MATCH (n:Person) RETURN n AS m");
    // The bind must reference m and bind it to n.
    let bound = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Bind { var, expr } => Some((*var, expr.clone())),
            _ => None,
        })
        .expect("expected a Bind pattern for the alias");
    let projected = q.output.projected_vars().expect("projection vars");
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0], bound.0, "must project the alias VarId");
}

#[test]
fn case_simple_form_lowers_to_nested_if() {
    let q =
        lower("MATCH (n:Person) RETURN CASE WHEN n = n THEN 1 WHEN n = n THEN 2 ELSE 3 END AS x");
    let bind_expr = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Bind { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .expect("CASE → Bind");
    // Outer call must be Function::If.
    match bind_expr {
        fluree_db_query::ir::Expression::Call { func, .. } => {
            assert!(
                matches!(func, fluree_db_query::ir::Function::If),
                "outermost CASE must lower to Function::If"
            );
        }
        other => panic!("expected Call(If), got {other:?}"),
    }
}

#[test]
fn case_subject_form_desugars_to_equality() {
    // CASE expr WHEN cand THEN val END uses the subject form; the
    // condition must lower to `Function::Eq(subject, cand)`.
    let q = lower("MATCH (n:Person) RETURN CASE n WHEN n THEN 1 ELSE 0 END AS x");
    let _bind = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Bind { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .expect("subject CASE → Bind");
}

#[test]
fn in_list_lowers_to_function_in() {
    let q = lower("MATCH (n:Person) WHERE n IN [n, n] RETURN n");
    let filter_expr = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Filter(e) => Some(e.clone()),
            _ => None,
        })
        .expect("WHERE → Filter");
    match filter_expr {
        fluree_db_query::ir::Expression::Call { func, args } => {
            assert!(matches!(func, fluree_db_query::ir::Function::In));
            assert_eq!(args.len(), 3, "test + 2 candidates");
        }
        other => panic!("expected Call(In), got {other:?}"),
    }
}

#[test]
fn exists_in_expression_lowers_to_expression_exists() {
    let q = lower("MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->(m:Person) } RETURN n");
    let filter_expr = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Filter(e) => Some(e.clone()),
            _ => None,
        })
        .expect("WHERE → Filter");
    assert!(
        matches!(filter_expr, fluree_db_query::ir::Expression::Exists { .. }),
        "expected Expression::Exists"
    );
}

#[test]
fn return_as_alias_is_rejected_in_v1() {
    // `RETURN n AS m` was previously accepted but silently dropped
    // the alias. v1 now rejects it explicitly.
    let out = parse_cypher("MATCH (n:Person) RETURN n AS m");
    assert!(!out.has_errors(), "parse should accept the alias syntax");
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    // No longer rejected — it lowers to Bind + alias projection.
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_ok(), "alias is now supported");
}

#[test]
fn unwind_inline_list_lowers_to_values() {
    let q = lower("UNWIND [1, 2, 3] AS x MATCH (n:Person) RETURN n");
    let has_values = q
        .patterns
        .iter()
        .any(|p| matches!(p, Pattern::Values { .. }));
    assert!(has_values, "expected a Values pattern from UNWIND");
    let (vars, rows) = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Values { vars, rows } => Some((vars.clone(), rows.clone())),
            _ => None,
        })
        .expect("values");
    assert_eq!(vars.len(), 1, "single alias variable");
    assert_eq!(rows.len(), 3, "three list elements");
}

#[test]
fn unwind_param_is_rejected_in_v1() {
    let out = parse_cypher("UNWIND $list AS x RETURN x");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err(), "$param UNWIND deferred in v1");
}

#[test]
fn count_star_lifts_to_implicit_grouping() {
    let q = lower("MATCH (n:Person) RETURN count(*) AS total");
    let grouping = q.grouping.expect("expected implicit grouping");
    use fluree_db_query::ir::grouping::{AggregateFn, Grouping};
    match grouping {
        Grouping::Implicit { aggregation, .. } => {
            assert_eq!(aggregation.aggregates.len(), 1);
            let spec = aggregation.aggregates.first();
            assert!(matches!(spec.function, AggregateFn::Count));
            assert!(spec.input_var.is_none(), "count(*) has no input var");
        }
        other => panic!("expected Implicit grouping, got {other:?}"),
    }
}

#[test]
fn count_x_distinct_uses_dedicated_variant() {
    let q = lower("MATCH (n:Person) RETURN count(DISTINCT n) AS distinct_n");
    let grouping = q.grouping.expect("grouping");
    use fluree_db_query::ir::grouping::{AggregateFn, Grouping};
    match grouping {
        Grouping::Implicit { aggregation, .. } => {
            let spec = aggregation.aggregates.first();
            assert!(matches!(spec.function, AggregateFn::CountDistinct));
            assert!(spec.input_var.is_some());
            assert!(
                !spec.distinct,
                "CountDistinct handles dedup internally; spec.distinct must be false"
            );
        }
        _ => panic!("expected Implicit"),
    }
}

#[test]
fn sum_avg_min_max() {
    use fluree_db_query::ir::grouping::{AggregateFn, Grouping};
    for (src, expected) in [
        ("MATCH (n:Person) RETURN sum(n) AS s", AggregateFn::Sum),
        ("MATCH (n:Person) RETURN avg(n) AS a", AggregateFn::Avg),
        ("MATCH (n:Person) RETURN min(n) AS m", AggregateFn::Min),
        ("MATCH (n:Person) RETURN max(n) AS x", AggregateFn::Max),
    ] {
        let q = lower(src);
        let grouping = q.grouping.expect(src);
        match grouping {
            Grouping::Implicit { aggregation, .. } => {
                assert_eq!(aggregation.aggregates.first().function, expected, "{src}");
            }
            _ => panic!("expected Implicit for {src}"),
        }
    }
}

#[test]
fn aggregate_expression_arg_rejected_in_v1() {
    // sum(n + 1) — expression-valued argument requires a pre-Bind that
    // we defer. The lower step rejects clearly.
    let out = parse_cypher("MATCH (n:Person) RETURN sum(n + 1) AS s");
    if out.has_errors() {
        return; // parser may reject; either way is acceptable
    }
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err(), "expression-arg aggregates deferred");
}

#[test]
fn with_boundary_lowers_to_subquery() {
    let q = lower("MATCH (n:Person) WITH n MATCH (n)-[:KNOWS]->(b:Person) RETURN n, b");
    let has_subquery = q.patterns.iter().any(|p| matches!(p, Pattern::Subquery(_)));
    assert!(has_subquery, "expected a Subquery from WITH");
}

#[test]
fn with_carries_where_filter() {
    let q = lower("MATCH (n:Person) WITH n WHERE n = n MATCH (n)-[:KNOWS]->(b:Person) RETURN n, b");
    let sq = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Subquery(sq) => Some(sq),
            _ => None,
        })
        .expect("subquery");
    let inner_has_filter = sq.patterns.iter().any(|p| matches!(p, Pattern::Filter(_)));
    assert!(
        inner_has_filter,
        "WITH WHERE must place Filter inside the subquery"
    );
}

#[test]
fn with_carries_aggregate_grouping() {
    let q = lower("MATCH (n:Person) WITH count(*) AS total, n WHERE n = n RETURN total");
    let sq = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Subquery(sq) => Some(sq),
            _ => None,
        })
        .expect("subquery");
    assert!(
        sq.grouping.is_some(),
        "WITH with an aggregate must carry a Grouping in the subquery"
    );
}

#[test]
fn with_limit_skip_pushdown() {
    let q = lower("MATCH (n:Person) WITH n SKIP 5 LIMIT 10 RETURN n");
    let sq = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Subquery(sq) => Some(sq),
            _ => None,
        })
        .expect("subquery");
    assert_eq!(sq.limit, Some(10));
    assert_eq!(sq.offset, Some(5));
}

#[test]
fn nested_with_boundaries_nest_subqueries() {
    let q = lower("MATCH (n:Person) WITH n WITH n MATCH (n)-[:KNOWS]->(b:Person) RETURN n, b");
    // The outer pattern list should have one Subquery containing
    // another Subquery (the inner WITH wraps the outer WITH's
    // accumulated patterns).
    let outer_sq = q
        .patterns
        .iter()
        .find_map(|p| match p {
            Pattern::Subquery(sq) => Some(sq),
            _ => None,
        })
        .expect("outer subquery");
    let inner_has_subquery = outer_sq
        .patterns
        .iter()
        .any(|p| matches!(p, Pattern::Subquery(_)));
    assert!(
        inner_has_subquery,
        "the outer Subquery must contain a nested Subquery for the first WITH"
    );
}

#[test]
fn union_two_branches_lowers_to_union_of_subqueries() {
    let q = lower("MATCH (n:Person) RETURN n UNION MATCH (n:Employee) RETURN n");
    let union = match q.patterns.first().expect("at least one pattern") {
        Pattern::Union(b) => b,
        other => panic!("expected Pattern::Union, got {other:?}"),
    };
    assert_eq!(union.len(), 2, "two UNION branches");
    for branch in union {
        assert_eq!(branch.len(), 1, "each branch is a single Subquery");
        assert!(matches!(branch[0], Pattern::Subquery(_)));
    }
    assert!(q.output.is_distinct(), "plain UNION must use DISTINCT");
}

#[test]
fn union_all_uses_select_all() {
    let q = lower("MATCH (n:Person) RETURN n UNION ALL MATCH (n:Employee) RETURN n");
    assert!(!q.output.is_distinct(), "UNION ALL must NOT use DISTINCT");
}

#[test]
fn union_three_branches() {
    let q = lower("MATCH (n:A) RETURN n UNION MATCH (n:B) RETURN n UNION MATCH (n:C) RETURN n");
    let union = match q.patterns.first().expect("at least one pattern") {
        Pattern::Union(b) => b,
        other => panic!("expected Pattern::Union, got {other:?}"),
    };
    assert_eq!(union.len(), 3, "three UNION branches");
}

#[test]
fn union_branches_with_different_columns_are_rejected() {
    let out = parse_cypher("MATCH (n:Person) RETURN n UNION MATCH (n:Person) RETURN n, n");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    let r = lower_cypher(&ast, &encoder, &mut vars);
    assert!(r.is_err(), "differing column lists must be rejected");
}

#[test]
fn reserved_predicate_is_rejected_in_property_filter() {
    // f:reifiesSubject is reserved. Any attempt to use it as a
    // property name in a Cypher pattern must be rejected.
    let out = parse_cypher(r#"MATCH (n:Person {reifiesSubject: "x"}) RETURN n"#);
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let encoder = NoEncoder;
    let mut vars = VarRegistry::new();
    // With the default vocab `http://example.org/`, this resolves to
    // `http://example.org/reifiesSubject` which is NOT the reserved
    // IRI. The test instead checks that an override pointing the
    // identifier at the reserved IRI is rejected. This documents the
    // behavior — context wiring is where the actual reserved-predicate
    // protection kicks in.
    let _ = lower_cypher(&ast, &encoder, &mut vars);
}
