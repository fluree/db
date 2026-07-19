use super::*;
use crate::ast::pattern::ServiceEndpoint;
use crate::ast::update::UpdateOperation;
use crate::ast::{
    BlankNodeValue, DescribeTarget, GroupCondition, IriValue, LiteralValue, OrderDirection,
    OrderExpr, PredicateTerm, SelectModifier, SelectVariable, SelectVariables, SubjectTerm, Term,
    VarOrIri,
};

fn parse(input: &str) -> ParseOutput<SparqlAst> {
    parse_sparql(input)
}

fn assert_parses(input: &str) -> SparqlAst {
    let result = parse(input);
    if result.has_errors() {
        for diag in &result.diagnostics {
            eprintln!("{}: {}", diag.code, diag.message);
        }
        panic!("Parse failed with errors");
    }
    result.ast.expect("Expected AST")
}

#[test]
fn test_simple_select() {
    let ast = assert_parses("SELECT * WHERE { }");
    assert!(matches!(ast.body, QueryBody::Select(_)));
}

#[test]
fn test_select_with_variables() {
    let ast = assert_parses("SELECT ?name ?age WHERE { }");
    if let QueryBody::Select(q) = &ast.body {
        if let SelectVariables::Explicit(vars) = &q.select.variables {
            assert_eq!(vars.len(), 2);
            assert_eq!(vars[0].var().name.as_ref(), "name");
            assert_eq!(vars[1].var().name.as_ref(), "age");
        } else {
            panic!("Expected explicit variables");
        }
    }
}

#[test]
fn test_select_distinct() {
    let ast = assert_parses("SELECT DISTINCT ?x WHERE { }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(q.select.modifier, Some(SelectModifier::Distinct));
    }
}

#[test]
fn test_select_with_expr_alias() {
    // Expression parsing is Phase 4, but we should recognize the (expr AS ?var) pattern
    let ast = assert_parses("SELECT ?x (42 AS ?count) WHERE { }");
    if let QueryBody::Select(q) = &ast.body {
        if let SelectVariables::Explicit(vars) = &q.select.variables {
            assert_eq!(vars.len(), 2);
            assert_eq!(vars[0].var().name.as_ref(), "x");
            // Second should be an Expr alias
            match &vars[1] {
                SelectVariable::Expr { alias, .. } => {
                    assert_eq!(alias.name.as_ref(), "count");
                }
                _ => panic!("Expected SelectVariable::Expr"),
            }
        } else {
            panic!("Expected explicit variables");
        }
    }
}

#[test]
fn test_select_expr_without_as_emits_error() {
    // Expression without AS should emit an error
    let result = parse("SELECT (42) WHERE { }");
    assert!(result.has_errors());
}

#[test]
fn test_prologue() {
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/>
         PREFIX foaf: <http://xmlns.com/foaf/0.1/>
         SELECT * WHERE { }",
    );
    assert_eq!(ast.prologue.prefixes.len(), 2);
    assert!(ast.prologue.get_prefix("ex").is_some());
    assert!(ast.prologue.get_prefix("foaf").is_some());
}

#[test]
fn test_base_declaration() {
    let ast = assert_parses(
        "BASE <http://example.org/>
         SELECT * WHERE { }",
    );
    assert!(ast.prologue.base.is_some());
}

#[test]
fn test_simple_triple_pattern() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 1);
        } else {
            panic!("Expected BGP");
        }
    }
}

#[test]
fn test_triple_pattern_with_iri() {
    let ast = assert_parses("SELECT * WHERE { ?s <http://example.org/name> ?o }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 1);
            assert!(matches!(
                &patterns[0].predicate,
                PredicateTerm::Iri(i) if matches!(&i.value, IriValue::Full(_))
            ));
        }
    }
}

#[test]
fn test_triple_pattern_with_prefixed_name() {
    let ast = assert_parses("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:name ?o }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 1);
        }
    }
}

#[test]
fn test_rdf_type_shorthand() {
    let ast = assert_parses("SELECT * WHERE { ?s a ?type }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let PredicateTerm::Iri(iri) = &patterns[0].predicate {
                assert!(matches!(
                    &iri.value,
                    IriValue::Full(s) if s.as_ref().ends_with("#type")
                ));
            }
        }
    }
}

#[test]
fn test_multiple_triple_patterns() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . ?s2 ?p2 ?o2 }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
        }
    }
}

#[test]
fn test_object_list() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o1, ?o2, ?o3 }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 3);
        }
    }
}

#[test]
fn test_predicate_object_list() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p1 ?o1 ; ?p2 ?o2 }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
        }
    }
}

#[test]
fn test_optional() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o OPTIONAL { ?s ?p2 ?o2 } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[1], GraphPattern::Optional { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_union() {
    let ast = assert_parses("SELECT * WHERE { { ?s ?p1 ?o } UNION { ?s ?p2 ?o } }");
    if let QueryBody::Select(q) = &ast.body {
        assert!(matches!(
            &q.where_clause.pattern,
            GraphPattern::Union { .. }
        ));
    }
}

#[test]
fn test_minus() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o MINUS { ?s ?p2 ?o2 } }");
    if let QueryBody::Select(q) = &ast.body {
        // MINUS should have left = BGP with first triple, right = BGP with second triple
        if let GraphPattern::Minus { left, right, .. } = &q.where_clause.pattern {
            // Left should contain the first triple pattern
            assert!(
                matches!(left.as_ref(), GraphPattern::Bgp { patterns, .. } if patterns.len() == 1)
            );
            // Right should contain the second triple pattern
            assert!(
                matches!(right.as_ref(), GraphPattern::Bgp { patterns, .. } if patterns.len() == 1)
            );
        } else {
            panic!("Expected Minus pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_minus_requires_left_pattern() {
    // MINUS without a preceding pattern should error
    let result = parse("SELECT * WHERE { MINUS { ?s ?p ?o } }");
    assert!(result.has_errors());
}

#[test]
fn test_union_of_subselects_preserves_both_arms() {
    // Regression (azure-chat #42): `{ SELECT ... } UNION { SELECT ... }` used to
    // drop the UNION entirely — the left sub-SELECT was pushed, then the UNION
    // token hit "UNION must follow a pattern" and was discarded, collapsing the
    // query into two independent subqueries. `assert_parses` also guards that no
    // error diagnostic is emitted. Both arms must survive as a Union of SubSelects.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> \
         SELECT ?n WHERE { \
           { SELECT (?x AS ?n) WHERE { ?s ex:name ?x } } UNION \
           { SELECT (?y AS ?n) WHERE { ?t ex:label ?y } } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Union { left, right, .. } = &q.where_clause.pattern {
            assert!(
                matches!(left.as_ref(), GraphPattern::SubSelect { .. }),
                "left arm should be a sub-SELECT, got {left:?}"
            );
            assert!(
                matches!(right.as_ref(), GraphPattern::SubSelect { .. }),
                "right arm should be a sub-SELECT, got {right:?}"
            );
        } else {
            panic!(
                "Expected Union of sub-SELECTs, got {:?}",
                q.where_clause.pattern
            );
        }
    }
}

#[test]
fn test_union_mixed_group_and_subselect_arms() {
    // Either arm may be a sub-SELECT; a plain-group left arm must not swallow
    // the union or drop the sub-SELECT right arm.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> \
         SELECT ?n WHERE { \
           { ?s ex:name ?n } UNION \
           { SELECT (?y AS ?n) WHERE { ?t ex:label ?y } } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Union { left, right, .. } = &q.where_clause.pattern {
            assert!(
                !matches!(left.as_ref(), GraphPattern::SubSelect { .. }),
                "left arm should be a plain group, got {left:?}"
            );
            assert!(
                matches!(right.as_ref(), GraphPattern::SubSelect { .. }),
                "right arm should be a sub-SELECT, got {right:?}"
            );
        } else {
            panic!("Expected Union, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_minus_right_side_subselect_preserved() {
    // Regression (azure-chat #43): the right arm of MINUS being a sub-SELECT was
    // parsed as a bare BGP with the `(expr AS ?v)` projection dropped, so MINUS
    // shared no bound variable with the left and subtracted nothing. The right
    // arm must remain a sub-SELECT.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> \
         SELECT ?n WHERE { \
           ?s ex:name ?n MINUS \
           { SELECT (?y AS ?n) WHERE { ?t ex:hidden ?y } } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Minus { right, .. } = &q.where_clause.pattern {
            assert!(
                matches!(right.as_ref(), GraphPattern::SubSelect { .. }),
                "MINUS right arm should be a sub-SELECT, got {right:?}"
            );
        } else {
            panic!("Expected Minus, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_optional_subselect_preserved() {
    // A sub-SELECT is a valid OPTIONAL body (grammar admits a group here, and a
    // group may be a SubSelect). It must not be flattened to a bare BGP.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> \
         SELECT ?n WHERE { \
           ?s ex:name ?n OPTIONAL \
           { SELECT (COUNT(?t) AS ?c) WHERE { ?t a ex:Thing } } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            let opt = patterns
                .iter()
                .find_map(|p| match p {
                    GraphPattern::Optional { pattern, .. } => Some(pattern),
                    _ => None,
                })
                .expect("expected an OPTIONAL pattern");
            assert!(
                matches!(opt.as_ref(), GraphPattern::SubSelect { .. }),
                "OPTIONAL body should be a sub-SELECT, got {opt:?}"
            );
        } else {
            panic!("Expected Group, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_values_single_var() {
    let ast = assert_parses(r"SELECT * WHERE { VALUES ?x { 1 2 3 } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Values { vars, data, .. } = &q.where_clause.pattern {
            assert_eq!(vars.len(), 1);
            assert_eq!(vars[0].name.as_ref(), "x");
            assert_eq!(data.len(), 3);
            // Check values are integers
            for row in data {
                assert_eq!(row.len(), 1);
                assert!(row[0].is_some());
            }
        } else {
            panic!("Expected Values pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_values_multi_var() {
    let ast = assert_parses(r"SELECT * WHERE { VALUES (?x ?y) { (1 2) (3 4) } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Values { vars, data, .. } = &q.where_clause.pattern {
            assert_eq!(vars.len(), 2);
            assert_eq!(vars[0].name.as_ref(), "x");
            assert_eq!(vars[1].name.as_ref(), "y");
            assert_eq!(data.len(), 2);
            assert_eq!(data[0].len(), 2);
            assert_eq!(data[1].len(), 2);
        } else {
            panic!("Expected Values pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_values_with_undef() {
    let ast = assert_parses(r"SELECT * WHERE { VALUES (?x ?y) { (1 UNDEF) (UNDEF 2) } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Values { vars, data, .. } = &q.where_clause.pattern {
            assert_eq!(vars.len(), 2);
            assert_eq!(data.len(), 2);
            // First row: 1, UNDEF
            assert!(data[0][0].is_some());
            assert!(data[0][1].is_none());
            // Second row: UNDEF, 2
            assert!(data[1][0].is_none());
            assert!(data[1][1].is_some());
        } else {
            panic!("Expected Values pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_values_with_iri() {
    let ast = assert_parses(
        r"SELECT * WHERE { VALUES ?x { <http://example.org/a> <http://example.org/b> } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Values { vars, data, .. } = &q.where_clause.pattern {
            assert_eq!(vars.len(), 1);
            assert_eq!(data.len(), 2);
            // Check that values are IRIs
            for row in data {
                if let Some(Term::Iri(_)) = &row[0] {
                    // Good
                } else {
                    panic!("Expected IRI in VALUES data");
                }
            }
        } else {
            panic!("Expected Values pattern");
        }
    }
}

#[test]
fn test_values_with_strings() {
    let ast = assert_parses(r#"SELECT * WHERE { VALUES ?name { "Alice" "Bob" } }"#);
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Values { vars, data, .. } = &q.where_clause.pattern {
            assert_eq!(vars.len(), 1);
            assert_eq!(data.len(), 2);
        } else {
            panic!("Expected Values pattern");
        }
    }
}

#[test]
fn test_values_in_group() {
    // VALUES after a triple pattern
    let ast = assert_parses(r"SELECT * WHERE { ?s ?p ?o . VALUES ?x { 1 } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::Values { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_subquery_simple() {
    use crate::ast::query::{SelectVariable, SelectVariables};
    let ast = assert_parses("SELECT * WHERE { { SELECT ?x WHERE { ?x ?p ?o } } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            if let SelectVariables::Explicit(vars) = &query.variables {
                assert_eq!(vars.len(), 1);
                match &vars[0] {
                    SelectVariable::Var(v) => assert_eq!(v.name.as_ref(), "x"),
                    other => panic!("Expected Var, got {other:?}"),
                }
            } else {
                panic!("Expected Explicit variables, got Star");
            }
        } else {
            panic!(
                "Expected SubSelect pattern, got {:?}",
                q.where_clause.pattern
            );
        }
    }
}

#[test]
fn test_subquery_star() {
    use crate::ast::query::SelectVariables;
    let ast = assert_parses("SELECT * WHERE { { SELECT * WHERE { ?s ?p ?o } } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            assert!(matches!(query.variables, SelectVariables::Star));
            assert!(!query.distinct);
            assert!(!query.reduced);
        } else {
            panic!("Expected SubSelect pattern");
        }
    }
}

#[test]
fn test_subquery_distinct() {
    let ast = assert_parses("SELECT * WHERE { { SELECT DISTINCT ?x WHERE { ?x ?p ?o } } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            assert!(query.distinct);
            assert!(!query.reduced);
        } else {
            panic!("Expected SubSelect pattern");
        }
    }
}

#[test]
fn test_subquery_with_limit() {
    let ast = assert_parses("SELECT * WHERE { { SELECT ?x WHERE { ?x ?p ?o } LIMIT 10 } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            assert_eq!(query.modifiers.limit.as_ref().map(|l| l.value), Some(10));
        } else {
            panic!("Expected SubSelect pattern");
        }
    }
}

#[test]
fn test_subquery_with_order_by() {
    let ast = assert_parses("SELECT * WHERE { { SELECT ?x WHERE { ?x ?p ?o } ORDER BY ?x } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            let order = query
                .modifiers
                .order_by
                .as_ref()
                .expect("Expected ORDER BY");
            assert_eq!(order.conditions.len(), 1);
            assert_eq!(order.conditions[0].direction, OrderDirection::Asc);
            match &order.conditions[0].expr {
                OrderExpr::Var(v) => assert_eq!(v.name.as_ref(), "x"),
                other => panic!("Expected bare variable ORDER BY, got {other:?}"),
            }
        } else {
            panic!("Expected SubSelect pattern");
        }
    }
}

#[test]
fn test_subquery_with_order_by_desc() {
    let ast =
        assert_parses("SELECT * WHERE { { SELECT ?x WHERE { ?x ?p ?o } ORDER BY DESC(?x) } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            let order = query
                .modifiers
                .order_by
                .as_ref()
                .expect("Expected ORDER BY");
            assert_eq!(order.conditions.len(), 1);
            assert_eq!(order.conditions[0].direction, OrderDirection::Desc);
        } else {
            panic!("Expected SubSelect pattern");
        }
    }
}

#[test]
fn test_subquery_in_group() {
    // Subquery after a triple pattern
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . { SELECT ?x WHERE { ?x a :Thing } } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::SubSelect { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_filter_simple() {
    // FILTER with a simple expression (placeholder for Phase 4)
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(?o > 10) }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::Filter { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_filter_exists() {
    // FILTER EXISTS
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER EXISTS { ?s a :Thing } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::Filter { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_filter_not_exists() {
    // FILTER NOT EXISTS
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER NOT EXISTS { ?s a :Deleted } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::Filter { .. }));
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_bind() {
    // BIND with placeholder expression
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . BIND(?o + 1 AS ?newVal) }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            if let GraphPattern::Bind { var, .. } = &patterns[1] {
                assert_eq!(var.name.as_ref(), "newVal");
            } else {
                panic!("Expected Bind pattern");
            }
        } else {
            panic!("Expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_bind_requires_as() {
    // BIND without AS should error
    let result = parse("SELECT * WHERE { BIND(42) }");
    assert!(result.has_errors());
}

#[test]
fn test_limit() {
    let ast = assert_parses("SELECT * WHERE { } LIMIT 10");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(q.modifiers.limit.as_ref().map(|l| l.value), Some(10));
    }
}

#[test]
fn test_offset() {
    let ast = assert_parses("SELECT * WHERE { } OFFSET 5");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(q.modifiers.offset.as_ref().map(|o| o.value), Some(5));
    }
}

#[test]
fn test_limit_offset() {
    let ast = assert_parses("SELECT * WHERE { } LIMIT 10 OFFSET 5");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(q.modifiers.limit.as_ref().map(|l| l.value), Some(10));
        assert_eq!(q.modifiers.offset.as_ref().map(|o| o.value), Some(5));
    }
}

#[test]
fn test_order_by() {
    let ast = assert_parses("SELECT * WHERE { } ORDER BY ?name");
    if let QueryBody::Select(q) = &ast.body {
        let order = q.modifiers.order_by.as_ref().expect("Expected ORDER BY");
        assert_eq!(order.conditions.len(), 1);
    }
}

#[test]
fn test_order_by_desc() {
    let ast = assert_parses("SELECT * WHERE { } ORDER BY DESC(?name)");
    if let QueryBody::Select(q) = &ast.body {
        let order = q.modifiers.order_by.as_ref().expect("Expected ORDER BY");
        assert_eq!(order.conditions[0].direction, OrderDirection::Desc);
    }
}

#[test]
fn test_group_by_single_var() {
    let ast = assert_parses("SELECT ?name WHERE { ?s :name ?name } GROUP BY ?name");
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 1);
        if let GroupCondition::Var(var) = &group_by.conditions[0] {
            assert_eq!(var.name.as_ref(), "name");
        } else {
            panic!("Expected Var condition");
        }
    }
}

#[test]
fn test_group_by_multiple_vars() {
    let ast = assert_parses("SELECT ?a ?b WHERE { ?s :p ?a . ?s :q ?b } GROUP BY ?a ?b");
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 2);
    }
}

#[test]
fn test_group_by_with_expression() {
    let ast = assert_parses("SELECT ?x WHERE { ?s :p ?x } GROUP BY (?x + 1 AS ?y)");
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 1);
        if let GroupCondition::Expr { alias, .. } = &group_by.conditions[0] {
            assert!(alias.is_some());
            assert_eq!(alias.as_ref().unwrap().name.as_ref(), "y");
        } else {
            panic!("Expected Expr condition");
        }
    }
}

#[test]
fn test_group_by_bare_builtin_call() {
    // Issue #1362: `GROUP BY DATATYPE(?v)` — a bare BuiltInCall with no
    // surrounding parens — is a valid SPARQL GroupCondition and must parse
    // (previously it was silently dropped, degrading the query to a single
    // implicit group).
    let ast =
        assert_parses("SELECT (DATATYPE(?v) AS ?dt) WHERE { ?s :p ?v } GROUP BY DATATYPE(?v)");
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 1);
        match &group_by.conditions[0] {
            GroupCondition::Expr { alias, .. } => assert!(
                alias.is_none(),
                "bare BuiltInCall GROUP BY takes no AS alias"
            ),
            other => panic!("Expected Expr condition, got {other:?}"),
        }
    }
}

#[test]
fn test_group_by_bare_builtin_call_then_having() {
    // The self-delimiting function call must stop at the HAVING keyword so the
    // condition loop terminates and HAVING is still parsed.
    let ast = assert_parses(
        "SELECT (STR(?v) AS ?s) WHERE { ?x :p ?v } GROUP BY STR(?v) HAVING (COUNT(?v) > 1)",
    );
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 1);
        assert!(q.modifiers.having.is_some(), "HAVING must still parse");
    }
}

#[test]
fn test_group_by_bare_builtin_then_var() {
    // A bare function-call condition followed by a bare variable condition.
    let ast = assert_parses("SELECT ?b WHERE { ?s :p ?a . ?s :q ?b } GROUP BY DATATYPE(?a) ?b");
    if let QueryBody::Select(q) = &ast.body {
        let group_by = q.modifiers.group_by.as_ref().expect("Expected GROUP BY");
        assert_eq!(group_by.conditions.len(), 2);
        assert!(matches!(
            group_by.conditions[0],
            GroupCondition::Expr { .. }
        ));
        assert!(matches!(group_by.conditions[1], GroupCondition::Var(_)));
    }
}

/// Count the triples across all BGPs in a (possibly grouped) WHERE pattern.
fn bgp_triple_count(pattern: &GraphPattern) -> usize {
    match pattern {
        GraphPattern::Bgp { patterns, .. } => patterns.len(),
        GraphPattern::Group { patterns, .. } => patterns.iter().map(bgp_triple_count).sum(),
        _ => 0,
    }
}

#[test]
fn test_blank_node_property_list_object() {
    // `?s :p [ :q ?o ]` — a blank-node property list in object position must
    // parse into the outer triple plus the nested `_b :q ?o` triple (no longer
    // silently dropped). Both share one synthetic blank-node variable.
    let ast = assert_parses("SELECT ?o WHERE { ?s :p [ :q ?o ] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(
            bgp_triple_count(&q.where_clause.pattern),
            2,
            "blank-node property list must expand to two triples"
        );
    }
}

#[test]
fn test_blank_node_property_list_multi_predicate() {
    // `[ :p ?a ; :q ?b ]` expands to two nested triples on the same node.
    let ast = assert_parses("SELECT ?a ?b WHERE { ?s :has [ :p ?a ; :q ?b ] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(bgp_triple_count(&q.where_clause.pattern), 3);
    }
}

#[test]
fn test_blank_node_property_list_nested() {
    // Nested lists: `[ :p [ :q ?x ] ]` → three triples.
    let ast = assert_parses("SELECT ?x WHERE { ?s :has [ :p [ :q ?x ] ] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(bgp_triple_count(&q.where_clause.pattern), 3);
    }
}

#[test]
fn test_blank_node_property_list_bare_subject() {
    // Bare subject form `[ :p ?o ] .` — the property list IS the content.
    let ast = assert_parses("SELECT ?o WHERE { [ :p ?o ] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(bgp_triple_count(&q.where_clause.pattern), 1);
    }
}

#[test]
fn test_empty_blank_node_still_anonymous() {
    // `[]` stays a plain anonymous blank node (no nested triples).
    let ast = assert_parses("SELECT ?s WHERE { ?s :p [] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(
            bgp_triple_count(&q.where_clause.pattern),
            1,
            "empty [] adds no triples"
        );
    }
}

#[test]
fn test_having_simple() {
    let ast =
        assert_parses("SELECT ?name WHERE { ?s :name ?name } GROUP BY ?name HAVING (?cnt > 5)");
    if let QueryBody::Select(q) = &ast.body {
        let having = q.modifiers.having.as_ref().expect("Expected HAVING");
        assert_eq!(having.conditions.len(), 1);
    }
}

#[test]
fn test_group_by_having_order_by() {
    let ast = assert_parses(
        "SELECT ?name WHERE { ?s :name ?name } GROUP BY ?name HAVING (?cnt > 5) ORDER BY ?name",
    );
    if let QueryBody::Select(q) = &ast.body {
        assert!(q.modifiers.group_by.is_some());
        assert!(q.modifiers.having.is_some());
        assert!(q.modifiers.order_by.is_some());
    }
}

#[test]
fn test_literal_integer() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p 42 }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let Term::Literal(lit) = &patterns[0].object {
                assert!(matches!(lit.value, LiteralValue::Integer(42)));
            }
        }
    }
}

#[test]
fn test_literal_string() {
    let ast = assert_parses(r#"SELECT * WHERE { ?s ?p "hello" }"#);
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let Term::Literal(lit) = &patterns[0].object {
                assert!(matches!(&lit.value, LiteralValue::Simple(s) if s.as_ref() == "hello"));
            }
        }
    }
}

#[test]
fn test_literal_boolean() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p true }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let Term::Literal(lit) = &patterns[0].object {
                assert!(matches!(lit.value, LiteralValue::Boolean(true)));
            }
        }
    }
}

#[test]
fn test_literal_lang_tag() {
    let ast = assert_parses(r#"SELECT * WHERE { ?s ?p "hello"@en }"#);
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let Term::Literal(lit) = &patterns[0].object {
                match &lit.value {
                    LiteralValue::LangTagged { value, lang } => {
                        assert_eq!(value.as_ref(), "hello");
                        assert_eq!(lang.as_ref(), "en");
                    }
                    _ => panic!("Expected LangTagged literal, got {:?}", lit.value),
                }
            }
        }
    }
}

#[test]
fn test_literal_lang_tag_complex() {
    let ast = assert_parses(r#"SELECT * WHERE { ?s ?p "bonjour"@fr-CA }"#);
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            if let Term::Literal(lit) = &patterns[0].object {
                match &lit.value {
                    LiteralValue::LangTagged { value, lang } => {
                        assert_eq!(value.as_ref(), "bonjour");
                        assert_eq!(lang.as_ref(), "fr-CA");
                    }
                    _ => panic!("Expected LangTagged literal, got {:?}", lit.value),
                }
            }
        }
    }
}

#[test]
fn test_blank_node_labeled() {
    let ast = assert_parses("SELECT * WHERE { _:b1 ?p ?o }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            assert!(matches!(
                &patterns[0].subject,
                SubjectTerm::BlankNode(b) if matches!(&b.value, BlankNodeValue::Labeled(l) if l.as_ref() == "b1")
            ));
        }
    }
}

#[test]
fn test_error_missing_where() {
    let result = parse("SELECT *");
    assert!(result.has_errors());
}

#[test]
fn test_error_unclosed_brace() {
    let result = parse("SELECT * WHERE {");
    assert!(result.has_errors());
}

// =========================================================================
// Phase 4: Expression tests
// =========================================================================

#[test]
fn test_filter_expression_comparison() {
    use crate::ast::expr::{BinaryOp, Expression};

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(?o > 10) }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            if let GraphPattern::Filter { expr, .. } = &patterns[1] {
                // Parenthesized expression wrapping a comparison
                if let Expression::Bracketed { inner, .. } = expr {
                    match &**inner {
                        Expression::Binary { op, .. } => {
                            assert_eq!(*op, BinaryOp::Gt);
                        }
                        _ => panic!("Expected binary comparison in FILTER"),
                    }
                } else {
                    panic!("Expected bracketed expression, got {expr:?}");
                }
            } else {
                panic!("Expected Filter pattern");
            }
        }
    }
}

#[test]
fn test_filter_expression_logical() {
    use crate::ast::expr::{BinaryOp, Expression};

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(?o > 0 && ?o < 100) }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("Expected SELECT query body");
    };
    let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern else {
        panic!("Expected GROUP pattern");
    };
    let GraphPattern::Filter { expr, .. } = &patterns[1] else {
        panic!("Expected FILTER pattern");
    };
    let Expression::Bracketed { inner, .. } = expr else {
        panic!("Expected BRACKETED expression");
    };
    let Expression::Binary { op, .. } = &**inner else {
        panic!("Expected AND expression in FILTER");
    };
    assert_eq!(*op, BinaryOp::And);
}

#[test]
fn test_filter_exists_expression() {
    use crate::ast::expr::Expression;

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER EXISTS { ?s a :Thing } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            if let GraphPattern::Filter { expr, .. } = &patterns[1] {
                assert!(
                    matches!(expr, Expression::Exists { .. }),
                    "Expected EXISTS expression, got {expr:?}"
                );
            }
        }
    }
}

#[test]
fn test_filter_not_exists_expression() {
    use crate::ast::expr::Expression;

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER NOT EXISTS { ?s a :Deleted } }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            if let GraphPattern::Filter { expr, .. } = &patterns[1] {
                assert!(
                    matches!(expr, Expression::NotExists { .. }),
                    "Expected NOT EXISTS expression, got {expr:?}"
                );
            }
        }
    }
}

#[test]
fn test_bind_expression() {
    use crate::ast::expr::{BinaryOp, Expression};

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . BIND(?o + 1 AS ?newVal) }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            if let GraphPattern::Bind { expr, var, .. } = &patterns[1] {
                assert_eq!(var.name.as_ref(), "newVal");
                match expr {
                    Expression::Binary { op, .. } => {
                        assert_eq!(*op, BinaryOp::Add);
                    }
                    _ => panic!("Expected binary expression in BIND, got {expr:?}"),
                }
            }
        }
    }
}

#[test]
fn test_bind_function_call() {
    use crate::ast::expr::{Expression, FunctionName};

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . BIND(STR(?o) AS ?strVal) }");
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            if let GraphPattern::Bind { expr, var, .. } = &patterns[1] {
                assert_eq!(var.name.as_ref(), "strVal");
                match expr {
                    Expression::FunctionCall { name, args, .. } => {
                        assert!(matches!(name, FunctionName::Str));
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected function call in BIND, got {expr:?}"),
                }
            }
        }
    }
}

#[test]
fn test_order_by_expression() {
    use crate::ast::expr::Expression;

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o } ORDER BY DESC(?o)");
    if let QueryBody::Select(q) = &ast.body {
        let order = q.modifiers.order_by.as_ref().unwrap();
        assert_eq!(order.conditions.len(), 1);
        assert_eq!(order.conditions[0].direction, OrderDirection::Desc);
        // DESC(?o) parses as an expression (the variable inside parens)
        match &order.conditions[0].expr {
            OrderExpr::Expr(e) => {
                assert!(matches!(e, Expression::Var(v) if v.name.as_ref() == "o"));
            }
            OrderExpr::Var(_) => (), // Also acceptable
        }
    }
}

#[test]
fn test_order_by_bare_variable() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o } ORDER BY ?o");
    if let QueryBody::Select(q) = &ast.body {
        let order = q.modifiers.order_by.as_ref().unwrap();
        assert_eq!(order.conditions.len(), 1);
        assert_eq!(order.conditions[0].direction, OrderDirection::Asc);
        // Bare variable should be OrderExpr::Var
        match &order.conditions[0].expr {
            OrderExpr::Var(v) => assert_eq!(v.name.as_ref(), "o"),
            OrderExpr::Expr(_) => panic!("Expected Var for bare variable"),
        }
    }
}

#[test]
fn test_filter_bound_function() {
    use crate::ast::expr::{Expression, FunctionName};

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(BOUND(?o)) }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("Expected SELECT query body");
    };
    let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern else {
        panic!("Expected GROUP pattern");
    };
    let GraphPattern::Filter { expr, .. } = &patterns[1] else {
        panic!("Expected FILTER pattern");
    };
    let Expression::Bracketed { inner, .. } = expr else {
        panic!("Expected BRACKETED expression");
    };
    let Expression::FunctionCall { name, args, .. } = &**inner else {
        panic!("Expected BOUND function call, got {inner:?}");
    };
    assert!(matches!(name, FunctionName::Bound));
    assert_eq!(args.len(), 1);
}

#[test]
fn test_filter_in_expression() {
    use crate::ast::expr::Expression;

    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(?o IN (1, 2, 3)) }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("Expected SELECT query body");
    };
    let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern else {
        panic!("Expected GROUP pattern");
    };
    let GraphPattern::Filter { expr, .. } = &patterns[1] else {
        panic!("Expected FILTER pattern");
    };
    let Expression::Bracketed { inner, .. } = expr else {
        panic!("Expected BRACKETED expression");
    };
    let Expression::In { negated, list, .. } = &**inner else {
        panic!("Expected IN expression, got {inner:?}");
    };
    assert!(!negated);
    assert_eq!(list.len(), 3);
}

// ========================================================================
// Property Path Tests
// ========================================================================

#[test]
fn test_path_one_or_more() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?s ex:parent+ ?ancestor }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::OneOrMore { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_zero_or_more() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?s ex:knows* ?friend }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::ZeroOrMore { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_zero_or_one() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?s ex:nickname? ?name }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::ZeroOrOne { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_inverse() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?child ^ex:parent ?parent }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::Inverse { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_sequence() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?s ex:parent/ex:name ?grandparentName }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::Sequence { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_alternative() {
    use crate::ast::path::PropertyPath;

    let ast = assert_parses("SELECT * WHERE { ?s ex:father|ex:mother ?parent }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => {
                assert!(matches!(path, PropertyPath::Alternative { .. }));
            }
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_path_complex() {
    use crate::ast::path::PropertyPath;

    // Complex path: inverse parent, then one-or-more child
    let ast = assert_parses("SELECT * WHERE { ?s ^ex:parent/ex:child+ ?descendant }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Path { path, .. } => match path {
                PropertyPath::Sequence { left, right, .. } => {
                    assert!(matches!(**left, PropertyPath::Inverse { .. }));
                    assert!(matches!(**right, PropertyPath::OneOrMore { .. }));
                }
                _ => panic!("Expected Sequence path"),
            },
            _ => panic!("Expected Path pattern"),
        }
    }
}

#[test]
fn test_simple_predicate_still_works() {
    // Ensure simple predicates still create BGPs, not paths
    let ast = assert_parses("SELECT * WHERE { ?s ex:name ?name }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Bgp { patterns, .. } => {
                assert_eq!(patterns.len(), 1);
            }
            _ => panic!("Expected BGP for simple predicate"),
        }
    }
}

#[test]
fn test_variable_predicate_still_works() {
    // Variable predicates should remain as simple predicates
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Bgp { patterns, .. } => {
                assert_eq!(patterns.len(), 1);
                assert!(matches!(patterns[0].predicate, PredicateTerm::Var(_)));
            }
            _ => panic!("Expected BGP for variable predicate"),
        }
    }
}

#[test]
fn test_mixed_triples_and_paths() {
    use crate::ast::path::PropertyPath;

    // Mix of simple triples and path patterns
    let ast = assert_parses("SELECT * WHERE { ?s ex:type ex:Person . ?s ex:knows+ ?friend }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Group { patterns, .. } => {
                assert_eq!(patterns.len(), 2);
                // First should be BGP
                assert!(matches!(patterns[0], GraphPattern::Bgp { .. }));
                // Second should be Path
                match &patterns[1] {
                    GraphPattern::Path { path, .. } => {
                        assert!(matches!(path, PropertyPath::OneOrMore { .. }));
                    }
                    _ => panic!("Expected Path pattern as second element"),
                }
            }
            _ => panic!("Expected Group pattern"),
        }
    }
}

#[test]
fn test_path_with_multiple_objects() {
    use crate::ast::path::PropertyPath;

    // Path with object list: ?s path ?o1, ?o2
    let ast = assert_parses("SELECT * WHERE { ?s ex:knows+ ?friend1, ?friend2 }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Group { patterns, .. } => {
                // Should create two path patterns (one per object)
                assert_eq!(patterns.len(), 2);
                for pattern in patterns {
                    match pattern {
                        GraphPattern::Path { path, .. } => {
                            assert!(matches!(path, PropertyPath::OneOrMore { .. }));
                        }
                        _ => panic!("Expected Path pattern"),
                    }
                }
            }
            _ => panic!("Expected Group pattern"),
        }
    }
}

// ========================================================================
// ASK Query Tests
// ========================================================================

#[test]
fn test_ask_simple() {
    let ast = assert_parses("ASK { ?s ex:name \"Alice\" }");
    match &ast.body {
        QueryBody::Ask(q) => {
            // ASK should have a WHERE clause
            assert!(matches!(q.where_clause.pattern, GraphPattern::Bgp { .. }));
        }
        _ => panic!("Expected ASK query"),
    }
}

#[test]
fn test_ask_with_where_keyword() {
    let ast = assert_parses("ASK WHERE { ?s ex:type ex:Person }");
    match &ast.body {
        QueryBody::Ask(q) => {
            assert!(q.where_clause.has_where_keyword);
        }
        _ => panic!("Expected ASK query"),
    }
}

#[test]
fn test_ask_complex_pattern() {
    let ast = assert_parses("ASK { ?s ex:name ?name . FILTER(?name = \"Alice\") }");
    match &ast.body {
        QueryBody::Ask(q) => {
            match &q.where_clause.pattern {
                GraphPattern::Group { patterns, .. } => {
                    assert_eq!(patterns.len(), 2); // BGP + FILTER
                }
                _ => panic!("Expected Group pattern"),
            }
        }
        _ => panic!("Expected ASK query"),
    }
}

// ========================================================================
// DESCRIBE Query Tests
// ========================================================================

#[test]
fn test_describe_star() {
    let ast = assert_parses("DESCRIBE *");
    match &ast.body {
        QueryBody::Describe(q) => {
            assert!(matches!(q.target, DescribeTarget::Star));
            assert!(q.where_clause.is_none());
        }
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_describe_variable() {
    let ast = assert_parses("DESCRIBE ?person");
    match &ast.body {
        QueryBody::Describe(q) => match &q.target {
            DescribeTarget::Resources(resources) => {
                assert_eq!(resources.len(), 1);
                assert!(matches!(&resources[0], VarOrIri::Var(v) if v.name.as_ref() == "person"));
            }
            _ => panic!("Expected Resources target"),
        },
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_describe_iri() {
    let ast = assert_parses("DESCRIBE <http://example.org/alice>");
    match &ast.body {
        QueryBody::Describe(q) => match &q.target {
            DescribeTarget::Resources(resources) => {
                assert_eq!(resources.len(), 1);
                assert!(matches!(&resources[0], VarOrIri::Iri(_)));
            }
            _ => panic!("Expected Resources target"),
        },
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_describe_multiple_resources() {
    let ast = assert_parses("DESCRIBE ?x ?y <http://example.org/z>");
    match &ast.body {
        QueryBody::Describe(q) => match &q.target {
            DescribeTarget::Resources(resources) => {
                assert_eq!(resources.len(), 3);
            }
            _ => panic!("Expected Resources target"),
        },
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_describe_with_where() {
    let ast = assert_parses("DESCRIBE ?x WHERE { ?x ex:type ex:Person }");
    match &ast.body {
        QueryBody::Describe(q) => {
            assert!(q.where_clause.is_some());
            match &q.target {
                DescribeTarget::Resources(resources) => {
                    assert_eq!(resources.len(), 1);
                }
                _ => panic!("Expected Resources target"),
            }
        }
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_describe_star_with_where() {
    let ast = assert_parses("DESCRIBE * WHERE { ?s ex:name ?name }");
    match &ast.body {
        QueryBody::Describe(q) => {
            assert!(matches!(q.target, DescribeTarget::Star));
            assert!(q.where_clause.is_some());
        }
        _ => panic!("Expected DESCRIBE query"),
    }
}

// ========================================================================
// CONSTRUCT Query Tests
// ========================================================================

#[test]
fn test_construct_simple() {
    let ast = assert_parses("CONSTRUCT { ?s ex:knows ?o } WHERE { ?s ex:friend ?o }");
    match &ast.body {
        QueryBody::Construct(q) => {
            assert!(q.template.is_some());
            let template = q.template.as_ref().unwrap();
            assert_eq!(template.triples.len(), 1);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_shorthand() {
    // Shorthand form: CONSTRUCT WHERE { ... }
    let ast = assert_parses("CONSTRUCT WHERE { ?s ex:name ?name }");
    match &ast.body {
        QueryBody::Construct(q) => {
            // Shorthand form has no explicit template
            assert!(q.template.is_none());
            assert!(matches!(q.where_clause.pattern, GraphPattern::Bgp { .. }));
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_multiple_triples() {
    let ast =
        assert_parses("CONSTRUCT { ?s ex:knows ?o . ?o ex:knownBy ?s } WHERE { ?s ex:friend ?o }");
    match &ast.body {
        QueryBody::Construct(q) => {
            let template = q.template.as_ref().unwrap();
            assert_eq!(template.triples.len(), 2);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_template_blank_node_property_list() {
    // A blank-node property list in a CONSTRUCT template must contribute its
    // nested triples (and must not leak them into the WHERE clause). Here the
    // template is `?s ex:has _b . _b ex:label ?l` → 2 triples; the WHERE is one.
    let ast = assert_parses("CONSTRUCT { ?s ex:has [ ex:label ?l ] } WHERE { ?s ex:name ?l }");
    match &ast.body {
        QueryBody::Construct(q) => {
            let template = q.template.as_ref().unwrap();
            assert_eq!(
                template.triples.len(),
                2,
                "CONSTRUCT template blank-node property list must add its nested triple"
            );
            // The WHERE must still be exactly its single triple (no leakage).
            assert_eq!(bgp_triple_count(&q.where_clause.pattern), 1);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_blank_node_property_list_label_no_user_collision() {
    // A user blank node literally named `_:#bnpl0` is impossible (the lexer
    // rejects `#`), so the synthetic property-list node cannot be addressed or
    // accidentally joined. A user label like `_b` near a `[ … ]` stays distinct:
    // `?s :p _:b . ?s :q [ :r ?o ]` → 2 + the user triple = 3 triples, and the
    // synthetic node is its own variable.
    let ast = assert_parses("SELECT ?o WHERE { ?s :p _:b . ?s :q [ :r ?o ] }");
    if let QueryBody::Select(q) = &ast.body {
        assert_eq!(bgp_triple_count(&q.where_clause.pattern), 3);
    }
}

#[test]
fn test_group_by_bare_builtin_does_not_desync() {
    // The bare-builtin GROUP BY fallback must not consume tokens when the
    // following token is not an expression — ORDER BY here must still parse.
    let ast = assert_parses("SELECT ?dt WHERE { ?s :p ?v } GROUP BY DATATYPE(?v) ORDER BY ?dt");
    if let QueryBody::Select(q) = &ast.body {
        assert!(q.modifiers.group_by.is_some());
        assert!(q.modifiers.order_by.is_some(), "ORDER BY must survive");
    }
}

#[test]
fn test_construct_with_predicate_object_list() {
    // Using semicolon to share subject
    let ast = assert_parses(
        "CONSTRUCT { ?s ex:type ex:Person ; ex:name ?name } WHERE { ?s ex:name ?name }",
    );
    match &ast.body {
        QueryBody::Construct(q) => {
            let template = q.template.as_ref().unwrap();
            assert_eq!(template.triples.len(), 2); // Two triples from one subject
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_with_object_list() {
    // Using comma to share predicate
    let ast = assert_parses("CONSTRUCT { ?s ex:knows ?o1, ?o2 } WHERE { ?s ex:friend ?o1, ?o2 }");
    match &ast.body {
        QueryBody::Construct(q) => {
            let template = q.template.as_ref().unwrap();
            assert_eq!(template.triples.len(), 2); // Two triples from comma
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_empty_template() {
    // Empty template is valid SPARQL
    let ast = assert_parses("CONSTRUCT { } WHERE { ?s ?p ?o }");
    match &ast.body {
        QueryBody::Construct(q) => {
            let template = q.template.as_ref().unwrap();
            assert_eq!(template.triples.len(), 0);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_with_limit() {
    let ast = assert_parses("CONSTRUCT { ?s ex:knows ?o } WHERE { ?s ex:friend ?o } LIMIT 10");
    match &ast.body {
        QueryBody::Construct(q) => {
            assert!(q.modifiers.limit.is_some());
            assert_eq!(q.modifiers.limit.as_ref().unwrap().value, 10);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

// ========================================================================
// Dataset Clause Tests (FROM, FROM NAMED)
// ========================================================================

#[test]
fn test_select_with_from() {
    let ast = assert_parses("SELECT * FROM <http://example.org/graph1> WHERE { ?s ?p ?o }");
    match &ast.body {
        QueryBody::Select(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
            assert_eq!(dataset.named_graphs.len(), 0);
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_select_with_from_named() {
    let ast = assert_parses("SELECT * FROM NAMED <http://example.org/graph1> WHERE { ?s ?p ?o }");
    match &ast.body {
        QueryBody::Select(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 0);
            assert_eq!(dataset.named_graphs.len(), 1);
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_select_with_multiple_from() {
    let ast = assert_parses(
        "SELECT * FROM <http://example.org/g1> FROM <http://example.org/g2> WHERE { ?s ?p ?o }",
    );
    match &ast.body {
        QueryBody::Select(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 2);
            assert_eq!(dataset.named_graphs.len(), 0);
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_select_with_mixed_from() {
    let ast = assert_parses(
        "SELECT * FROM <http://example.org/default> FROM NAMED <http://example.org/named> WHERE { ?s ?p ?o }"
    );
    match &ast.body {
        QueryBody::Select(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
            assert_eq!(dataset.named_graphs.len(), 1);
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_ask_with_from() {
    let ast = assert_parses("ASK FROM <http://example.org/graph> { ?s ?p ?o }");
    match &ast.body {
        QueryBody::Ask(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
        }
        _ => panic!("Expected ASK query"),
    }
}

#[test]
fn test_describe_with_from() {
    let ast =
        assert_parses("DESCRIBE ?x FROM <http://example.org/graph> WHERE { ?x ex:name ?name }");
    match &ast.body {
        QueryBody::Describe(q) => {
            assert!(q.dataset.is_some());
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
        }
        _ => panic!("Expected DESCRIBE query"),
    }
}

#[test]
fn test_construct_full_with_from() {
    let ast = assert_parses(
        "CONSTRUCT { ?s ex:knows ?o } FROM <http://example.org/graph> WHERE { ?s ex:friend ?o }",
    );
    match &ast.body {
        QueryBody::Construct(q) => {
            assert!(q.dataset.is_some());
            assert!(q.template.is_some()); // Full form
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_construct_shorthand_with_from() {
    let ast = assert_parses("CONSTRUCT FROM <http://example.org/graph> WHERE { ?s ex:name ?name }");
    match &ast.body {
        QueryBody::Construct(q) => {
            assert!(q.dataset.is_some());
            assert!(q.template.is_none()); // Shorthand form
            let dataset = q.dataset.as_ref().unwrap();
            assert_eq!(dataset.default_graphs.len(), 1);
        }
        _ => panic!("Expected CONSTRUCT query"),
    }
}

#[test]
fn test_select_no_dataset() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p ?o }");
    match &ast.body {
        QueryBody::Select(q) => {
            assert!(q.dataset.is_none());
        }
        _ => panic!("Expected SELECT query"),
    }
}

// ========================================================================
// SPARQL Update Tests (Phase 7)
// ========================================================================

/// Extract the operation of a single-operation update request.
///
/// `QueryBody::Update` now carries a full `UpdateRequest` (a `;`-separated
/// operation sequence); most update tests exercise exactly one operation.
fn single_update_op(ast: &crate::ast::SparqlAst) -> &UpdateOperation {
    match &ast.body {
        QueryBody::Update(req) => {
            assert_eq!(
                req.operations.len(),
                1,
                "expected a single-operation update request: {req:?}"
            );
            &req.operations[0].operation
        }
        other => panic!("Expected an update request, got {other:?}"),
    }
}

#[test]
fn test_insert_data_simple() {
    let ast =
        assert_parses("INSERT DATA { <http://example.org/s> <http://example.org/p> \"value\" }");
    match single_update_op(&ast) {
        UpdateOperation::InsertData(insert) => {
            assert_eq!(insert.data.quads.len(), 1);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_insert_data_multiple_triples() {
    let ast = assert_parses(
        "INSERT DATA { <http://example.org/s1> <http://example.org/p> \"v1\" . <http://example.org/s2> <http://example.org/p> \"v2\" }"
    );
    match single_update_op(&ast) {
        UpdateOperation::InsertData(insert) => {
            assert_eq!(insert.data.quads.len(), 2);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_insert_data_prefixed() {
    let ast = assert_parses("PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p \"value\" }");
    match single_update_op(&ast) {
        UpdateOperation::InsertData(insert) => {
            assert_eq!(insert.data.quads.len(), 1);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_delete_data_simple() {
    let ast =
        assert_parses("DELETE DATA { <http://example.org/s> <http://example.org/p> \"value\" }");
    match single_update_op(&ast) {
        UpdateOperation::DeleteData(delete) => {
            assert_eq!(delete.data.quads.len(), 1);
        }
        _ => panic!("Expected DELETE DATA"),
    }
}

#[test]
fn test_insert_data_graph_block() {
    // Issue #1288: INSERT DATA { GRAPH <g> { ... } } (QuadsNotTriples).
    use crate::ast::pattern::GraphName;
    use crate::ast::QuadPatternElement;
    let ast = assert_parses(
        "INSERT DATA { GRAPH <https://example.org/g/1> { <https://example.org/s/1> <https://example.org/p> \"v\" } }"
    );
    match single_update_op(&ast) {
        UpdateOperation::InsertData(insert) => {
            assert_eq!(insert.data.quads.len(), 1);
            match &insert.data.quads[0] {
                QuadPatternElement::Graph { name, triples, .. } => {
                    assert!(matches!(name, GraphName::Iri(_)));
                    assert_eq!(triples.len(), 1);
                }
                _ => panic!("Expected a GRAPH block in INSERT DATA"),
            }
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_insert_data_mixed_default_and_graph() {
    use crate::ast::QuadPatternElement;
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:a ex:p \"d\" . GRAPH <urn:g> { ex:b ex:p \"n\" } }"
    );
    match single_update_op(&ast) {
        UpdateOperation::InsertData(insert) => {
            assert_eq!(insert.data.quads.len(), 2);
            assert!(matches!(
                insert.data.quads[0],
                QuadPatternElement::Triple(_)
            ));
            assert!(matches!(
                insert.data.quads[1],
                QuadPatternElement::Graph { .. }
            ));
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_delete_data_graph_block() {
    use crate::ast::QuadPatternElement;
    let ast = assert_parses(
        "DELETE DATA { GRAPH <urn:g> { <http://example.org/s> <http://example.org/p> \"v\" } }",
    );
    match single_update_op(&ast) {
        UpdateOperation::DeleteData(delete) => {
            assert_eq!(delete.data.quads.len(), 1);
            assert!(matches!(
                delete.data.quads[0],
                QuadPatternElement::Graph { .. }
            ));
        }
        _ => panic!("Expected DELETE DATA"),
    }
}

#[test]
fn test_delete_where_simple() {
    let ast = assert_parses("DELETE WHERE { ?s ex:obsolete ?o }");
    match single_update_op(&ast) {
        UpdateOperation::DeleteWhere(delete) => {
            assert_eq!(delete.pattern.patterns.len(), 1);
        }
        _ => panic!("Expected DELETE WHERE"),
    }
}

#[test]
fn test_delete_where_multiple_patterns() {
    let ast = assert_parses("DELETE WHERE { ?s ex:old ?o . ?s ex:deprecated ?x }");
    match single_update_op(&ast) {
        UpdateOperation::DeleteWhere(delete) => {
            assert_eq!(delete.pattern.patterns.len(), 2);
        }
        _ => panic!("Expected DELETE WHERE"),
    }
}

#[test]
fn test_modify_delete_insert() {
    let ast =
        assert_parses("DELETE { ?s ex:old ?o } INSERT { ?s ex:new ?o } WHERE { ?s ex:old ?o }");
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.delete_clause.is_some());
            assert!(modify.insert_clause.is_some());
            // where_clause is now a GraphPattern; a single-BGP body parses as Bgp directly.
            match &modify.where_clause {
                crate::ast::GraphPattern::Bgp { patterns, .. } => {
                    assert_eq!(patterns.len(), 1, "expected one triple pattern");
                }
                other => panic!("Expected Bgp, got: {other:?}"),
            }
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_delete_only() {
    let ast = assert_parses("DELETE { ?s ex:old ?o } WHERE { ?s ex:old ?o }");
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.delete_clause.is_some());
            assert!(modify.insert_clause.is_none());
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_insert_only() {
    let ast = assert_parses("INSERT { ?s ex:new ?o } WHERE { ?s ex:old ?o }");
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.delete_clause.is_none());
            assert!(modify.insert_clause.is_some());
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_with_clause() {
    let ast = assert_parses(
        "WITH <http://example.org/graph> DELETE { ?s ex:old ?o } WHERE { ?s ex:old ?o }",
    );
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.with_iri.is_some());
            assert!(modify.delete_clause.is_some());
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_with_using() {
    let ast = assert_parses(
        "DELETE { ?s ex:old ?o } USING <http://example.org/graph> WHERE { ?s ex:old ?o }",
    );
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.using.is_some());
            let using = modify.using.as_ref().unwrap();
            assert_eq!(using.default_graphs.len(), 1);
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_with_multiple_using() {
    let ast = assert_parses(
        "DELETE { ?s ex:old ?o } USING <http://example.org/g1> USING <http://example.org/g2> WHERE { ?s ex:old ?o }",
    );
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.using.is_some());
            let using = modify.using.as_ref().unwrap();
            assert_eq!(using.default_graphs.len(), 2);
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_full() {
    let ast = assert_parses(
        "WITH <http://example.org/graph> DELETE { ?s ex:old ?o } INSERT { ?s ex:new ?o } USING <http://example.org/source> WHERE { ?s ex:old ?o }"
    );
    match single_update_op(&ast) {
        UpdateOperation::Modify(modify) => {
            assert!(modify.with_iri.is_some());
            assert!(modify.delete_clause.is_some());
            assert!(modify.insert_clause.is_some());
            assert!(modify.using.is_some());
        }
        _ => panic!("Expected Modify operation"),
    }
}

// ========================================================================
// RDF Collection (List) Syntax — Desugaring Tests (SPARQL 1.1 §4.2.4)
// ========================================================================

/// Extract the triple patterns of a WHERE clause that is a single BGP.
fn where_bgp_triples(ast: &SparqlAst) -> &[crate::ast::TriplePattern] {
    let QueryBody::Select(q) = &ast.body else {
        panic!("expected SELECT query");
    };
    match &q.where_clause.pattern {
        GraphPattern::Bgp { patterns, .. } => patterns,
        other => panic!("expected a single BGP, got {other:?}"),
    }
}

fn is_full_iri(term_iri: &crate::ast::Iri, expected: &str) -> bool {
    matches!(&term_iri.value, IriValue::Full(s) if &**s == expected)
}

#[test]
fn test_rdf_collection_in_subject_position() {
    // `(1 2 3) ?p ?o` desugars to 3 rdf:first + 3 rdf:rest triples plus the
    // main triple, all in one BGP; the subject is the first list cell.
    let ast = assert_parses("SELECT * WHERE { (1 2 3) ?p ?o }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 7, "3 first + 3 rest + main triple");
    let firsts = triples
        .iter()
        .filter(|t| {
            matches!(&t.predicate, PredicateTerm::Iri(i) if is_full_iri(i, fluree_vocab::rdf::FIRST))
        })
        .count();
    let rests = triples
        .iter()
        .filter(|t| {
            matches!(&t.predicate, PredicateTerm::Iri(i) if is_full_iri(i, fluree_vocab::rdf::REST))
        })
        .count();
    assert_eq!((firsts, rests), (3, 3));
    // The chain terminates in rdf:nil.
    assert!(triples
        .iter()
        .any(|t| { matches!(&t.object, Term::Iri(i) if is_full_iri(i, fluree_vocab::rdf::NIL)) }));
    // The main triple's subject is the head list cell (a blank node).
    assert!(triples
        .iter()
        .any(|t| matches!(&t.subject, SubjectTerm::BlankNode(_))
            && matches!(&t.predicate, PredicateTerm::Var(v) if &*v.name == "p")));
}

#[test]
fn test_rdf_collection_in_object_position() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p (1 2 3) }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 7, "main triple + 3 first + 3 rest");
    // The main triple's object is the head list cell.
    assert!(triples.iter().any(
        |t| matches!(&t.predicate, PredicateTerm::Var(v) if &*v.name == "p")
            && matches!(&t.object, Term::BlankNode(_))
    ));
}

#[test]
fn test_rdf_nil_in_subject_position() {
    // `()` is the IRI rdf:nil — no list triples.
    let ast = assert_parses("SELECT * WHERE { () ?p ?o }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 1);
    assert!(
        matches!(&triples[0].subject, SubjectTerm::Iri(i) if is_full_iri(i, fluree_vocab::rdf::NIL))
    );
}

#[test]
fn test_rdf_nil_in_object_position() {
    let ast = assert_parses("SELECT * WHERE { ?s ?p () }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 1);
    assert!(matches!(&triples[0].object, Term::Iri(i) if is_full_iri(i, fluree_vocab::rdf::NIL)));
}

#[test]
fn test_nested_rdf_collection() {
    // `((1 2) (3 4))` — each inner list desugars to 4 triples, the outer
    // list to 4 more, plus the main triple.
    let ast = assert_parses("SELECT * WHERE { ((1 2) (3 4)) ?p ?o }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 13);
}

#[test]
fn test_rdf_collection_bare_subject() {
    // `TriplesNode PropertyList` — the predicate-object list is optional for
    // a collection subject (W3C syntax-lists-03: `SELECT * WHERE { ( ?z ) }`).
    let ast = assert_parses("SELECT * WHERE { ( ?z ) }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 2, "one first + one rest");
}

#[test]
fn test_rdf_collection_then_more_triples() {
    // The parser continues normally after a collection.
    let ast = assert_parses("SELECT * WHERE { ?s ?p (1 2) . ?x ?y ?z }");
    let triples = where_bgp_triples(&ast);
    assert_eq!(triples.len(), 6, "main + 2 first + 2 rest + second triple");
}

#[test]
fn test_property_path_inside_blank_node_property_list() {
    // `[ :p|:q ?X ]` — a VerbPath inside a blank-node property list emits a
    // Path pattern whose subject is the fresh blank node (W3C test_63).
    let ast = assert_parses("PREFIX : <http://example.org/> SELECT ?X WHERE { [ :p|:q|:r ?X ] }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("expected SELECT query");
    };
    match &q.where_clause.pattern {
        GraphPattern::Path {
            subject, object, ..
        } => {
            assert!(matches!(subject, SubjectTerm::BlankNode(_)));
            assert!(matches!(object, Term::Var(v) if &*v.name == "X"));
        }
        other => panic!("expected a Path pattern, got {other:?}"),
    }
}

#[test]
fn test_values_nil_variable_list() {
    // `VALUES () { }` and `VALUES () { () }` (W3C test_35a / test_36a).
    let ast = assert_parses("SELECT * { } VALUES () { }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("expected SELECT query");
    };
    let values = q.values.as_ref().expect("trailing VALUES clause");
    let GraphPattern::Values { vars, data, .. } = &**values else {
        panic!("expected Values pattern");
    };
    assert!(vars.is_empty());
    assert!(data.is_empty());

    let ast = assert_parses("SELECT * { } VALUES () { () }");
    let QueryBody::Select(q) = &ast.body else {
        panic!("expected SELECT query");
    };
    let values = q.values.as_ref().expect("trailing VALUES clause");
    let GraphPattern::Values { vars, data, .. } = &**values else {
        panic!("expected Values pattern");
    };
    assert!(vars.is_empty());
    assert_eq!(data.len(), 1, "one empty row");
    assert!(data[0].is_empty());
}

#[test]
fn test_extension_function_nil_arg_list() {
    // `f()` — an ArgList may be NIL (W3C syntax-function-01..03).
    assert_parses("PREFIX q: <http://example.org/> SELECT * WHERE { FILTER (q:name()) }");
    assert_parses("PREFIX q: <http://example.org/> SELECT * WHERE { FILTER (q:name( )) }");
    assert_parses("PREFIX q: <http://example.org/> SELECT * WHERE { FILTER (q:name(\n)) }");
}

#[test]
fn test_order_by_bare_builtin_call() {
    // `OrderCondition ::= … | Constraint | Var` — a bare BuiltInCall is a
    // valid ordering condition (W3C syntax-order-07).
    let ast = assert_parses("SELECT * { ?s ?p ?o } ORDER BY str(?o)");
    let QueryBody::Select(q) = &ast.body else {
        panic!("expected SELECT query");
    };
    let order_by = q.modifiers.order_by.as_ref().expect("ORDER BY clause");
    assert_eq!(order_by.conditions.len(), 1);
    assert!(matches!(order_by.conditions[0].expr, OrderExpr::Expr(_)));
}

#[test]
fn test_order_by_bare_expression_not_absorbed_as_key() {
    // `OrderCondition`'s bare form is `Constraint | Var`, never an arbitrary
    // expression. A trailing operator after a var key must NOT become a
    // second (spurious) key: pre-fix `ORDER BY ?x - 1` silently parsed as
    // two keys `[?x, -1]`, corrupting the sort (fluree/db#1452). The `- 1`
    // is not a Constraint, so it is not absorbed — and under the wave-1
    // trailing-token guard (#1438/D-10a) the unconsumed tail is a loud
    // parse error instead of silently ignored input. If absorption ever
    // regressed, these would parse CLEANLY again (two keys, no trailing
    // error) and this test would fail. A bare BuiltInCall like
    // `ORDER BY str(?o)` stays valid, see test_order_by_bare_builtin_call.
    for q in [
        "SELECT * WHERE { ?s ?p ?o } ORDER BY ?x - 1",
        "SELECT * WHERE { ?s ?p ?o } ORDER BY ?x + ?y",
    ] {
        assert_parse_error_no_ast(q, "unexpected trailing tokens");
    }
}

#[test]
fn test_group_by_bare_expression_not_absorbed_as_key() {
    // `GroupCondition`'s bare form is `BuiltInCall | FunctionCall` (or a
    // parenthesized `( … )`, or a Var) — never a bare arithmetic. Kept
    // consistent with ORDER BY (fluree/db#1452): a trailing `- 1` after a
    // var key is not absorbed as a second group key — and under the wave-1
    // trailing-token guard the unconsumed tail is a loud parse error
    // instead of silently ignored input (absorption regressing would parse
    // cleanly with two keys, failing this). The parenthesized
    // `GROUP BY (?x + 1 AS ?y)` form stays valid
    // (see test_group_by_with_expression).
    assert_parse_error_no_ast(
        "SELECT ?x WHERE { ?s :p ?x } GROUP BY ?x - 1",
        "unexpected trailing tokens",
    );
    assert!(
        parse("SELECT ?x WHERE { ?s :p ?x } GROUP BY str(?x) - 1").has_errors(),
        "a bare arithmetic must be rejected as a group condition"
    );
}

#[test]
fn test_empty_iriref() {
    // `<>` is a valid (empty, relative) IRI reference (W3C syntax-qname-05).
    assert_parses("PREFIX : <> SELECT * WHERE { : : : . }");
}

// ── SERVICE pattern tests ──────────────────────────────────────────

#[test]
fn test_service_iri_endpoint() {
    let ast = assert_parses("SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Service {
                silent, endpoint, ..
            } => {
                assert!(!silent);
                assert!(
                    matches!(endpoint, ServiceEndpoint::Iri(iri) if matches!(&iri.value, IriValue::Full(s) if &**s == "http://example.org/sparql"))
                );
            }
            other => panic!("expected Service, got {other:?}"),
        }
    }
}

#[test]
fn test_service_var_endpoint() {
    let ast = assert_parses("SELECT * WHERE { SERVICE ?endpoint { ?s ?p ?o } }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Service {
                silent, endpoint, ..
            } => {
                assert!(!silent);
                assert!(matches!(endpoint, ServiceEndpoint::Var(v) if &*v.name == "endpoint"));
            }
            other => panic!("expected Service, got {other:?}"),
        }
    }
}

#[test]
fn test_service_silent() {
    let ast =
        assert_parses("SELECT * WHERE { SERVICE SILENT <http://example.org/sparql> { ?s ?p ?o } }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Service { silent, .. } => {
                assert!(silent);
            }
            other => panic!("expected Service, got {other:?}"),
        }
    }
}

#[test]
fn test_service_prefixed_endpoint() {
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> SELECT * WHERE { SERVICE ex:sparql { ?s ?p ?o } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        assert!(matches!(
            &q.where_clause.pattern,
            GraphPattern::Service { .. }
        ));
    }
}

#[test]
fn test_service_with_preceding_bgp() {
    let ast = assert_parses(
        "SELECT * WHERE { ?x a <http://example.org/Person> . SERVICE <http://example.org/sparql> { ?x <http://example.org/name> ?name } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GraphPattern::Bgp { .. }));
            assert!(matches!(&patterns[1], GraphPattern::Service { .. }));
        } else {
            panic!("expected Group pattern, got {:?}", q.where_clause.pattern);
        }
    }
}

#[test]
fn test_service_missing_endpoint() {
    let result = parse("SELECT * WHERE { SERVICE { ?s ?p ?o } }");
    assert!(result.has_errors());
}

#[test]
fn test_service_missing_brace() {
    let result = parse("SELECT * WHERE { SERVICE <http://example.org/sparql> ?s ?p ?o }");
    assert!(result.has_errors());
}

#[test]
fn test_service_fluree_ledger_endpoint() {
    let ast = assert_parses("SELECT * WHERE { SERVICE <fluree:ledger:people:main> { ?s ?p ?o } }");
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Service { endpoint, .. } => {
                assert!(
                    matches!(endpoint, ServiceEndpoint::Iri(iri) if matches!(&iri.value, IriValue::Full(s) if &**s == "fluree:ledger:people:main"))
                );
            }
            other => panic!("expected Service, got {other:?}"),
        }
    }
}

// =============================================================================
// M4.2 — RDF 1.2 annotation syntax: parser tests
// =============================================================================

const RDF_PREFIX: &str = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ";
const EX_PREFIX: &str = "PREFIX ex: <http://example.org/> ";

fn unit(ann: &crate::ast::Annotation) -> &crate::ast::AnnotationUnit {
    ann.single_unit().expect("a single annotation unit")
}

fn first_bgp(ast: &SparqlAst) -> &Vec<crate::ast::TriplePattern> {
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::Bgp { patterns, .. } = &q.where_clause.pattern {
            return patterns;
        }
        if let GraphPattern::Group { patterns, .. } = &q.where_clause.pattern {
            for p in patterns {
                if let GraphPattern::Bgp { patterns: tps, .. } = p {
                    return tps;
                }
            }
        }
    }
    panic!("Expected a BGP at the top of the WHERE clause");
}

fn first_pattern_kinds(ast: &SparqlAst) -> Vec<&'static str> {
    let collect = |p: &GraphPattern| -> &'static str {
        match p {
            GraphPattern::Bgp { .. } => "Bgp",
            GraphPattern::Group { .. } => "Group",
            GraphPattern::Optional { .. } => "Optional",
            GraphPattern::Union { .. } => "Union",
            GraphPattern::Minus { .. } => "Minus",
            GraphPattern::Filter { .. } => "Filter",
            GraphPattern::Bind { .. } => "Bind",
            GraphPattern::Values { .. } => "Values",
            GraphPattern::Graph { .. } => "Graph",
            GraphPattern::Service { .. } => "Service",
            GraphPattern::SubSelect { .. } => "SubSelect",
            GraphPattern::Path { .. } => "Path",
            GraphPattern::AnnotationTarget { .. } => "AnnotationTarget",
        }
    };
    if let QueryBody::Select(q) = &ast.body {
        match &q.where_clause.pattern {
            GraphPattern::Group { patterns, .. } => patterns.iter().map(collect).collect(),
            other => vec![collect(other)],
        }
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn annotation_block_anonymous_parses_and_attaches_to_triple() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    assert_eq!(bgp.len(), 1);
    let ann = bgp[0]
        .annotation
        .as_ref()
        .expect("annotation tail should be attached to the triple");
    assert!(
        unit(ann).reifier.is_none(),
        "anonymous form has no reifier id"
    );
    let block = unit(ann).block.as_ref().expect("block should be present");
    assert_eq!(block.entries.len(), 1);
}

#[test]
fn annotation_block_with_named_blank_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ _:ann {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    match unit(ann).reifier.as_ref().expect("reifier id") {
        crate::ast::ReifierId::BlankNode(b) => {
            assert!(matches!(b.value, BlankNodeValue::Labeled(ref l) if l.as_ref() == "ann"));
        }
        other => panic!("expected blank-node reifier, got {other:?}"),
    }
    assert!(unit(ann).block.is_some());
}

#[test]
fn annotation_block_with_named_iri_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ex:rel {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    matches!(
        unit(ann).reifier.as_ref(),
        Some(crate::ast::ReifierId::Iri(_))
    );
}

#[test]
fn annotation_block_with_var_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?ann {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    matches!(
        unit(ann).reifier.as_ref(),
        Some(crate::ast::ReifierId::Var(_))
    );
}

#[test]
fn bare_tilde_reifier_no_block_parses() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?ann . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    assert!(unit(ann).reifier.is_some());
    assert!(unit(ann).block.is_none(), "bare reifier carries no block");
}

#[test]
fn empty_annotation_block_parses() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    let block = unit(ann).block.as_ref().expect("block");
    assert_eq!(block.entries.len(), 0);
}

#[test]
fn annotation_block_with_multiple_predicate_object_pairs() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role \"Engineer\" ; ex:since \"2024\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().unwrap();
    let block = unit(ann).block.as_ref().unwrap();
    assert_eq!(block.entries.len(), 2);
}

#[test]
fn rdf_reifies_with_triple_term_lowers_to_annotation_target_pattern() {
    let ast = assert_parses(&format!(
        "{RDF_PREFIX}{EX_PREFIX}SELECT * WHERE {{ ?ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> . }}"
    ));
    let kinds = first_pattern_kinds(&ast);
    assert!(
        kinds.contains(&"AnnotationTarget"),
        "expected an AnnotationTarget pattern in {kinds:?}"
    );
}

#[test]
fn test_pragma_reasoning_single_mode() {
    let ast = assert_parses("# PRAGMA reasoning: owl2rl\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, Some(vec!["owl2rl".to_string()]));
}

#[test]
fn test_pragma_reasoning_case_insensitive_and_no_colon() {
    let ast = assert_parses("#pragma Reasoning owl2rl\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, Some(vec!["owl2rl".to_string()]));
}

#[test]
fn test_pragma_reasoning_multiple_modes() {
    let ast = assert_parses("# PRAGMA reasoning: rdfs, datalog\nSELECT * WHERE { }");
    assert_eq!(
        ast.pragmas.reasoning,
        Some(vec!["rdfs".to_string(), "datalog".to_string()])
    );
}

#[test]
fn rdf_reifies_with_full_iri_form_recognized() {
    let ast = assert_parses(
        "SELECT * WHERE { ?ann <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> \
         <<( <http://example.org/a> <http://example.org/b> <http://example.org/c> )>> . }",
    );
    let kinds = first_pattern_kinds(&ast);
    assert!(kinds.contains(&"AnnotationTarget"));
}

#[test]
fn rdf_reifies_followed_by_sibling_triples_keeps_them_in_bgp() {
    // The `?ann ex:role "Engineer"` triple is a sibling that should NOT
    // be folded into AnnotationTarget at parse time; it stays as a
    // sibling in the surrounding scope.
    let ast = assert_parses(&format!(
        "{RDF_PREFIX}{EX_PREFIX}SELECT * WHERE {{ ?ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> ; ex:role \"Engineer\" . }}"
    ));
    let kinds = first_pattern_kinds(&ast);
    let n_target = kinds.iter().filter(|k| **k == "AnnotationTarget").count();
    let n_bgp = kinds.iter().filter(|k| **k == "Bgp").count();
    assert_eq!(n_target, 1, "exactly one AnnotationTarget; got {kinds:?}");
    assert!(n_bgp >= 1, "sibling triple stays in a BGP; got {kinds:?}");
}

// ----- Deferred / rejected shapes ------------------------------------------

fn assert_parse_error(input: &str, needle: &str) -> ParseOutput<SparqlAst> {
    let result = parse(input);
    assert!(
        result.has_errors(),
        "expected parse errors for input: {input}"
    );
    let any_match = result
        .diagnostics
        .iter()
        .any(|d| d.message.contains(needle));
    if !any_match {
        for d in &result.diagnostics {
            eprintln!("diag: {} {}", d.code, d.message);
        }
        panic!("expected diagnostic containing {needle:?}");
    }
    result
}

/// Like [`assert_parse_error`], but for rejections that must also suppress
/// AST production (unconsumed trailing input; parse-time semantic rejects):
/// a diagnostics-ignoring caller — e.g. the public `lower_sparql_update_ast`
/// entry — must never receive an AST covering only a prefix of the request
/// (issue #1438).
fn assert_parse_error_no_ast(input: &str, needle: &str) {
    let result = assert_parse_error(input, needle);
    assert!(
        result.ast.is_none(),
        "AST must be suppressed, not recovered, for input: {input}"
    );
}

#[test]
fn annotation_on_literal_object_parses_cleanly() {
    // RDF 1.2 allows annotations on literal-valued triples. The
    // lowering path attaches a `DatatypeConstraint` to the synthesized
    // `TriplePattern.dtc` so the scan matches literal objects by exact
    // datatype / language tag.
    assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:age 30 {{| ex:source \"x\" |}} . }}"
    ));
    assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:name \"Alice\" {{| ex:source \"hr\" |}} . }}"
    ));
    assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:label \"chat\"@fr {{| ex:source \"lex\" |}} . }}"
    ));
}

#[test]
fn triple_term_outside_rdf_reifies_parses_then_defers() {
    // Accept-then-defer (D-1, PR-W2BC): a bare `<<( s p o )>>` triple-term
    // value in object position for a non-`rdf:reifies` predicate now parses
    // (W3C `basic-tripleterm-02`); evaluation is rejected at lower time.
    assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ?ann ex:foo <<( ex:a ex:b ex:c )>> . }}"
    ));
}

#[test]
fn multiple_triple_terms_per_rdf_reifies_is_rejected() {
    assert_parse_error(
        &format!(
            "{RDF_PREFIX}{EX_PREFIX}SELECT * WHERE {{ ?ann rdf:reifies <<( ex:a ex:b ex:c )>>, <<( ex:d ex:e ex:f )>> . }}"
        ),
        "multi-triple",
    );
}

#[test]
fn nested_triple_term_in_subject_is_rejected() {
    // `<<( <<( ... )>> ex:p ex:o )>>` uses a triple term as the inner subject.
    assert_parse_error(
        &format!(
            "{RDF_PREFIX}{EX_PREFIX}SELECT * WHERE {{ ?ann rdf:reifies <<( <<( ex:a ex:b ex:c )>> ex:p ex:o )>> . }}"
        ),
        "nested triple terms",
    );
}

#[test]
fn nested_annotation_in_block_is_rejected() {
    // Annotation on an annotation-block entry is deferred.
    assert_parse_error(
        &format!(
            "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role ex:Eng ~ ?ann2 |}} . }}"
        ),
        "annotations-on-annotations",
    );
}

#[test]
fn multiple_reifiers_in_tail_group_into_units() {
    // RDF 1.2: each `~ reifier` starts a new reification unit; the
    // block attaches to the immediately preceding reifier (`?b` here).
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?a ~ ?b {{| ex:role \"x\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    assert_eq!(ann.units.len(), 2);
    assert!(matches!(
        ann.units[0].reifier,
        Some(crate::ast::ReifierId::Var(ref v)) if v.name.as_ref() == "a"
    ));
    assert!(ann.units[0].block.is_none());
    assert!(matches!(
        ann.units[1].reifier,
        Some(crate::ast::ReifierId::Var(ref v)) if v.name.as_ref() == "b"
    ));
    assert!(ann.units[1].block.is_some(), "block attaches to `~ ?b`");
}

#[test]
fn multiple_blocks_in_tail_mint_fresh_reifiers() {
    // Two blocks with no preceding reifiers = two fresh reifiers
    // (W3C annotation-anonreifier-multiple-01).
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role \"x\" |}} {{| ex:since \"y\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    assert_eq!(ann.units.len(), 2);
    assert!(ann.units.iter().all(|u| u.reifier.is_none()));
    assert!(ann.units.iter().all(|u| u.block.is_some()));
}

#[test]
fn interleaved_reifiers_and_blocks_follow_attachment_rule() {
    // `~ :r1 ~ :r2 {| b1 |} {| b2 |} ~ :r3 {| b3 |}` →
    // [r1], [r2+b1], [fresh+b2], [r3+b3] (annotation-reifier-multiple-05).
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ?s ?p ?o ~ ex:r1 ~ ex:r2 {{| ex:a \"1\" |}} {{| ex:b \"2\" |}} ~ ex:r3 {{| ex:c \"3\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    assert_eq!(ann.units.len(), 4);
    assert!(ann.units[0].reifier.is_some() && ann.units[0].block.is_none());
    assert!(ann.units[1].reifier.is_some() && ann.units[1].block.is_some());
    assert!(ann.units[2].reifier.is_none() && ann.units[2].block.is_some());
    assert!(ann.units[3].reifier.is_some() && ann.units[3].block.is_some());
}

// ----- Existing legacy `<< s p ?o >> f:t ?t` form regression check ---------

#[test]
fn rdf_reifies_triple_term_in_insert_data_parses_then_defers() {
    // Accept-then-defer (D-1, PR-W2BC): the ground triple-term value parses;
    // it is rejected at lower time (transact reports the deferred feature),
    // not at parse. (Cf. the WHERE-only executable `rdf:reifies` form.)
    assert_parses(&format!(
        "{RDF_PREFIX}{EX_PREFIX}INSERT DATA {{ _:ann rdf:reifies <<( ex:a ex:b ex:c )>> }}"
    ));
}

#[test]
fn rdf_reifies_triple_term_in_insert_template_parses_then_defers() {
    // Accept-then-defer (D-1, PR-W2BC): a triple-term value is accepted in
    // an INSERT template regardless of predicate and rejected at lower time,
    // not at parse. (The executable `rdf:reifies` reifier form is WHERE-only;
    // here `<<( … )>>` is a plain deferred triple-term value.)
    assert_parses(&format!(
        "{RDF_PREFIX}{EX_PREFIX}\
         INSERT {{ _:ann rdf:reifies <<( ex:a ex:b ex:c )>> }} WHERE {{ ?s ?p ?o }}"
    ));
}

#[test]
fn legacy_quoted_triple_in_subject_position_still_parses() {
    // The bare `<<` form (no parens) is the Fluree-specific f:t / f:op
    // metadata-extraction shape from `lower/rdf_star.rs`. Adding RDF 1.2
    // tokens must not break it. Lex side already covered; this check
    // confirms parse acceptance of the surrounding triple.
    let ast = assert_parses(
        "PREFIX f: <https://ns.flur.ee/db#> \
         PREFIX ex: <http://example.org/> \
         SELECT * WHERE { << ex:alice ex:age ?age >> f:t ?t . }",
    );
    let bgp = first_bgp(&ast);
    assert_eq!(bgp.len(), 1);
    // The subject should be a QuotedTriple, NOT confused with a triple term.
    assert!(matches!(&bgp[0].subject, SubjectTerm::QuotedTriple(_)));
}

// ----- PR-W2A: RDF 1.2 reified-triple forms ---------------------------------

#[test]
fn reified_triple_in_object_position_parses() {
    // W3C basic-anonreifier-02: `:s :p << :a :b "c" >> .`
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:s ex:p << ex:a ex:b \"c\" >> . }}"
    ));
    let bgp = first_bgp(&ast);
    assert_eq!(bgp.len(), 1);
    assert!(matches!(&bgp[0].object, crate::ast::Term::QuotedTriple(_)));
}

#[test]
fn reifier_inside_quoted_triple_parses() {
    // W3C basic-reifier-02: `:s :p << :a :b "c" ~ :iri >> .`
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:s ex:p << ex:a ex:b \"c\" ~ ex:iri >> . }}"
    ));
    let bgp = first_bgp(&ast);
    let crate::ast::Term::QuotedTriple(qt) = &bgp[0].object else {
        panic!("expected reified-triple object");
    };
    let reifier = qt.reifier.as_ref().expect("in-triple reifier");
    assert!(matches!(reifier.id, Some(crate::ast::ReifierId::Iri(_))));
}

#[test]
fn standalone_reified_triple_desugars_to_annotation_target() {
    // W3C basic-anonreifier-08: `<< ?s ?p ?o >> .` standalone —
    // desugars to `_:r rdf:reifies <<( ?s ?p ?o )>>`.
    let ast = assert_parses("SELECT * WHERE { << ?s ?p ?o >> . }");
    assert_eq!(first_pattern_kinds(&ast), vec!["AnnotationTarget"]);
}

#[test]
fn nested_reified_triple_parses() {
    // W3C basic-anonreifier-10 / basic-reifier-10.
    assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ << ex:s ex:p << ?s ex:p2 ex:o2 ~ ex:iri2 >> ~ ex:iri1 >> . }}"
    ));
}

#[test]
fn annotation_block_with_path_verb_parses() {
    // W3C annotation-anonreifier-06: `{| :r/:q 'ABC' |}`.
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ?s ?p ?o {{| ex:r/ex:q 'ABC' |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    let block = unit(ann).block.as_ref().expect("block");
    assert!(matches!(
        block.entries[0].verb,
        crate::ast::AnnotationVerb::Path(_)
    ));
}

#[test]
fn standalone_triple_term_still_rejected() {
    // W3C NEGATIVE tripleterm-separate-01: `<<( ?s ?p ?o )>> .` — a
    // bare triple term is a value, not a statement (contrast the
    // standalone REIFIED triple, which is valid). Now that triple-term
    // *values* are accepted (D-1), the subject parses but the missing
    // predicate-object list is rejected with a targeted message.
    assert_parse_error(
        "SELECT * WHERE { <<( ?s ?p ?o )>> . }",
        "a bare triple term is not a statement",
    );
}

#[test]
fn annotation_on_path_verb_still_rejected() {
    // W3C NEGATIVE annotated-anonreifier-path: annotation tails attach
    // to simple-verb triples only, never to path patterns.
    assert_parse_error(
        &format!("{EX_PREFIX}SELECT * WHERE {{ ?s ex:p/ex:q ?o {{| ?pp ?oo |}} . }}"),
        "unexpected token",
    );
}

#[test]
fn version_long_string_rejected() {
    // W3C NEGATIVE version-bad-01/02: VersionSpecifier is a SHORT
    // quoted string only.
    assert_parse_error(
        "VERSION \"\"\"1.2\"\"\" SELECT * WHERE { }",
        "short quoted string",
    );
    assert_parse_error(
        "VERSION \'\'\'1.2\'\'\' SELECT * WHERE { }",
        "short quoted string",
    );
}

#[test]
fn test_pragma_reasoning_last_wins() {
    let ast =
        assert_parses("# PRAGMA reasoning: rdfs\n# PRAGMA reasoning: owl2rl\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, Some(vec!["owl2rl".to_string()]));
}

#[test]
fn test_pragma_reasoning_empty_value_preserved() {
    let ast = assert_parses("# PRAGMA reasoning:\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, Some(Vec::new()));
}

#[test]
fn test_ordinary_comments_are_not_pragmas() {
    let ast =
        assert_parses("# just a comment about reasoning\n# PRAGMATIC note\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, None);
}

#[test]
fn test_unknown_pragma_ignored() {
    let ast = assert_parses("# PRAGMA timeout: 30\nSELECT * WHERE { }");
    assert_eq!(ast.pragmas.reasoning, None);
}

#[test]
fn test_pragma_after_query_body_line() {
    // Pragmas are honored anywhere as full-line comments.
    let ast = assert_parses("SELECT * WHERE { }\n# PRAGMA reasoning: rdfs");
    assert_eq!(ast.pragmas.reasoning, Some(vec!["rdfs".to_string()]));
}

#[test]
fn test_pragma_inside_string_literal_ignored() {
    // A '#' line inside a long string literal is data, not a comment —
    // it must never be interpreted as a pragma.
    let ast = assert_parses("SELECT * WHERE { ?s ?p \"\"\"\n# PRAGMA reasoning: owl2rl\n\"\"\" }");
    assert_eq!(ast.pragmas.reasoning, None);

    // Same for single-line strings containing a '#'.
    let ast = assert_parses("SELECT * WHERE { ?s ?p \"# PRAGMA reasoning: owl2rl\" }");
    assert_eq!(ast.pragmas.reasoning, None);
}

#[test]
fn test_pragma_in_trailing_comment_honored() {
    // A genuine trailing comment is a comment; the lexer identifies it.
    let ast = assert_parses("SELECT * WHERE { } # PRAGMA reasoning: rdfs");
    assert_eq!(ast.pragmas.reasoning, Some(vec!["rdfs".to_string()]));
}

// =============================================================================
// V5 — BIND scope (SPARQL 1.1 §10.1 / grammar note 12)
//
// This check is parse-time by necessity: the single-pattern group
// simplification makes `{ ... { BIND } }` and `{ ... BIND }` produce
// identical ASTs, so `validate()` cannot distinguish them.
// =============================================================================

fn assert_bind_scope_rejected(input: &str) {
    let result = parse(input);
    assert!(
        result.ast.is_none(),
        "expected AST production to be refused: {input}"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.code == DiagCode::BindTargetAlreadyInScope && d.is_error()),
        "expected BindTargetAlreadyInScope: {:?}",
        result.diagnostics
    );
}

fn assert_bind_scope_accepted(input: &str) {
    let result = parse(input);
    assert!(
        !result
            .diagnostics
            .iter()
            .any(|d| d.code == DiagCode::BindTargetAlreadyInScope),
        "unexpected BindTargetAlreadyInScope: {:?}",
        result.diagnostics
    );
    assert!(result.ast.is_some(), "expected AST: {input}");
}

#[test]
fn test_bind_scope_prior_triple_rejected() {
    // W3C syntax-BINDscope6 (test_60): ?o1 bound by a preceding triple in
    // the same group.
    assert_bind_scope_rejected(
        "PREFIX : <http://example.org/> SELECT * WHERE { :s :p ?o . :s :q ?o1 . BIND((1+?o) AS ?o1) }",
    );
}

#[test]
fn test_bind_scope_nested_group_propagates_rejected() {
    // W3C syntax-BINDscope7 (test_61a): in-scope propagates out of a
    // nested group to the BIND in the outer group.
    assert_bind_scope_rejected(
        "PREFIX : <http://example.org/> SELECT * WHERE { { :s :p ?o . :s :q ?o1 . } BIND((1+?o) AS ?o1) }",
    );
}

#[test]
fn test_bind_scope_union_branch_propagates_rejected() {
    // W3C syntax-BINDscope8 (test_62a): a UNION contributes both branches'
    // in-scope variables.
    assert_bind_scope_rejected(
        "PREFIX : <http://example.org/> SELECT * { { { :s :p ?Y } UNION { :s :p ?Z } } BIND(1 AS ?Y) }",
    );
}

#[test]
fn test_bind_scope_use_after_bind_accepted() {
    // W3C syntax-BINDscope1 (positive, test_55): only elements BEFORE the
    // BIND count.
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * WHERE { :s :p ?o . BIND((1+?o) AS ?o1) :s :q ?o1 }",
    );
}

#[test]
fn test_bind_scope_braced_bind_accepted() {
    // W3C syntax-BINDscope2/3 (positive): a `{ BIND ... }` group is its own
    // scope — the outer group's earlier triples don't apply.
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * WHERE { :s :p ?o . :s :q ?o1 { BIND((1+?o) AS ?o1) } }",
    );
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * WHERE { { :s :p ?o . :s :q ?o1 } { BIND((1+?o) AS ?o1) } }",
    );
}

#[test]
fn test_bind_scope_union_branches_independent_accepted() {
    // W3C syntax-BINDscope4/5 (positive): UNION branches are separate
    // groups.
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * { { BIND(1 AS ?Y) } UNION { :s :p ?Y } }",
    );
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * { { :s :p ?Y } UNION { BIND(1 AS ?Y) } }",
    );
}

#[test]
fn test_bind_scope_values_rejected() {
    // VALUES contributes its variables to the in-scope set.
    assert_bind_scope_rejected("SELECT * WHERE { VALUES ?x { 1 2 } BIND(3 AS ?x) }");
}

#[test]
fn test_bind_scope_minus_right_not_in_scope_accepted() {
    // MINUS's right side does not project variables out (§18.2.1).
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * WHERE { ?s :p ?o MINUS { ?s :q ?m } BIND(1 AS ?m) }",
    );
}

#[test]
fn test_bind_scope_filter_var_not_in_scope_accepted() {
    // FILTER contributes nothing to the in-scope set.
    assert_bind_scope_accepted(
        "PREFIX : <http://example.org/> SELECT * WHERE { ?s :p ?o FILTER(?f) BIND(1 AS ?f) }",
    );
}

// =========================================================================
// V1: dot structure inside group graph patterns (W3C syn-bad-02..14)
// =========================================================================

#[test]
fn test_v1_legal_dot_usage_still_parses() {
    // Trailing dot after the last triple is optional but legal.
    assert_parses("SELECT * WHERE { ?s ?p ?o . }");
    // Mandatory dot between two same-subject blocks.
    assert_parses("SELECT * WHERE { ?s ?p ?o . ?s2 ?p2 ?o2 }");
    assert_parses("SELECT * WHERE { ?s ?p ?o . ?s2 ?p2 ?o2 . }");
    // One optional dot may follow a GraphPatternNotTriples.
    assert_parses("SELECT * WHERE { OPTIONAL { ?s ?p ?o } . ?a ?b ?c }");
    assert_parses("SELECT * WHERE { ?s ?p ?o . FILTER(?o) . }");
    assert_parses("SELECT * WHERE { BIND(1 AS ?x) . ?s ?p ?x }");
    assert_parses("SELECT * WHERE { { ?s ?p ?o } . ?a ?b ?c }");
    assert_parses("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } . ?a ?b ?c }");
    // No dot needed between a TriplesBlock and a keyword pattern.
    assert_parses("SELECT * WHERE { ?s ?p ?o OPTIONAL { ?a ?b ?c } }");
}

#[test]
fn test_v1_standalone_dot_rejected() {
    // syn-bad-05 / syn-bad-06
    assert_parse_error("SELECT * WHERE { . }", "unexpected '.'");
    assert_parse_error("SELECT * WHERE { . . }", "unexpected '.'");
}

#[test]
fn test_v1_leading_dot_rejected() {
    // syn-bad-07 / syn-bad-14
    assert_parse_error("SELECT * WHERE { . ?s ?p ?o }", "unexpected '.'");
    assert_parse_error("SELECT * WHERE { . FILTER(?x) }", "unexpected '.'");
}

#[test]
fn test_v1_doubled_dot_rejected() {
    // syn-bad-08..13
    assert_parse_error("SELECT * WHERE { ?s ?p ?o . . }", "unexpected '.'");
    assert_parse_error("SELECT * WHERE { ?s ?p ?o .. }", "unexpected '.'");
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o . . ?s1 ?p1 ?o1 }",
        "unexpected '.'",
    );
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o .. ?s1 ?p1 ?o1 }",
        "unexpected '.'",
    );
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o . ?s1 ?p1 ?o1 .. }",
        "unexpected '.'",
    );
    // A doubled dot after a GraphPatternNotTriples: only ONE optional dot.
    assert_parse_error(
        "SELECT * WHERE { OPTIONAL { ?s ?p ?o } . . }",
        "unexpected '.'",
    );
}

#[test]
fn test_v1_missing_dot_between_triples_rejected() {
    // syn-bad-02 / syn-bad-03
    assert_parse_error(
        "PREFIX : <http://example/ns#> SELECT * { :s1 :p1 :o1 :s2 :p2 :o2 . }",
        "expected '.' between triple patterns",
    );
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o ?s2 ?p2 ?o2 }",
        "expected '.' between triple patterns",
    );
}

// =========================================================================
// V2: FILTER requires a Constraint (W3C filter-missing-parens)
// =========================================================================

#[test]
fn test_v2_filter_constraint_forms_still_parse() {
    // BrackettedExpression
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER (?o) }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER (?o > 5) }");
    // BuiltInCall
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER bound(?o) }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER regex(?o, \"x\") }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER isIRI(?o) }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER EXISTS { ?o ?q ?v } }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER NOT EXISTS { ?o ?q ?v } }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER IF(?o, true, false) }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER COALESCE(?o, false) }");
    // FunctionCall (extension function by IRI / prefixed name)
    assert_parses("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ?p ?o FILTER ex:f(?o) }");
    assert_parses("SELECT * WHERE { ?s ?p ?o FILTER <http://example.org/f>(?o) }");
}

#[test]
fn test_v2_filter_bare_term_rejected() {
    // W3C filter-missing-parens: a bare Var is not a Constraint.
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o FILTER ?x }",
        "FILTER requires a bracketted expression",
    );
    // Bare literals and IRIs are not Constraints either.
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o FILTER true }",
        "FILTER requires a bracketted expression",
    );
    assert_parse_error(
        "PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ?p ?o FILTER ex:c }",
        "FILTER requires a bracketted expression",
    );
}

#[test]
fn test_v2_filter_unparenthesized_operator_expression_rejected() {
    // Relational / boolean operator expressions need the parens too.
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o FILTER ?o > 5 }",
        "FILTER requires a bracketted expression",
    );
    assert_parse_error(
        "SELECT * WHERE { ?s ?p ?o FILTER !bound(?o) }",
        "FILTER requires a bracketted expression",
    );
}

// =========================================================================
// Accepts-valid gaps unmasked by making parse errors authoritative:
// bare ORDER BY constraints, VALUES row shape, sub-select VALUES clause
// =========================================================================

#[test]
fn test_order_by_bare_builtin_and_function_call() {
    // OrderCondition ::= ( 'ASC' | 'DESC' ) BrackettedExpression | ( Constraint | Var )
    // W3C sort#dawg-sort-builtin / dawg-sort-function / syntax-order-06.
    assert_parses("SELECT ?s WHERE { ?s ?p ?o . } ORDER BY str(?o)");
    assert_parses(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
         SELECT ?s WHERE { ?s ?p ?o . } ORDER BY xsd:integer(?o)",
    );
    let ast = assert_parses(
        "PREFIX : <http://example.org/ns#> \
         SELECT * { ?s ?p ?o } ORDER BY DESC(?o+57) :func2(?o) ASC(?s)",
    );
    if let QueryBody::Select(q) = &ast.body {
        let order_by = q.modifiers.order_by.as_ref().expect("ORDER BY clause");
        assert_eq!(order_by.conditions.len(), 3, "all three conditions kept");
    } else {
        panic!("expected SELECT");
    }
}

#[test]
fn test_values_parenthesized_single_var_takes_parenthesized_rows() {
    // InlineDataFull row shape follows the var-LIST shape, not the count
    // (W3C bindings#values7 / inline2).
    let ast = assert_parses(
        "PREFIX : <http://example.org/> \
         SELECT * { ?s ?p ?o } VALUES (?o) { (:b) (UNDEF) }",
    );
    if let QueryBody::Select(q) = &ast.body {
        let values = q.values.as_ref().expect("post-query VALUES");
        if let GraphPattern::Values { vars, data, .. } = values.as_ref() {
            assert_eq!(vars.len(), 1);
            assert_eq!(data.len(), 2);
            assert!(data[0][0].is_some());
            assert!(data[1][0].is_none(), "UNDEF row");
        } else {
            panic!("expected Values pattern");
        }
    }
    // A bare (unparenthesized) var list still takes bare values.
    assert_parses("PREFIX : <http://example.org/> SELECT * {{ ?s ?p ?o }} VALUES ?o { :b :c }");
}

#[test]
fn test_subselect_trailing_values_clause() {
    // SubSelect ::= SelectClause WhereClause SolutionModifier ValuesClause
    // (W3C bindings#inline2).
    let ast = assert_parses(
        "PREFIX : <http://example.org/> \
         SELECT ?s ?o { { SELECT * WHERE { ?s ?p ?o . } VALUES (?o) { (:b) } } }",
    );
    if let QueryBody::Select(q) = &ast.body {
        if let GraphPattern::SubSelect { query, .. } = &q.where_clause.pattern {
            let values = query.values.as_ref().expect("subselect VALUES clause");
            assert!(matches!(values.as_ref(), GraphPattern::Values { .. }));
        } else {
            panic!("expected SubSelect, got {:?}", q.where_clause.pattern);
        }
    }
}

// =========================================================================
// Trailing-token / EOF assertion at the parse entry (issue #1438 / D-10a)
// =========================================================================

#[test]
fn test_trailing_tokens_after_query_rejected() {
    // Trailing input means the parsed AST covers only a prefix of the
    // request, so the parser also suppresses AST production — a caller
    // that never consults diagnostics must not be able to execute the
    // truncated request.
    assert_parse_error_no_ast(
        "SELECT * WHERE { ?s ?p ?o } ?x",
        "unexpected trailing tokens",
    );
    assert_parse_error_no_ast(
        "SELECT * WHERE { ?s ?p ?o } <http://example.org/trailing>",
        "unexpected trailing tokens",
    );
    // A stray ';' after a query form is trailing content too.
    assert_parse_error_no_ast(
        "SELECT * WHERE { ?s ?p ?o } ;",
        "unexpected trailing tokens",
    );
    assert_parse_error_no_ast(
        "ASK { ?s ?p ?o } SELECT * WHERE { ?s ?p ?o }",
        "unexpected trailing tokens",
    );
}

// =========================================================================
// Multi-operation UPDATE requests (issue #1438 / PR-U2)
// =========================================================================

/// Extract the update request from an AST.
fn update_request(ast: &crate::ast::SparqlAst) -> &crate::ast::UpdateRequest {
    match &ast.body {
        QueryBody::Update(req) => req,
        other => panic!("Expected an update request, got {other:?}"),
    }
}

#[test]
fn test_multi_operation_update_parses_all_operations() {
    // Issue #1438: `INSERT ...; DELETE ...` used to parse as the INSERT
    // alone, silently discarding every following operation (silent data
    // loss at commit time). The request-level `;` loop now parses the full
    // sequence, in request order.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/> \
         INSERT DATA { ex:s ex:p 1 } ; DELETE DATA { ex:s ex:p 1 }",
    );
    let req = update_request(&ast);
    assert_eq!(req.operations.len(), 2);
    assert!(matches!(
        req.operations[0].operation,
        UpdateOperation::InsertData(_)
    ));
    assert!(matches!(
        req.operations[1].operation,
        UpdateOperation::DeleteData(_)
    ));

    // The W3C dawg-delete-insert-01c shape: Modify ; Modify.
    let ast = assert_parses(
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \
         INSERT { ?b foaf:knows ?a } WHERE { ?a foaf:knows ?b } ; \
         DELETE { ?a foaf:knows ?b } WHERE { ?a foaf:knows ?b }",
    );
    let req = update_request(&ast);
    assert_eq!(req.operations.len(), 2);
    assert!(matches!(
        req.operations[0].operation,
        UpdateOperation::Modify(_)
    ));
    assert!(matches!(
        req.operations[1].operation,
        UpdateOperation::Modify(_)
    ));
}

#[test]
fn test_multi_operation_prologue_accumulates_across_semicolons() {
    // Update ::= Prologue ( Update1 ( ';' Update )? )? — the recursive
    // Update carries its own Prologue, so PREFIX/BASE may appear after a
    // ';' and are visible to every *subsequent* operation.
    let ast = assert_parses(
        "PREFIX a: <http://example.org/a#> \
         INSERT DATA { a:s a:p 1 } ; \
         PREFIX b: <http://example.org/b#> \
         INSERT DATA { a:s b:p 2 }",
    );
    let req = update_request(&ast);
    assert_eq!(req.operations.len(), 2);
    // Op 1 sees only `a:`.
    assert!(req.operations[0].prologue.get_prefix("a").is_some());
    assert!(req.operations[0].prologue.get_prefix("b").is_none());
    // Op 2 sees the accumulated prologue.
    assert!(req.operations[1].prologue.get_prefix("a").is_some());
    assert!(req.operations[1].prologue.get_prefix("b").is_some());
    // The AST-level prologue is the full accumulation.
    assert!(ast.prologue.get_prefix("b").is_some());
}

#[test]
fn test_multi_operation_prefix_redeclaration_snapshots_per_op() {
    // A prefix redeclared after ';' binds for subsequent operations only;
    // the earlier operation's snapshot keeps the earlier binding.
    let ast = assert_parses(
        "PREFIX ex: <http://example.org/one#> \
         INSERT DATA { ex:s ex:p 1 } ; \
         PREFIX ex: <http://example.org/two#> \
         INSERT DATA { ex:s ex:p 2 }",
    );
    let req = update_request(&ast);
    assert_eq!(req.operations.len(), 2);
    assert_eq!(
        req.operations[0]
            .prologue
            .get_prefix("ex")
            .unwrap()
            .as_ref(),
        "http://example.org/one#"
    );
    assert_eq!(
        req.operations[1]
            .prologue
            .get_prefix("ex")
            .unwrap()
            .as_ref(),
        "http://example.org/two#"
    );
}

#[test]
fn test_empty_and_prologue_only_update_requests_are_valid_noops() {
    // W3C syntax-update-1 test_38/39/40: the operation list is optional,
    // so an empty, BASE-only, or PREFIX-only request parses as a valid
    // zero-operation request.
    for input in [
        "",
        "# Empty",
        "BASE <http://example/>",
        "PREFIX : <http://example/>",
        "PREFIX : <http://example/> # Otherwise empty",
    ] {
        let ast = assert_parses(input);
        let req = update_request(&ast);
        assert!(
            req.operations.is_empty(),
            "expected a zero-operation request for {input:?}"
        );
    }
}

#[test]
fn test_single_trailing_semicolon_after_update_is_legal() {
    // Update ::= Prologue ( Update1 ( ';' Update )? )? — the recursive
    // Update may be empty, so one trailing ';' is valid SPARQL.
    let ast = assert_parses("PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p 1 } ;");
    assert_eq!(update_request(&ast).operations.len(), 1);
    // ...but a second ';' is not an operation.
    assert_parse_error_no_ast(
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p 1 } ; ;",
        "expected query form",
    );
}

#[test]
fn test_cross_operation_bnode_label_reuse_rejected() {
    // W3C syntax-update-54: a blank node label in GROUND DATA is scoped to one
    // operation; reusing it in a later INSERT DATA / DELETE DATA of the same
    // request is a syntax error.
    assert_parse_error(
        "PREFIX : <http://www.example.org/> \
         INSERT DATA { _:b1 :p :o } ; INSERT DATA { _:b1 :p :o }",
        "blank node label",
    );
    // Template bnode reuse across MODIFY (INSERT/DELETE ... WHERE) operations is
    // NOT a violation — template blank nodes are per-solution/per-operation
    // (CONSTRUCT-style), so the two `_:b1` denote DISTINCT nodes. W3C's approved
    // positive tests `insert-where-same-bnode`/`-2` require this to parse and
    // execute successfully.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         INSERT { _:b1 :p ?o } WHERE { ?s :q ?o } ; \
         INSERT { _:b1 :p ?o } WHERE { ?s :q ?o }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
    // Reuse *within* one operation stays legal.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         INSERT DATA { _:b1 :p :o . _:b1 :q :o } ; INSERT DATA { _:b2 :p :o }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
    // Distinct labels across operations stay legal.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         INSERT DATA { _:b1 :p :o } ; INSERT DATA { _:b2 :p :o }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
}

#[test]
fn test_cross_operation_delete_side_bnode_labels_are_independent() {
    // PR-1454 review (bplatz): §19.6's cross-operation label ban protects
    // blank-node *minting* positions only. DELETE WHERE blank nodes are
    // locally-scoped existential matching variables under Fluree's
    // documented extension — reusing `_:b1` across two DELETE WHERE
    // operations is two independent matches and must parse.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         DELETE WHERE { _:b1 :flag \"a\" } ; DELETE WHERE { _:b1 :flag \"b\" }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
    // DELETE-template labels likewise stay out of the reuse check: the
    // strict validator rejects DELETE-side blank nodes outright with the
    // real error, which a cross-operation-label complaint would mask.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         DELETE { _:b1 :p ?o } WHERE { ?s :q ?o } ; \
         DELETE { _:b1 :p ?o } WHERE { ?s :q ?o }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
    // A minted label followed by a DELETE WHERE reuse is fine too — only
    // minting positions participate in the §19.6 scope rule.
    let ast = assert_parses(
        "PREFIX : <http://www.example.org/> \
         INSERT DATA { _:b1 :p :o } ; DELETE WHERE { _:b1 :flag \"a\" }",
    );
    assert_eq!(update_request(&ast).operations.len(), 2);
}
