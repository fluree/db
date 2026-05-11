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
            assert_eq!(query.limit, Some(10));
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
            assert_eq!(query.order_by.len(), 1);
            assert_eq!(query.order_by[0].var.name.as_ref(), "x");
            assert!(!query.order_by[0].descending);
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
            assert_eq!(query.order_by.len(), 1);
            assert!(query.order_by[0].descending);
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

#[test]
fn test_insert_data_simple() {
    let ast =
        assert_parses("INSERT DATA { <http://example.org/s> <http://example.org/p> \"value\" }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::InsertData(insert)) => {
            assert_eq!(insert.data.triples.len(), 1);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_insert_data_multiple_triples() {
    let ast = assert_parses(
        "INSERT DATA { <http://example.org/s1> <http://example.org/p> \"v1\" . <http://example.org/s2> <http://example.org/p> \"v2\" }"
    );
    match &ast.body {
        QueryBody::Update(UpdateOperation::InsertData(insert)) => {
            assert_eq!(insert.data.triples.len(), 2);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_insert_data_prefixed() {
    let ast = assert_parses("PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p \"value\" }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::InsertData(insert)) => {
            assert_eq!(insert.data.triples.len(), 1);
        }
        _ => panic!("Expected INSERT DATA"),
    }
}

#[test]
fn test_delete_data_simple() {
    let ast =
        assert_parses("DELETE DATA { <http://example.org/s> <http://example.org/p> \"value\" }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::DeleteData(delete)) => {
            assert_eq!(delete.data.triples.len(), 1);
        }
        _ => panic!("Expected DELETE DATA"),
    }
}

#[test]
fn test_delete_where_simple() {
    let ast = assert_parses("DELETE WHERE { ?s ex:obsolete ?o }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::DeleteWhere(delete)) => {
            assert_eq!(delete.pattern.patterns.len(), 1);
        }
        _ => panic!("Expected DELETE WHERE"),
    }
}

#[test]
fn test_delete_where_multiple_patterns() {
    let ast = assert_parses("DELETE WHERE { ?s ex:old ?o . ?s ex:deprecated ?x }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::DeleteWhere(delete)) => {
            assert_eq!(delete.pattern.patterns.len(), 2);
        }
        _ => panic!("Expected DELETE WHERE"),
    }
}

#[test]
fn test_modify_delete_insert() {
    let ast =
        assert_parses("DELETE { ?s ex:old ?o } INSERT { ?s ex:new ?o } WHERE { ?s ex:old ?o }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
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
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
            assert!(modify.delete_clause.is_some());
            assert!(modify.insert_clause.is_none());
        }
        _ => panic!("Expected Modify operation"),
    }
}

#[test]
fn test_modify_insert_only() {
    let ast = assert_parses("INSERT { ?s ex:new ?o } WHERE { ?s ex:old ?o }");
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
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
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
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
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
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
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
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
    match &ast.body {
        QueryBody::Update(UpdateOperation::Modify(modify)) => {
            assert!(modify.with_iri.is_some());
            assert!(modify.delete_clause.is_some());
            assert!(modify.insert_clause.is_some());
            assert!(modify.using.is_some());
        }
        _ => panic!("Expected Modify operation"),
    }
}

// ========================================================================
// RDF Collection (List) Syntax — Error Recovery Tests
// ========================================================================

#[test]
fn test_rdf_collection_in_subject_position() {
    // RDF collection syntax in subject position should produce an error, not hang.
    let result = parse("SELECT * WHERE { (1 2 3) ?p ?o }");
    assert!(
        result.has_errors(),
        "RDF collection in subject position should produce an error"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("collection")),
        "Error should mention 'collection': {:?}",
        result
            .diagnostics
            .iter()
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_rdf_collection_in_object_position() {
    // RDF collection syntax in object position should produce an error, not hang.
    let result = parse("SELECT * WHERE { ?s ?p (1 2 3) }");
    assert!(
        result.has_errors(),
        "RDF collection in object position should produce an error"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("collection")),
        "Error should mention 'collection': {:?}",
        result
            .diagnostics
            .iter()
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_rdf_nil_in_subject_position() {
    // Empty list () in subject position should produce an error, not hang.
    let result = parse("SELECT * WHERE { () ?p ?o }");
    assert!(
        result.has_errors(),
        "Nil in subject position should produce an error"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("collection")),
        "Error should mention 'collection': {:?}",
        result
            .diagnostics
            .iter()
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_rdf_nil_in_object_position() {
    // Empty list () in object position should produce an error, not hang.
    let result = parse("SELECT * WHERE { ?s ?p () }");
    assert!(
        result.has_errors(),
        "Nil in object position should produce an error"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("collection")),
        "Error should mention 'collection': {:?}",
        result
            .diagnostics
            .iter()
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_nested_rdf_collection_no_hang() {
    // Nested collections should be skipped without hanging.
    let result = parse("SELECT * WHERE { ((1 2) (3 4)) ?p ?o }");
    assert!(
        result.has_errors(),
        "Nested collections should produce errors"
    );
}

#[test]
fn test_rdf_collection_parser_recovers() {
    // After skipping a collection, the parser should recover and parse
    // subsequent triple patterns.
    let result = parse("SELECT * WHERE { ?s ?p (1 2) . ?x ?y ?z }");
    assert!(result.has_errors(), "Collection should produce an error");
    // The AST should still be produced (error recovery, not fatal).
    assert!(
        result.ast.is_some(),
        "Parser should recover and produce an AST despite collection error"
    );
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
    assert!(ann.reifier.is_none(), "anonymous form has no reifier id");
    let block = ann.block.as_ref().expect("block should be present");
    assert_eq!(block.entries.len(), 1);
}

#[test]
fn annotation_block_with_named_blank_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ _:ann {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    match ann.reifier.as_ref().expect("reifier id") {
        crate::ast::ReifierId::BlankNode(b) => {
            assert!(matches!(b.value, BlankNodeValue::Labeled(ref l) if l.as_ref() == "ann"));
        }
        other => panic!("expected blank-node reifier, got {other:?}"),
    }
    assert!(ann.block.is_some());
}

#[test]
fn annotation_block_with_named_iri_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ex:rel {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    matches!(ann.reifier.as_ref(), Some(crate::ast::ReifierId::Iri(_)));
}

#[test]
fn annotation_block_with_var_reifier() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?ann {{| ex:role \"Engineer\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    matches!(ann.reifier.as_ref(), Some(crate::ast::ReifierId::Var(_)));
}

#[test]
fn bare_tilde_reifier_no_block_parses() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?ann . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    assert!(ann.reifier.is_some());
    assert!(ann.block.is_none(), "bare reifier carries no block");
}

#[test]
fn empty_annotation_block_parses() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let ann = bgp[0].annotation.as_ref().expect("annotation tail");
    let block = ann.block.as_ref().expect("block");
    assert_eq!(block.entries.len(), 0);
}

#[test]
fn annotation_block_with_multiple_predicate_object_pairs() {
    let ast = assert_parses(&format!(
        "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role \"Engineer\" ; ex:since \"2024\" |}} . }}"
    ));
    let bgp = first_bgp(&ast);
    let block = bgp[0].annotation.as_ref().unwrap().block.as_ref().unwrap();
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

fn assert_parse_error(input: &str, needle: &str) {
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
}

#[test]
fn annotation_on_literal_object_is_rejected() {
    assert_parse_error(
        &format!("{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:age 30 {{| ex:source \"x\" |}} . }}"),
        "literal-valued",
    );
}

#[test]
fn triple_term_outside_rdf_reifies_is_rejected() {
    assert_parse_error(
        &format!("{EX_PREFIX}SELECT * WHERE {{ ?ann ex:foo <<( ex:a ex:b ex:c )>> . }}"),
        "object of rdf:reifies",
    );
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
fn duplicate_reifier_in_tail_is_rejected() {
    assert_parse_error(
        &format!(
            "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme ~ ?a ~ ?b {{| ex:role \"x\" |}} . }}"
        ),
        "at most one reifier",
    );
}

#[test]
fn duplicate_block_in_tail_is_rejected() {
    assert_parse_error(
        &format!(
            "{EX_PREFIX}SELECT * WHERE {{ ex:alice ex:worksFor ex:acme {{| ex:role \"x\" |}} {{| ex:since \"y\" |}} . }}"
        ),
        "at most one annotation block",
    );
}

// ----- Existing legacy `<< s p ?o >> f:t ?t` form regression check ---------

#[test]
fn rdf_reifies_in_insert_data_is_rejected_with_clear_error() {
    // The rdf:reifies + triple-term form is WHERE-only in v1.
    // SPARQL UPDATE uses the `~ {| |}` form instead.
    assert_parse_error(
        &format!(
            "{RDF_PREFIX}{EX_PREFIX}INSERT DATA {{ _:ann rdf:reifies <<( ex:a ex:b ex:c )>> }}"
        ),
        "object of rdf:reifies",
    );
}

#[test]
fn rdf_reifies_in_insert_template_is_rejected_with_clear_error() {
    assert_parse_error(
        &format!(
            "{RDF_PREFIX}{EX_PREFIX}\
             INSERT {{ _:ann rdf:reifies <<( ex:a ex:b ex:c )>> }} WHERE {{ ?s ?p ?o }}"
        ),
        "object of rdf:reifies",
    );
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
