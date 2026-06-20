#![allow(clippy::needless_raw_string_hashes)]

//! Cypher write-path lowering tests.
//!
//! These exercise the Cypher write surface. The generated `Txn` shape is
//! compared to expectations; the
//! shared staging pipeline is exercised by the API-level integration
//! tests once they land.

use fluree_db_cypher::parse_cypher;
use fluree_db_transact::ir::TxnOpts;
use fluree_db_transact::ir::{TemplateTerm, TxnType};
use fluree_db_transact::lower_cypher_update::{lower_cypher_update, CypherLowerOpts};
use fluree_db_transact::namespace::NamespaceRegistry;

fn lower(src: &str) -> fluree_db_transact::ir::Txn {
    let out = parse_cypher(src);
    assert!(!out.has_errors(), "parse errors: {:?}", out.diagnostics);
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    )
    .expect("lower")
}

#[test]
fn create_single_labeled_node_emits_rdf_type_triple() {
    let txn = lower("CREATE (n:Person)");
    assert_eq!(txn.txn_type, TxnType::Insert);
    assert_eq!(txn.delete_templates.len(), 0);
    assert_eq!(txn.insert_templates.len(), 1);
    let t = &txn.insert_templates[0];
    // Subject is a blank-node template (since `n` has no @id binding).
    assert!(matches!(t.subject, TemplateTerm::BlankNode(_)));
}

#[test]
fn create_node_with_properties() {
    let txn = lower(r#"CREATE (n:Person {name: "Alice", age: 30})"#);
    // 1 label + 2 properties = 3 templates.
    assert_eq!(txn.insert_templates.len(), 3);
}

#[test]
fn create_directed_relationship_emits_base_and_reifier_bundle() {
    let txn = lower(r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#);
    // Every Cypher relationship reifies (LPG identity):
    // 2 labels + 2 props + 1 base edge + 3 reifier bundle triples = 8 templates.
    assert_eq!(
        txn.insert_templates.len(),
        8,
        "templates: {:?}",
        txn.insert_templates
    );
}

#[test]
fn create_null_property_is_skipped() {
    // Cypher: a null property value means "no property" — not a stored null.
    let txn = lower(r#"CREATE (n:Person {name: "Alice", nick: null})"#);
    // 1 label + 1 name property = 2 (nick: null skipped).
    assert_eq!(
        txn.insert_templates.len(),
        2,
        "nick:null should be skipped: {:?}",
        txn.insert_templates
    );
}

#[test]
fn optional_match_before_create_is_rejected() {
    let out = parse_cypher(
        r#"MATCH (a:Person {name: "Alice"})
           OPTIONAL MATCH (b:Person {name: "Ghost"})
           CREATE (a)-[:KNOWS]->(b)"#,
    );
    assert!(!out.has_errors(), "parse should succeed");
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(
        r.is_err(),
        "OPTIONAL MATCH before CREATE should be rejected"
    );
}

#[test]
fn create_relationship_with_properties_adds_body_triples() {
    let txn = lower("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)");
    // 2 labels + 1 base + 3 bundle + 1 ann body = 7
    assert_eq!(
        txn.insert_templates.len(),
        7,
        "templates: {:?}",
        txn.insert_templates
    );
}

#[test]
fn create_two_parallel_relationships_mints_distinct_annotation_subjects() {
    // Every Cypher relationship reifies, so two parallel edges get distinct
    // annotation subjects (lowering mints distinct template blank nodes; the
    // generator freshens them per solution).
    let txn = lower(
        r#"CREATE
              (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
              (a)-[:KNOWS]->(b)"#,
    );
    // Verify two distinct annotation subjects appear in the reifies
    // bundle.
    let subjects: std::collections::HashSet<String> = txn
        .insert_templates
        .iter()
        .filter_map(|t| match (&t.predicate, &t.subject) {
            (TemplateTerm::Sid(_), TemplateTerm::BlankNode(label))
                if label.starts_with("_:cy_ann_") =>
            {
                Some(label.clone())
            }
            _ => None,
        })
        .collect();
    assert!(
        subjects.len() >= 2,
        "expected at least 2 annotation subjects, got {subjects:?}"
    );
}

#[test]
fn create_undirected_relationship_is_rejected() {
    let out = parse_cypher("CREATE (a:Person)-[:KNOWS]-(b:Person)");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(r.is_err());
}

#[test]
fn create_bare_node_is_rejected() {
    let out = parse_cypher("CREATE ()");
    // Empty parens may parse, but parsing a node with no var/label/props
    // depends on the parser shape. Try both — if parse fails, that's
    // an acceptable rejection point too.
    if out.has_errors() {
        return;
    }
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(r.is_err(), "expected rejection of bare CREATE ()");
}

#[test]
fn read_query_via_update_entry_is_rejected() {
    let out = parse_cypher("MATCH (n:Person) RETURN n");
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(r.is_err());
}

#[test]
fn detach_delete_emits_inbound_and_outbound_scans() {
    let txn = lower("MATCH (n:Person {name: \"Alice\"}) DETACH DELETE n");
    assert_eq!(txn.txn_type, TxnType::Update);
    // WHERE: label + inline-name filter + OPTIONAL outbound + OPTIONAL inbound.
    assert_eq!(
        txn.where_patterns.len(),
        4,
        "where: {:?}",
        txn.where_patterns
    );
    // Delete both directions: (n ?p ?o) and (?s ?p2 n).
    assert_eq!(txn.delete_templates.len(), 2);
    assert_eq!(txn.insert_templates.len(), 0);
    // LPG lifecycle is enabled so relationship body metadata is cascaded.
    assert_eq!(txn.opts.lpg_edge_lifecycle, Some(true));
}

#[test]
fn deferred_write_shapes_are_rejected() {
    for src in [
        // Bare DELETE n needs a relationship-existence probe.
        "MATCH (n:Person) DELETE n",
        // MERGE ON MATCH SET needs a complementary EXISTS branch.
        "MERGE (n:Person {name: \"A\"}) ON MATCH SET n.x = 1",
        // A property-bearing MERGE relationship needs an annotation-sidecar guard.
        "MERGE (a:Person {name: \"A\"})-[:KNOWS {since: 2020}]->(b:Person {name: \"B\"})",
        // Undirected MERGE relationship is rejected.
        "MERGE (a:Person {name: \"A\"})-[:KNOWS]-(b:Person {name: \"B\"})",
        // Multi-hop MERGE pattern is deferred.
        "MERGE (a:Person {name: \"A\"})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)",
        // A leading MATCH is allowed only before a *relationship* MERGE — a
        // node MERGE must stand alone.
        "MATCH (a:Person) MERGE (n:Person {name: \"A\"})",
        // OPTIONAL MATCH before a relationship MERGE risks a partial reifier
        // bundle (optionally-unbound endpoint), so it is rejected.
        "MATCH (a:Person {name: \"A\"}) OPTIONAL MATCH (b:Person {name: \"B\"}) \
         MERGE (a)-[:KNOWS]->(b)",
    ] {
        let out = parse_cypher(src);
        if out.has_errors() {
            continue;
        }
        let ast = out.ast.unwrap();
        let mut ns = NamespaceRegistry::new();
        let r = lower_cypher_update(
            &ast,
            &mut ns,
            TxnOpts::default(),
            CypherLowerOpts::default(),
        );
        assert!(
            r.is_err(),
            "expected deferred-feature error for `{src}`, got {:?}",
            r.ok()
        );
    }
}

#[test]
fn match_set_property_emits_update_with_optional_old_value() {
    let txn = lower(r#"MATCH (n:Person {name: "Alice"}) SET n.age = 42"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    // WHERE: label triple + inline-name triple + OPTIONAL old-age.
    assert_eq!(
        txn.where_patterns.len(),
        3,
        "where: {:?}",
        txn.where_patterns
    );
    // Retract old age (var object), insert new age.
    assert_eq!(txn.delete_templates.len(), 1);
    assert_eq!(txn.insert_templates.len(), 1);
    assert!(matches!(
        txn.delete_templates[0].object,
        TemplateTerm::Var(_)
    ));
    assert!(matches!(
        txn.insert_templates[0].object,
        TemplateTerm::Value(_)
    ));
}

#[test]
fn match_set_property_to_null_removes_it() {
    // `SET n.prop = null` is Cypher's property removal: retract, no insert.
    let txn = lower(r#"MATCH (n:Person {name: "Alice"}) SET n.age = null"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    assert_eq!(txn.delete_templates.len(), 1, "retract the old value");
    assert_eq!(txn.insert_templates.len(), 0, "null asserts nothing");
    assert!(matches!(
        txn.delete_templates[0].object,
        TemplateTerm::Var(_)
    ));
}

#[test]
fn match_set_label_is_additive_insert_only() {
    let txn = lower("MATCH (n:Person) SET n:Employee");
    assert_eq!(txn.txn_type, TxnType::Update);
    assert_eq!(txn.delete_templates.len(), 0);
    assert_eq!(txn.insert_templates.len(), 1);
}

#[test]
fn match_set_map_merge_emits_per_key_replace() {
    let txn = lower(r#"MATCH (n:Person {name: "Alice"}) SET n += {age: 42, city: "NYC"}"#);
    // Two keys → two OPTIONAL old-value retracts + two inserts.
    assert_eq!(txn.delete_templates.len(), 2);
    assert_eq!(txn.insert_templates.len(), 2);
}

#[test]
fn match_set_map_replace_emits_bounded_property_scan() {
    let txn = lower(r#"MATCH (n:Person {name: "Alice"}) SET n = {name: "Alicia", city: "Paris"}"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    // WHERE: label triple + inline-name triple + OPTIONAL old-property scan.
    assert_eq!(
        txn.where_patterns.len(),
        3,
        "where: {:?}",
        txn.where_patterns
    );
    assert_eq!(txn.delete_templates.len(), 1);
    assert_eq!(txn.insert_templates.len(), 2);
    assert!(matches!(
        txn.delete_templates[0].predicate,
        TemplateTerm::Var(_)
    ));
    assert!(matches!(
        txn.delete_templates[0].object,
        TemplateTerm::Var(_)
    ));
}

#[test]
fn match_remove_property_emits_delete_only() {
    let txn = lower(r#"MATCH (n:Person {name: "Alice"}) REMOVE n.age"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    assert_eq!(txn.delete_templates.len(), 1);
    assert_eq!(txn.insert_templates.len(), 0);
    assert!(matches!(
        txn.delete_templates[0].object,
        TemplateTerm::Var(_)
    ));
}

/// Lower a write statement, expecting a rejection (returns the error message).
fn lower_err(src: &str) -> String {
    let out = parse_cypher(src);
    assert!(!out.has_errors(), "parse errors: {:?}", out.diagnostics);
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    )
    .expect_err("expected lowering to reject")
    .to_string()
}

#[test]
fn with_passes_through_variables_into_a_write() {
    // `WITH a, b` carries both forward → the SET target stays in scope.
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person)
           WITH a, b
           SET b.seen = true"#,
    );
    assert_eq!(txn.txn_type, TxnType::Update);
    assert_eq!(txn.insert_templates.len(), 1, "one SET insert");
}

#[test]
fn with_computed_alias_binds_and_carries_into_set() {
    use fluree_db_query::parse::UnresolvedPattern;
    // A computed projection becomes a Bind the SET can reference.
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"})
           WITH a, a.age + 1 AS next
           SET a.nextAge = next"#,
    );
    let has_next_bind = txn
        .where_patterns
        .iter()
        .any(|p| matches!(p, UnresolvedPattern::Bind { var, .. } if var.as_ref() == "?next"));
    assert!(has_next_bind, "where: {:?}", txn.where_patterns);
    // SET nextAge: retract old + assert new = 1 insert, 1 delete.
    assert_eq!(txn.insert_templates.len(), 1);
    assert_eq!(txn.delete_templates.len(), 1);
}

#[test]
fn with_renames_a_variable_for_a_write() {
    // `WITH a AS p` — the alias is the only in-scope name afterward.
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"})
           WITH a AS p
           SET p.flag = true"#,
    );
    assert_eq!(txn.insert_templates.len(), 1);
}

#[test]
fn with_narrows_scope_so_dropped_target_is_rejected() {
    // `b` is dropped by `WITH a`, so a later SET on `b` is unbound.
    let msg = lower_err(
        r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person)
           WITH a
           SET b.x = 1"#,
    );
    assert!(msg.contains("not bound"), "msg: {msg}");
}

#[test]
fn with_aggregate_projection_is_rejected() {
    // Aggregation in a write-side WITH is deferred (no grouping in single-Txn).
    let msg = lower_err(
        r#"MATCH (a:Person)-[:KNOWS]->(b:Person)
           WITH a, count(b) AS friends
           SET a.friends = friends"#,
    );
    assert!(
        msg.contains("count") || msg.contains("aggregat"),
        "msg: {msg}"
    );
}

#[test]
fn with_distinct_or_slice_before_write_is_rejected() {
    for src in [
        r#"MATCH (a:Person) WITH DISTINCT a SET a.x = 1"#,
        r#"MATCH (a:Person) WITH a LIMIT 5 SET a.x = 1"#,
        r#"MATCH (a:Person) WITH a ORDER BY a.name SET a.x = 1"#,
    ] {
        let msg = lower_err(src);
        assert!(
            msg.contains("deferred"),
            "expected a deferred error for `{src}`, got: {msg}"
        );
    }
}

#[test]
fn with_before_delete_is_rejected() {
    // DELETE resolution keys off the raw MATCH variables, so a WITH rename or
    // dropped target can't be honored — reject the combination outright (rather
    // than mis-routing or deleting an out-of-scope variable).
    for src in [
        r#"MATCH (a:Person) WITH a AS p DELETE p"#,
        r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WITH r AS edge DELETE edge"#,
        r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WITH a DELETE r"#,
    ] {
        let msg = lower_err(src);
        assert!(
            msg.contains("WITH before DELETE"),
            "expected a WITH-before-DELETE rejection for `{src}`, got: {msg}"
        );
    }
}

#[test]
fn merge_single_node_emits_not_exists_guard_and_create_inserts() {
    use fluree_db_query::parse::UnresolvedPattern;
    let txn = lower(r#"MERGE (n:Person {name: "Alice"})"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    // One NOT EXISTS guard over the identifying pattern.
    assert_eq!(
        txn.where_patterns.len(),
        1,
        "where: {:?}",
        txn.where_patterns
    );
    assert!(matches!(
        txn.where_patterns[0],
        UnresolvedPattern::NotExists(_)
    ));
    // Create branch: label + name = 2 inserts, no deletes.
    assert_eq!(txn.insert_templates.len(), 2);
    assert_eq!(txn.delete_templates.len(), 0);
}

#[test]
fn merge_on_create_set_adds_inserts() {
    let txn = lower(r#"MERGE (n:Person {name: "Alice"}) ON CREATE SET n.created = "yes""#);
    // label + name + created = 3 inserts.
    assert_eq!(
        txn.insert_templates.len(),
        3,
        "inserts: {:?}",
        txn.insert_templates
    );
    assert_eq!(txn.delete_templates.len(), 0);
}

#[test]
fn merge_must_be_only_write_clause() {
    // Multiple MERGEs and CREATE … MERGE are rejected (guards would be
    // conjunctive / blind to earlier writes).
    for src in [
        r#"MERGE (a:Person {name: "A"}) MERGE (b:Person {name: "B"})"#,
        r#"CREATE (a:Person {name: "A"}) MERGE (b:Person {name: "B"})"#,
    ] {
        let out = parse_cypher(src);
        if out.has_errors() {
            continue;
        }
        let ast = out.ast.unwrap();
        let mut ns = NamespaceRegistry::new();
        let r = lower_cypher_update(
            &ast,
            &mut ns,
            TxnOpts::default(),
            CypherLowerOpts::default(),
        );
        assert!(r.is_err(), "expected rejection for `{src}`");
    }
}

#[test]
fn merge_on_create_set_on_identity_key_is_rejected() {
    let out = parse_cypher(r#"MERGE (n:Person {name: "Alice"}) ON CREATE SET n.name = "Alicia""#);
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(
        r.is_err(),
        "ON CREATE SET on an identity-map key should be rejected"
    );
}

#[test]
fn merge_on_create_map_merge_skips_null_entries() {
    // Consistent with `n.x = null`: a null map entry asserts nothing.
    let txn = lower(r#"MERGE (n:Person {name: "Alice"}) ON CREATE SET n += {x: null, y: 1}"#);
    // label + name (identity) + y = 3 (x skipped).
    assert_eq!(
        txn.insert_templates.len(),
        3,
        "inserts: {:?}",
        txn.insert_templates
    );
}

#[test]
fn merge_relationship_emits_path_guard_and_create_branch() {
    use fluree_db_query::parse::UnresolvedPattern;
    let txn = lower(r#"MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#);
    assert_eq!(txn.txn_type, TxnType::Update);
    // One NOT EXISTS guard spanning the whole path.
    assert_eq!(
        txn.where_patterns.len(),
        1,
        "where: {:?}",
        txn.where_patterns
    );
    assert!(matches!(
        txn.where_patterns[0],
        UnresolvedPattern::NotExists(_)
    ));
    // Guard contents: 2 labels + 2 name filters + 1 rel triple = 5.
    if let UnresolvedPattern::NotExists(guard) = &txn.where_patterns[0] {
        assert_eq!(guard.len(), 5, "guard: {guard:?}");
    }
    // Create branch: 2 labels + 2 names + base edge + 3 reifier triples = 8.
    assert_eq!(
        txn.insert_templates.len(),
        8,
        "inserts: {:?}",
        txn.insert_templates
    );
    assert_eq!(txn.delete_templates.len(), 0);
    // Endpoints are fresh blank nodes (neither var was MATCH-bound).
    let base = txn
        .insert_templates
        .iter()
        .find(|t| {
            matches!(t.subject, TemplateTerm::BlankNode(_))
                && matches!(t.object, TemplateTerm::BlankNode(_))
        })
        .expect("a base edge between two blank-node endpoints");
    assert!(matches!(base.subject, TemplateTerm::BlankNode(_)));
    assert!(matches!(base.object, TemplateTerm::BlankNode(_)));
}

#[test]
fn merge_relationship_on_create_set_routes_to_endpoint() {
    // ON CREATE SET on the tail endpoint adds one insert on that node.
    let txn = lower(
        r#"MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})
           ON CREATE SET b.note = "new""#,
    );
    // 8 path inserts + 1 ON CREATE SET = 9.
    assert_eq!(
        txn.insert_templates.len(),
        9,
        "inserts: {:?}",
        txn.insert_templates
    );
}

#[test]
fn merge_relationship_incoming_direction_orients_edge() {
    // `<-[:KNOWS]-` puts the tail node on the subject side of the base edge.
    let txn = lower(r#"MERGE (a:Person {name: "Alice"})<-[:KNOWS]-(b:Person {name: "Bob"})"#);
    assert_eq!(txn.insert_templates.len(), 8);
}

#[test]
fn merge_relationship_with_bound_endpoints_uses_match_vars() {
    use fluree_db_query::parse::UnresolvedPattern;
    // Scope B: both endpoints bound by a leading MATCH → per-row find-or-create.
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
           MERGE (a)-[:KNOWS]->(b)"#,
    );
    assert_eq!(txn.txn_type, TxnType::Update);
    // WHERE: 2 MATCH labels + 2 name filters + 1 NOT EXISTS guard = 5.
    assert_eq!(
        txn.where_patterns.len(),
        5,
        "where: {:?}",
        txn.where_patterns
    );
    let guard = txn
        .where_patterns
        .iter()
        .find_map(|p| match p {
            UnresolvedPattern::NotExists(g) => Some(g),
            _ => None,
        })
        .expect("a NOT EXISTS guard");
    // Bound endpoints add no label/prop guard — just the rel triple.
    assert_eq!(guard.len(), 1, "guard: {guard:?}");
    // Create branch: only the base edge + 3 reifier triples (endpoints exist).
    assert_eq!(
        txn.insert_templates.len(),
        4,
        "inserts: {:?}",
        txn.insert_templates
    );
    // The base edge references the MATCH-bound variables, not blank nodes.
    let base = &txn.insert_templates[0];
    assert!(matches!(base.subject, TemplateTerm::Var(_)));
    assert!(matches!(base.object, TemplateTerm::Var(_)));
}

#[test]
fn merge_relationship_mixed_bound_and_new_endpoint() {
    use fluree_db_query::parse::UnresolvedPattern;
    // Bound head + a new tail node introduced by the MERGE: per matched `a`,
    // find-or-create a Pet named Rex.
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"})
           MERGE (a)-[:HAS_PET]->(p:Pet {name: "Rex"})"#,
    );
    // WHERE: 1 MATCH label + 1 name filter + 1 NOT EXISTS guard = 3.
    assert_eq!(
        txn.where_patterns.len(),
        3,
        "where: {:?}",
        txn.where_patterns
    );
    let guard = txn
        .where_patterns
        .iter()
        .find_map(|p| match p {
            UnresolvedPattern::NotExists(g) => Some(g),
            _ => None,
        })
        .expect("a NOT EXISTS guard");
    // Guard: the new tail's label + name (probe) + the rel triple = 3.
    assert_eq!(guard.len(), 3, "guard: {guard:?}");
    // Create: new Pet's label + name + base edge + 3 reifier triples = 6.
    assert_eq!(
        txn.insert_templates.len(),
        6,
        "inserts: {:?}",
        txn.insert_templates
    );
    // The new endpoint is a blank node; the bound head stays a Var.
    let base = txn
        .insert_templates
        .iter()
        .find(|t| {
            matches!(t.subject, TemplateTerm::Var(_))
                && matches!(t.object, TemplateTerm::BlankNode(_))
        })
        .expect("base edge: bound head Var → new tail blank node");
    assert!(matches!(base.subject, TemplateTerm::Var(_)));
    assert!(matches!(base.object, TemplateTerm::BlankNode(_)));
}

#[test]
fn match_create_relationship_references_bound_nodes() {
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
           CREATE (a)-[:KNOWS]->(b)"#,
    );
    assert_eq!(txn.txn_type, TxnType::Update);
    // WHERE: 2 labels + 2 inline-name filters.
    assert_eq!(
        txn.where_patterns.len(),
        4,
        "where: {:?}",
        txn.where_patterns
    );
    // CREATE: base edge + 3 reifier bundle triples = 4 (no new labels).
    assert_eq!(
        txn.insert_templates.len(),
        4,
        "inserts: {:?}",
        txn.insert_templates
    );
    // Base edge endpoints reference the MATCH-bound variables.
    let base = &txn.insert_templates[0];
    assert!(matches!(base.subject, TemplateTerm::Var(_)));
    assert!(matches!(base.object, TemplateTerm::Var(_)));
}

#[test]
fn match_create_new_node_uses_blank_node() {
    let txn = lower(
        r#"MATCH (a:Person {name: "Alice"})
           CREATE (a)-[:HAS_PET]->(p:Pet {name: "Rex"})"#,
    );
    assert_eq!(txn.txn_type, TxnType::Update);
    // The bound `a` appears as a Var subject (the base edge); the new
    // Pet node `p` appears as a blank-node subject (its label/prop
    // triples).
    let has_var_subject = txn
        .insert_templates
        .iter()
        .any(|t| matches!(t.subject, TemplateTerm::Var(_)));
    let has_bnode_subject = txn
        .insert_templates
        .iter()
        .any(|t| matches!(t.subject, TemplateTerm::BlankNode(_)));
    assert!(has_var_subject, "bound `a` should drive a Var-subject edge");
    assert!(has_bnode_subject, "new `p` should be a blank node");
}

#[test]
fn match_set_relationship_property_binds_annotation_subject() {
    use fluree_db_query::parse::UnresolvedPattern;
    let txn = lower("MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.since = 2020");
    assert_eq!(txn.txn_type, TxnType::Update);
    // The named relationship lowers to an EdgeAnnotation that binds `r`.
    assert!(
        txn.where_patterns
            .iter()
            .any(|p| matches!(p, UnresolvedPattern::EdgeAnnotation { .. })),
        "where: {:?}",
        txn.where_patterns
    );
    // SET r.since: retract old, insert new — both keyed on the `r` var subject.
    assert_eq!(txn.delete_templates.len(), 1);
    assert_eq!(txn.insert_templates.len(), 1);
    assert!(matches!(
        txn.insert_templates[0].subject,
        TemplateTerm::Var(_)
    ));
}

#[test]
fn match_remove_label_deletes_rdf_type_triple() {
    let txn = lower("MATCH (n:Person) REMOVE n:Person");
    assert_eq!(txn.delete_templates.len(), 1);
    assert!(matches!(
        txn.delete_templates[0].object,
        TemplateTerm::Sid(_)
    ));
}

#[test]
fn set_on_unbound_variable_is_rejected() {
    let out = parse_cypher("MATCH (n:Person) SET m.age = 1");
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(
        r.is_err(),
        "SET on a variable not bound by MATCH should error"
    );
}

#[test]
fn standalone_set_without_match_is_rejected() {
    let out = parse_cypher("SET n.age = 1");
    if out.has_errors() {
        return;
    }
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(r.is_err(), "SET without a preceding MATCH should error");
}
