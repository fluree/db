//! Cypher write-path lowering tests.
//!
//! These exercise the CREATE-only v1 surface from GQL_CYPHER_SUPPORT.md
//! §M5.4. The generated `Txn` shape is compared to expectations; the
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
        // Relationship MERGE is deferred.
        "MERGE (a:Person {name: \"A\"})-[:KNOWS]->(b:Person {name: \"B\"})",
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
fn set_map_replace_is_deferred() {
    let out = parse_cypher(r#"MATCH (n:Person {name: "A"}) SET n = {age: 1}"#);
    assert!(!out.has_errors());
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(
        &ast,
        &mut ns,
        TxnOpts::default(),
        CypherLowerOpts::default(),
    );
    assert!(r.is_err(), "SET n = {{…}} should be deferred");
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
