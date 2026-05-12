//! Cypher write-path lowering tests.
//!
//! These exercise the CREATE-only v1 surface from GQL_CYPHER_SUPPORT.md
//! §M5.4. The generated `Txn` shape is compared to expectations; the
//! shared staging pipeline is exercised by the API-level integration
//! tests once they land.

use fluree_db_cypher::parse_cypher;
use fluree_db_transact::ir::{TemplateTerm, TxnType};
use fluree_db_transact::lower_cypher_update::{lower_cypher_update, CypherLowerOpts};
use fluree_db_transact::namespace::NamespaceRegistry;
use fluree_db_transact::ir::TxnOpts;

fn lower(src: &str) -> fluree_db_transact::ir::Txn {
    let out = parse_cypher(src);
    assert!(!out.has_errors(), "parse errors: {:?}", out.diagnostics);
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    lower_cypher_update(&ast, &mut ns, TxnOpts::default(), CypherLowerOpts::default())
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
    let txn = lower(
        r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
    );
    // 2 labels + 2 props + 1 base edge + 3 reifier bundle triples
    // (subject, predicate, object) = 8 templates.
    assert_eq!(
        txn.insert_templates.len(),
        8,
        "templates: {:?}",
        txn.insert_templates
    );
}

#[test]
fn create_relationship_with_properties_adds_body_triples() {
    let txn = lower(
        r#"CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)"#,
    );
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
    let r = lower_cypher_update(&ast, &mut ns, TxnOpts::default(), CypherLowerOpts::default());
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
    let r = lower_cypher_update(&ast, &mut ns, TxnOpts::default(), CypherLowerOpts::default());
    assert!(r.is_err(), "expected rejection of bare CREATE ()");
}

#[test]
fn read_query_via_update_entry_is_rejected() {
    let out = parse_cypher("MATCH (n:Person) RETURN n");
    let ast = out.ast.unwrap();
    let mut ns = NamespaceRegistry::new();
    let r = lower_cypher_update(&ast, &mut ns, TxnOpts::default(), CypherLowerOpts::default());
    assert!(r.is_err());
}

#[test]
fn merge_set_delete_remove_are_deferred() {
    for src in [
        "MERGE (n:Person {name: \"A\"})",
        "MATCH (n:Person) SET n.age = 42",
        "MATCH (n:Person) DELETE n",
        "MATCH (n:Person) REMOVE n.age",
    ] {
        let out = parse_cypher(src);
        if out.has_errors() {
            continue;
        }
        let ast = out.ast.unwrap();
        let mut ns = NamespaceRegistry::new();
        let r =
            lower_cypher_update(&ast, &mut ns, TxnOpts::default(), CypherLowerOpts::default());
        assert!(
            r.is_err(),
            "expected deferred-feature error for `{src}`, got {:?}",
            r.ok()
        );
    }
}
