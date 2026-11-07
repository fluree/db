//! `f:additionalDataSource`: graphs read with the validated graph, as one
//! graph, when SHACL checks its nodes. Which nodes are checked still comes
//! from the graph being written.

#![cfg(all(feature = "native", feature = "shacl"))]

use fluree_db_api::validate::ValidateOptions;
use fluree_db_api::{ApiError, Fluree, FlureeBuilder};
use fluree_db_transact::TransactError;

const PREFIXES: &str = r"
    @prefix f:   <https://ns.flur.ee/db#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix ex:  <http://example.org/> .
";

const SHAPES: &str = r#"
    GRAPH <http://example.org/shapes> {
        ex:ListingShape rdf:type sh:NodeShape ;
            sh:targetClass ex:Listing ;
            sh:property ex:listingCommodity .
        ex:listingCommodity sh:path ex:commodity ;
            sh:class ex:Commodity ;
            sh:maxCount 1 .

        ex:OrderShape rdf:type sh:NodeShape ;
            sh:targetClass ex:Order ;
            sh:property ex:orderPrice .
        ex:orderPrice sh:path ( ex:product ex:price ) ;
            sh:minCount 1 .

        ex:ShipmentShape rdf:type sh:NodeShape ;
            sh:targetClass ex:Shipment ;
            sh:property ex:shipmentItem .
        ex:shipmentItem sh:path ex:item ;
            sh:node ex:ActiveShape .
        ex:ActiveShape rdf:type sh:NodeShape ;
            sh:property ex:activeStatus .
        ex:activeStatus sh:path ex:status ;
            sh:hasValue "active" .

        ex:AuditShape rdf:type sh:NodeShape ;
            sh:targetClass ex:Audited ;
            sh:sparql ex:auditedCommodity .
        ex:auditedCommodity sh:message "commodity is not in the catalog" ;
            sh:select "SELECT $this ?value WHERE { $this <http://example.org/commodity> ?value . FILTER NOT EXISTS { ?value a <http://example.org/Commodity> } }" .

        ex:FlaggedShape rdf:type sh:NodeShape ;
            sh:targetSubjectsOf ex:flag ;
            sh:targetObjectsOf ex:flags ;
            sh:property ex:flaggedReason .
        ex:flaggedReason sh:path ex:reason ;
            sh:minCount 1 .
    }
"#;

const CATALOG: &str = "http://example.org/catalog";
const PRICING: &str = "http://example.org/pricing";
const ORG_A: &str = "http://example.org/org-a";
const ORG_B: &str = "http://example.org/org-b";

/// `owner`'s `f:additionalDataSource` list, each source a Turtle term naming
/// a graph.
fn sources_trig(owner: &str, sources: &[&str]) -> String {
    sources
        .iter()
        .enumerate()
        .map(|(i, selector)| {
            format!(
                "<{owner}> f:additionalDataSource <{owner}-src{i}> .
                 <{owner}-src{i}> rdf:type f:GraphRef ; f:graphSource <{owner}-src{i}-graph> .
                 <{owner}-src{i}-graph> f:graphSelector {selector} .\n"
            )
        })
        .collect()
}

/// The config graph: SHACL on, shapes from `<shapes>`, `sources` as the
/// ledger-wide list, and a per-graph list for each graph in `overrides`.
fn config_trig(ledger_id: &str, sources: &[&str], overrides: &[(&str, &[&str])]) -> String {
    let mut body = format!(
        "<urn:config:main> rdf:type f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
         <urn:config:shacl> f:shaclEnabled true ; f:shapesSource <urn:config:shapes> .
         <urn:config:shapes> rdf:type f:GraphRef ; f:graphSource <urn:config:shapes-graph> .
         <urn:config:shapes-graph> f:graphSelector <http://example.org/shapes> .\n{}",
        sources_trig("urn:config:shacl", sources)
    );
    for (i, (graph, graph_sources)) in overrides.iter().enumerate() {
        let shacl = format!("urn:config:override{i}-shacl");
        body.push_str(&format!(
            "<urn:config:main> f:graphOverrides <urn:config:override{i}> .
             <urn:config:override{i}> rdf:type f:GraphConfig ;
                 f:targetGraph <{graph}> ; f:shaclDefaults <{shacl}> .\n{}",
            sources_trig(&shacl, graph_sources)
        ));
    }
    format!("GRAPH <urn:fluree:{ledger_id}#config> {{\n{body}}}\n")
}

/// A ledger holding the shapes, `data` (TriG) and the config, all written in
/// one transaction, which the new config does not yet govern.
async fn ledger(
    name: &str,
    data: &str,
    sources: &[&str],
    overrides: &[(&str, &[&str])],
) -> (Fluree, String) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(name).await.expect("create ledger");
    let ledger_id = format!("{name}:main");
    write(
        &fluree,
        &ledger_id,
        &format!(
            "{SHAPES}{data}{}",
            config_trig(&ledger_id, sources, overrides)
        ),
    )
    .await
    .expect("shapes, data and config");
    (fluree, ledger_id)
}

async fn write(fluree: &Fluree, ledger_id: &str, trig: &str) -> fluree_db_api::Result<()> {
    fluree
        .graph(ledger_id)
        .transact()
        .upsert_turtle(&format!("{PREFIXES}{trig}"))
        .commit()
        .await
        .map(|_| ())
}

async fn sparql_update(
    fluree: &Fluree,
    ledger_id: &str,
    update: &str,
) -> fluree_db_api::Result<()> {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&format!("PREFIX ex: <http://example.org/>\n{update}"))
        .commit()
        .await
        .map(|_| ())
}

fn in_graph(graph: &str, triples: &str) -> String {
    format!("GRAPH <{graph}> {{ {triples} }}\n")
}

fn assert_violation(result: fluree_db_api::Result<()>, why: &str) {
    match result {
        Err(ApiError::Transact(TransactError::ShaclViolation(_))) => {}
        other => panic!("{why}: expected a SHACL violation, got {other:?}"),
    }
}

fn assert_error_mentions(result: fluree_db_api::Result<()>, expected: &str) {
    match result {
        Err(err) => assert!(
            err.to_string().contains(expected),
            "expected an error mentioning {expected:?}, got: {err}"
        ),
        Ok(()) => panic!("expected an error mentioning {expected:?}, the write succeeded"),
    }
}

#[tokio::test]
async fn without_a_source_the_written_graph_is_read_alone() {
    let catalog = in_graph(CATALOG, "ex:coffee a ex:Commodity .");
    let (fluree, ledger_id) = ledger("it/additional-data/unset", &catalog, &[], &[]).await;

    let listing = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    assert_violation(
        write(&fluree, &ledger_id, &listing).await,
        "the catalog graph is not read unless configured",
    );
}

#[tokio::test]
async fn sh_class_reads_a_source_graph() {
    let catalog = in_graph(CATALOG, "ex:coffee a ex:Commodity .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/class",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let member = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &member)
        .await
        .expect("a commodity typed in the catalog graph satisfies sh:class");

    let non_member = in_graph(ORG_A, "ex:listing-2 a ex:Listing ; ex:commodity ex:tea .");
    assert_violation(
        write(&fluree, &ledger_id, &non_member).await,
        "a value typed in no graph read is not a member",
    );
}

/// The value's type and the type's superclass edge are both in the source,
/// and the edge counts whichever graph is written, the source itself
/// included. A graph that is not listed has its own subclass edges ignored,
/// as without sources.
#[tokio::test]
async fn sh_class_follows_subclass_edges_in_a_source() {
    let catalog = in_graph(
        CATALOG,
        "ex:coffee a ex:Arabica . ex:Arabica rdfs:subClassOf ex:Commodity .",
    );
    let (fluree, ledger_id) = ledger(
        "it/additional-data/subclass",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let listing = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &listing)
        .await
        .expect("an Arabica is a Commodity by the catalog's subclass edge");

    let in_catalog = in_graph(
        CATALOG,
        "ex:listing-2 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &in_catalog)
        .await
        .expect("the catalog lists itself, so its subclass edge counts when it is written");

    let own_edge = in_graph(
        ORG_A,
        "ex:tea a ex:Assam . ex:Assam rdfs:subClassOf ex:Commodity .
         ex:listing-3 a ex:Listing ; ex:commodity ex:tea .",
    );
    assert_violation(
        write(&fluree, &ledger_id, &own_edge).await,
        "org-a is not listed, so its own subclass edge is not read",
    );
}

/// Which shapes apply is decided from the written graph: a type the source
/// alone gives a node does not make the node's shape apply there.
#[tokio::test]
async fn a_type_only_in_a_source_does_not_target() {
    let catalog = in_graph(CATALOG, "ex:listing-1 a ex:Listing .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/source-type",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let untyped = in_graph(ORG_A, "ex:listing-1 ex:commodity ex:tea .");
    write(&fluree, &ledger_id, &untyped)
        .await
        .expect("org-a does not type listing-1, so the listing shape does not apply");

    let typed = in_graph(ORG_A, "ex:listing-2 a ex:Listing ; ex:commodity ex:tea .");
    assert_violation(
        write(&fluree, &ledger_id, &typed).await,
        "typed in org-a, the listing shape applies",
    );
}

/// Likewise for `sh:targetSubjectsOf` and `sh:targetObjectsOf`: an edge only
/// the source holds does not make the shape apply in the written graph.
#[tokio::test]
async fn a_predicate_target_only_in_a_source_does_not_target() {
    let catalog = in_graph(
        CATALOG,
        "ex:item-1 ex:flag true . ex:auditor ex:flags ex:item-2 .",
    );
    let (fluree, ledger_id) = ledger(
        "it/additional-data/source-predicate",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let unflagged = in_graph(ORG_A, r#"ex:item-1 ex:note "a" . ex:item-2 ex:note "b" ."#);
    write(&fluree, &ledger_id, &unflagged)
        .await
        .expect("org-a holds neither flag edge, so the flagged shape does not apply");

    let subject_of = in_graph(ORG_A, "ex:item-3 ex:flag true .");
    assert_violation(
        write(&fluree, &ledger_id, &subject_of).await,
        "flagged in org-a, the shape applies to the subject",
    );
    let object_of = in_graph(ORG_A, "ex:auditor ex:flags ex:item-4 .");
    assert_violation(
        write(&fluree, &ledger_id, &object_of).await,
        "flagged in org-a, the shape applies to the object",
    );
}

/// The order and its product are in the written graph, the product's price
/// in the source: the path has to step from one graph into the other.
#[tokio::test]
async fn a_path_steps_from_the_written_graph_into_a_source() {
    let pricing = in_graph(PRICING, "ex:widget ex:price 10 .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/path",
        &pricing,
        &[&format!("<{PRICING}>")],
        &[],
    )
    .await;

    let priced = in_graph(ORG_A, "ex:order-1 a ex:Order ; ex:product ex:widget .");
    write(&fluree, &ledger_id, &priced)
        .await
        .expect("the path reaches the price in the pricing graph");

    let unpriced = in_graph(ORG_A, "ex:order-2 a ex:Order ; ex:product ex:gadget .");
    assert_violation(
        write(&fluree, &ledger_id, &unpriced).await,
        "a product with no price anywhere fails sh:minCount",
    );
}

#[tokio::test]
async fn a_nested_shape_reads_the_referenced_node_from_a_source() {
    let catalog = in_graph(
        CATALOG,
        r#"ex:coffee ex:status "active" . ex:tea ex:status "retired" ."#,
    );
    let (fluree, ledger_id) = ledger(
        "it/additional-data/node",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let active = in_graph(ORG_A, "ex:shipment-1 a ex:Shipment ; ex:item ex:coffee .");
    write(&fluree, &ledger_id, &active)
        .await
        .expect("the item's status is read from the catalog graph");

    let retired = in_graph(ORG_A, "ex:shipment-2 a ex:Shipment ; ex:item ex:tea .");
    assert_violation(
        write(&fluree, &ledger_id, &retired).await,
        "a retired item fails the nested shape",
    );
}

/// A fact stored in both graphs counts once, and a fact only the source holds
/// counts too: a source can make a record fail as well as pass.
#[tokio::test]
async fn a_fact_in_two_graphs_counts_once() {
    let catalog = in_graph(
        CATALOG,
        "ex:coffee a ex:Commodity . ex:tea a ex:Commodity .
         ex:listing-1 ex:commodity ex:coffee .
         ex:listing-2 ex:commodity ex:tea .",
    );
    let (fluree, ledger_id) = ledger(
        "it/additional-data/duplicates",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let same_fact = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &same_fact)
        .await
        .expect("the commodity in both graphs is one value for sh:maxCount 1");

    let second_value = in_graph(
        ORG_A,
        "ex:listing-2 a ex:Listing ; ex:commodity ex:coffee .",
    );
    assert_violation(
        write(&fluree, &ledger_id, &second_value).await,
        "the source's own commodity is a second value for sh:maxCount 1",
    );
}

#[tokio::test]
async fn sh_sparql_reads_the_union() {
    let catalog = in_graph(CATALOG, "ex:coffee a ex:Commodity .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/sparql",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let member = in_graph(ORG_A, "ex:audit-1 a ex:Audited ; ex:commodity ex:coffee .");
    write(&fluree, &ledger_id, &member)
        .await
        .expect("the constraint query sees the catalog's typing");

    let non_member = in_graph(ORG_A, "ex:audit-2 a ex:Audited ; ex:commodity ex:tea .");
    assert_violation(
        write(&fluree, &ledger_id, &non_member).await,
        "the constraint query finds no typing for tea",
    );
}

#[tokio::test]
async fn checks_read_the_transactions_own_changes_to_a_source() {
    let catalog = in_graph(CATALOG, "ex:coffee a ex:Commodity .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/staged",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let added_together = format!(
        "{}{}",
        in_graph(CATALOG, "ex:tea a ex:Commodity ."),
        in_graph(ORG_A, "ex:listing-1 a ex:Listing ; ex:commodity ex:tea ."),
    );
    write(&fluree, &ledger_id, &added_together)
        .await
        .expect("a commodity added in the same transaction is a member");

    assert_violation(
        sparql_update(
            &fluree,
            &ledger_id,
            &format!(
                "DELETE DATA {{ GRAPH <{CATALOG}> {{ ex:coffee a ex:Commodity }} }} ;
                 INSERT DATA {{ GRAPH <{ORG_A}> {{ ex:listing-2 a ex:Listing ; ex:commodity ex:coffee }} }}"
            ),
        )
        .await,
        "a commodity removed in the same transaction is not a member",
    );
}

/// Records are checked when they are written: a change to a source alone does
/// not re-check the records that read it.
#[tokio::test]
async fn a_source_change_does_not_recheck_records_already_written() {
    let catalog = in_graph(CATALOG, "ex:coffee a ex:Commodity .");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/recheck",
        &catalog,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;
    let listing = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &listing)
        .await
        .expect("listing references a catalog commodity");

    sparql_update(
        &fluree,
        &ledger_id,
        &format!("DELETE DATA {{ GRAPH <{CATALOG}> {{ ex:coffee a ex:Commodity }} }}"),
    )
    .await
    .expect("removing a commodity does not re-check the listings that use it");

    let touched = in_graph(ORG_A, r#"ex:listing-1 ex:label "coffee, 1kg" ."#);
    assert_violation(
        write(&fluree, &ledger_id, &touched).await,
        "the listing is checked again when it is next written",
    );
}

#[tokio::test]
async fn the_write_that_creates_a_source_graph_reads_it() {
    let (fluree, ledger_id) = ledger(
        "it/additional-data/bootstrap",
        "",
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    assert_error_mentions(
        write(&fluree, &ledger_id, &in_graph(ORG_B, "ex:x ex:p 1 .")).await,
        &format!("f:additionalDataSource graph '{CATALOG}' not found"),
    );

    let together = format!(
        "{}{}",
        in_graph(CATALOG, "ex:coffee a ex:Commodity ."),
        in_graph(
            ORG_A,
            "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee ."
        ),
    );
    write(&fluree, &ledger_id, &together)
        .await
        .expect("the transaction creating the catalog graph reads it");
}

#[tokio::test]
async fn the_system_graphs_cannot_be_sources() {
    for (name, selector, expected) in [
        (
            "it/additional-data/txn-meta",
            "f:txnMetaGraph".to_string(),
            "cannot name the txn-meta graph",
        ),
        (
            "it/additional-data/config-graph",
            "<urn:fluree:it/additional-data/config-graph:main#config>".to_string(),
            "cannot name the config graph",
        ),
    ] {
        let (fluree, ledger_id) = ledger(name, "", &[&selector], &[]).await;
        let listing = in_graph(ORG_A, "ex:listing-1 a ex:Listing .");
        assert_error_mentions(write(&fluree, &ledger_id, &listing).await, expected);
    }
}

#[tokio::test]
async fn a_per_graph_list_replaces_the_ledger_list() {
    let data = format!(
        "{}{}",
        in_graph(CATALOG, "ex:coffee a ex:Commodity ."),
        in_graph(PRICING, "ex:widget ex:price 10 ."),
    );
    let pricing = format!("<{PRICING}>");
    let (fluree, ledger_id) = ledger(
        "it/additional-data/override",
        &data,
        &[&format!("<{CATALOG}>")],
        &[(ORG_A, &[&pricing])],
    )
    .await;

    let org_a_listing = in_graph(
        ORG_A,
        "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .",
    );
    assert_violation(
        write(&fluree, &ledger_id, &org_a_listing).await,
        "org-a's list replaces the ledger's, so the catalog is not read",
    );
    let org_a_order = in_graph(ORG_A, "ex:order-1 a ex:Order ; ex:product ex:widget .");
    write(&fluree, &ledger_id, &org_a_order)
        .await
        .expect("org-a reads its own source");
    let org_b_listing = in_graph(
        ORG_B,
        "ex:listing-2 a ex:Listing ; ex:commodity ex:coffee .",
    );
    write(&fluree, &ledger_id, &org_b_listing)
        .await
        .expect("org-b inherits the ledger's list");
}

/// The validate API reads the same graphs as transaction-time checks.
#[tokio::test]
async fn validate_reads_the_sources_too() {
    let data = format!(
        "{}{}",
        in_graph(CATALOG, "ex:coffee a ex:Commodity ."),
        in_graph(
            ORG_A,
            "ex:listing-1 a ex:Listing ; ex:commodity ex:coffee .
             ex:listing-2 a ex:Listing ; ex:commodity ex:tea .",
        ),
    );
    let (fluree, ledger_id) = ledger(
        "it/additional-data/validate",
        &data,
        &[&format!("<{CATALOG}>")],
        &[],
    )
    .await;

    let report = fluree
        .validate_ledger(
            &ledger_id,
            &ValidateOptions {
                graph: Some(ORG_A.to_string()),
                ..ValidateOptions::default()
            },
        )
        .await
        .expect("validate org-a");
    let failing: Vec<_> = report
        .results
        .iter()
        .map(|result| result.focus_node.as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        failing,
        ["http://example.org/listing-2"],
        "only the listing whose commodity no graph types fails: {report:?}"
    );
}
