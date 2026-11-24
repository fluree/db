//! A ledger's list storage (`f:dataModel` / `f:listStorage`): set when the
//! ledger is created, by its first transaction, and fixed after.

use fluree_db_api::{CreateLedgerOptions, FlureeBuilder, ListStorage};

const PREFIX: &str = "PREFIX f: <https://ns.flur.ee/db#>\nPREFIX ex: <http://example.org/>\n";

fn rdf_lists() -> CreateLedgerOptions {
    CreateLedgerOptions {
        list_storage: ListStorage::Rdf,
    }
}

/// The SPARQL update `body` on `id`, and the error it is refused with.
async fn sparql(fluree: &fluree_db_api::Fluree, id: &str, body: &str) -> Result<(), String> {
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{body}"))
        .commit()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `listStorage` in `id`'s ledger info.
async fn list_storage(fluree: &fluree_db_api::Fluree, id: &str) -> String {
    let info = fluree.ledger_info(id).execute().await.expect("ledger info");
    info["listStorage"]
        .as_str()
        .unwrap_or_else(|| panic!("listStorage in {info}"))
        .to_string()
}

/// The config graph, the config subject and the data model group of `id`,
/// as `ListStorage`'s first transaction names them.
fn names(id: &str) -> (String, String, String) {
    (
        format!("<urn:fluree:{id}#config>"),
        format!("<urn:fluree:{id}:config:ledger>"),
        format!("<urn:fluree:{id}:config:dataModel>"),
    )
}

/// The data model `fields` (on the group) in `id`'s config.
fn data_model(id: &str, fields: &str) -> String {
    let (config, subject, model) = names(id);
    format!(
        "GRAPH {config} {{ {subject} a f:LedgerConfig ; f:dataModel {model} . {model} {fields} }}"
    )
}

/// A ledger created storing RDF lists records that by its first
/// transaction; one created with the defaults stores compact lists and
/// writes nothing.
#[tokio::test]
async fn create_records_the_list_storage() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rdf = fluree
        .create_ledger_with("it/list-storage/rdf:main", rdf_lists())
        .await
        .expect("create");
    assert_eq!(rdf.t(), 1);
    assert_eq!(
        list_storage(&fluree, "it/list-storage/rdf:main").await,
        "rdf"
    );

    let compact = fluree
        .create_ledger_with(
            "it/list-storage/compact:main",
            CreateLedgerOptions::default(),
        )
        .await
        .expect("create");
    assert_eq!(compact.t(), 0);
    assert_eq!(
        list_storage(&fluree, "it/list-storage/compact:main").await,
        "compact"
    );
}

/// Past a ledger's first transaction its list storage does not change: not
/// by setting it, retracting it, or retracting the config subject holding
/// it. Restating it, or stating the default on a ledger of compact lists,
/// changes nothing and is accepted.
#[tokio::test]
async fn the_list_storage_is_fixed_after_the_first_transaction() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-storage/fixed:main";
    fluree.create_ledger_with(id, rdf_lists()).await.unwrap();
    let (config, subject, model) = names(id);
    let fixed = |err: String| {
        assert!(
            err.contains("fixed when the ledger is created")
                && err.contains("RDF lists (f:RdfLists)"),
            "{err}"
        );
    };
    fixed(
        sparql(
            &fluree,
            id,
            &format!(
                "DELETE DATA {{ GRAPH {config} {{ {model} f:listStorage f:RdfLists }} }} ;\n\
                 INSERT DATA {{ GRAPH {config} {{ {model} f:listStorage f:CompactLists }} }}"
            ),
        )
        .await
        .expect_err("switching to compact lists"),
    );
    fixed(
        sparql(
            &fluree,
            id,
            &format!("DELETE DATA {{ GRAPH {config} {{ {model} f:listStorage f:RdfLists }} }}"),
        )
        .await
        .expect_err("retracting the list storage"),
    );
    fixed(
        sparql(
            &fluree,
            id,
            &format!("DELETE DATA {{ GRAPH {config} {{ {subject} a f:LedgerConfig }} }}"),
        )
        .await
        .expect_err("retracting the config subject"),
    );
    sparql(
        &fluree,
        id,
        &format!("INSERT DATA {{ GRAPH {config} {{ {model} f:listStorage f:RdfLists }} }}"),
    )
    .await
    .expect("restating it");
    assert_eq!(list_storage(&fluree, id).await, "rdf");

    let id = "it/list-storage/fixed-compact:main";
    fluree.create_ledger(id).await.unwrap();
    sparql(&fluree, id, "INSERT DATA { ex:s ex:p 1 }")
        .await
        .unwrap();
    let err = sparql(
        &fluree,
        id,
        &format!(
            "INSERT DATA {{ {} }}",
            data_model(id, "f:listStorage f:RdfLists")
        ),
    )
    .await
    .expect_err("RDF lists after the first transaction");
    assert!(
        err.contains("fixed when the ledger is created")
            && err.contains("compact lists (f:CompactLists)"),
        "{err}"
    );
    sparql(
        &fluree,
        id,
        &format!(
            "INSERT DATA {{ {} }}",
            data_model(id, "f:listStorage f:CompactLists")
        ),
    )
    .await
    .expect("stating the default");
    assert_eq!(list_storage(&fluree, id).await, "compact");
}

/// The first transaction sets the list storage only when it writes nothing
/// but config: data beside it would be stored under the config before it.
#[tokio::test]
async fn the_first_transaction_sets_the_list_storage_alone() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-storage/first:main";
    fluree.create_ledger(id).await.unwrap();
    let setting = data_model(id, "f:listStorage f:RdfLists");
    let err = sparql(
        &fluree,
        id,
        &format!("INSERT DATA {{ ex:s ex:p 1 . {setting} }}"),
    )
    .await
    .expect_err("the setting with data");
    assert!(err.contains("a transaction of its own"), "{err}");
    assert_eq!(list_storage(&fluree, id).await, "compact");

    sparql(&fluree, id, &format!("INSERT DATA {{ {setting} }}"))
        .await
        .expect("the setting alone");
    assert_eq!(list_storage(&fluree, id).await, "rdf");
}

/// A data model holds one field, `f:listStorage`, valued `f:CompactLists`
/// or `f:RdfLists`, and sits on the `f:LedgerConfig`. Anything else is
/// refused, `f:overrideControl` included.
#[tokio::test]
async fn a_data_model_holds_only_its_list_storage() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, fields) in [
        (
            "override",
            "f:listStorage f:RdfLists ; f:overrideControl f:OverrideNone",
        ),
        ("field", "f:listStorage f:RdfLists ; f:otherSetting true"),
        ("value", "f:listStorage f:SharedLists"),
        ("string", "f:listStorage \"RdfLists\""),
    ] {
        let id = format!("it/list-storage/holds-{name}:main");
        fluree.create_ledger(&id).await.unwrap();
        let err = sparql(
            &fluree,
            &id,
            &format!("INSERT DATA {{ {} }}", data_model(&id, fields)),
        )
        .await
        .expect_err(name);
        assert!(err.contains("f:dataModel has one field"), "{name}: {err}");
    }

    let id = "it/list-storage/holds-graph:main";
    fluree.create_ledger(id).await.unwrap();
    let (config, subject, model) = names(id);
    let err = sparql(
        &fluree,
        id,
        &format!(
            "INSERT DATA {{ GRAPH {config} {{ {subject} a f:LedgerConfig ; \
             f:graphOverrides <urn:g1> . <urn:g1> a f:GraphConfig ; f:targetGraph ex:g ; \
             f:dataModel {model} . {model} f:listStorage f:RdfLists }} }}"
        ),
    )
    .await
    .expect_err("a data model on a graph override");
    assert!(
        err.contains("belongs on the ledger's f:LedgerConfig"),
        "{err}"
    );
}

/// A branch reads the list storage its ledger was created with.
#[tokio::test]
async fn a_branch_keeps_the_list_storage() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger_with("it/list-storage/branch:main", rdf_lists())
        .await
        .unwrap();
    fluree
        .create_branch("it/list-storage/branch", "dev", None, None)
        .await
        .unwrap();
    assert_eq!(
        list_storage(&fluree, "it/list-storage/branch:dev").await,
        "rdf"
    );
}

// ---------------------------------------------------------------------------
// Writing to a ledger of RDF lists
// ---------------------------------------------------------------------------

/// A ledger of RDF lists named `name`.
async fn rdf_ledger(fluree: &fluree_db_api::Fluree, name: &str) -> String {
    let id = format!("it/list-storage/{name}:main");
    fluree.create_ledger_with(&id, rdf_lists()).await.unwrap();
    id
}

/// The rows of a SPARQL `query` on `id`, sorted, each its values; a blank
/// node is `"_:"`.
async fn rows(fluree: &fluree_db_api::Fluree, id: &str, query: &str) -> Vec<Vec<String>> {
    let ledger = fluree.ledger(id).await.expect("load");
    let text =
        format!("{PREFIX}PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n{query}");
    let result = crate::support::query_sparql(fluree, &ledger, &text)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    let value = |v: &serde_json::Value| {
        let id = v
            .as_str()
            .or_else(|| v.get("@id").and_then(|id| id.as_str()));
        match id {
            Some(id) if id.starts_with("_:") => "_:".to_string(),
            _ => match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            },
        }
    };
    let mut rows: Vec<Vec<String>> = crate::support::normalize_rows(&result)
        .iter()
        .map(|row| match row {
            serde_json::Value::Array(values) => values.iter().map(value).collect(),
            other => vec![value(other)],
        })
        .collect();
    rows.sort();
    rows
}

/// The number of `rdf:first` cells `id` stores.
async fn cells(fluree: &fluree_db_api::Fluree, id: &str) -> usize {
    rows(fluree, id, "SELECT ?c WHERE { ?c rdf:first ?f }")
        .await
        .len()
}

/// `ex:s ex:p`'s value in a JSON-LD crawl of `id`.
async fn crawl(fluree: &fluree_db_api::Fluree, id: &str) -> serde_json::Value {
    let ledger = fluree.ledger(id).await.expect("load");
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:s": ["*"]}
    });
    let out = crate::support::query_jsonld_formatted(fluree, &ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("crawl: {e}"));
    out[0]["ex:p"].clone()
}

/// Every way of writing a list, a list of lists included, stores the cells
/// RDF defines: `ex:s ex:p ?o` binds the head cell, a walk reaches the
/// members, and a JSON-LD crawl reads the list. Before and after indexing.
#[tokio::test]
async fn every_syntax_stores_rdf_lists_as_their_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let writes: [(&str, &str); 5] = [
        ("jsonld", ""),
        ("turtle", "ex:s ex:p ( 1 2 3 ) ."),
        ("sparql", "INSERT DATA { ex:s ex:p ( 1 2 3 ) }"),
        (
            "spelled",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
             ex:s ex:p _:a . _:a rdf:first 1 ; rdf:rest _:b . _:b rdf:first 2 ; rdf:rest _:c .\n\
             _:c rdf:first 3 ; rdf:rest rdf:nil .",
        ),
        ("nested", "ex:s ex:p ( ( 1 2 ) 3 ) ."),
    ];
    for (name, write) in writes {
        let id = rdf_ledger(&fluree, &format!("syntax-{name}")).await;
        match name {
            "jsonld" => {
                fluree
                    .graph(&id)
                    .transact()
                    .insert(&serde_json::json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:s", "ex:p": {"@list": [1, 2, 3]}
                    }))
                    .commit()
                    .await
                    .unwrap();
            }
            "sparql" => sparql(&fluree, &id, write).await.unwrap(),
            _ => {
                fluree
                    .graph(&id)
                    .transact()
                    .insert_turtle(&format!("@prefix ex: <http://example.org/> .\n{write}"))
                    .commit()
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
        let (members, crawled, cell_count) = match name {
            "nested" => (
                vec![vec!["3".to_string()], vec!["_:".to_string()]],
                serde_json::json!({"@list": [{"@list": [1, 2]}, 3]}),
                4,
            ),
            _ => (
                vec![vec!["1".into()], vec!["2".into()], vec!["3".into()]],
                serde_json::json!({"@list": [1, 2, 3]}),
                3,
            ),
        };
        for pass in ["novelty", "index"] {
            if pass == "index" {
                crate::support::rebuild_and_publish_index(&fluree, &id).await;
            }
            assert_eq!(
                rows(&fluree, &id, "SELECT ?o WHERE { ex:s ex:p ?o }").await,
                vec![vec!["_:".to_string()]],
                "{name} ({pass}): the owner holds the head cell"
            );
            assert_eq!(cells(&fluree, &id).await, cell_count, "{name} ({pass})");
            assert_eq!(
                rows(
                    &fluree,
                    &id,
                    "SELECT ?v WHERE { ex:s ex:p/rdf:rest*/rdf:first ?v }"
                )
                .await,
                members,
                "{name} ({pass}): the walk"
            );
            assert_eq!(
                crawl(&fluree, &id).await,
                crawled,
                "{name} ({pass}): the crawl"
            );
        }
    }
}

/// Writing the same list again writes a second list, as RDF has it: each
/// `( … )` is fresh blank nodes. A `DELETE WHERE` collection deletes the
/// cells it matches.
#[tokio::test]
async fn rdf_lists_are_written_and_deleted_as_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = rdf_ledger(&fluree, "again").await;
    for _ in 0..2 {
        sparql(&fluree, &id, "INSERT DATA { ex:s ex:p ( 1 2 ) }")
            .await
            .unwrap();
    }
    assert_eq!(cells(&fluree, &id).await, 4, "two lists");
    assert_eq!(
        rows(&fluree, &id, "SELECT ?o WHERE { ex:s ex:p ?o }")
            .await
            .len(),
        2
    );
    sparql(&fluree, &id, "DELETE WHERE { ex:s ex:p ( 1 2 ) }")
        .await
        .unwrap();
    assert_eq!(cells(&fluree, &id).await, 0, "both lists' cells deleted");
    assert!(rows(&fluree, &id, "SELECT ?o WHERE { ex:s ex:p ?o }")
        .await
        .is_empty());
}

/// A `@list` in a JSON-LD delete names a compact list by its members; a
/// ledger of RDF lists refuses it rather than deleting nothing.
#[tokio::test]
async fn a_jsonld_list_delete_is_refused_for_rdf_lists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = rdf_ledger(&fluree, "jsonld-delete").await;
    sparql(&fluree, &id, "INSERT DATA { ex:s ex:p ( 1 2 ) }")
        .await
        .unwrap();
    let err = fluree
        .graph(&id)
        .transact()
        .update(&serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "delete": {"@id": "ex:s", "ex:p": {"@list": [1, 2]}}
        }))
        .commit()
        .await
        .expect_err("a @list delete");
    assert!(err.to_string().contains("stores RDF lists"), "{err}");
    assert_eq!(cells(&fluree, &id).await, 2);
}
