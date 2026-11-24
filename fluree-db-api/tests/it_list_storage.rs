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
