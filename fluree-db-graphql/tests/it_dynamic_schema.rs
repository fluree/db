//! The dynamic schema — built at runtime from a [`SchemaModel`], one root
//! resolver that receives the entire selection subtree, and generic pass-through
//! resolvers for everything nested.
//!
//! What has to hold for the design to work:
//!  * recursive input objects (`PersonFilter.AND: [PersonFilter!]`, `knows: PersonFilter`) register;
//!  * interfaces, unions and custom scalars register and introspect;
//!  * the root resolver can recover its own selection subtree *with fragment type
//!    conditions intact* — which is why the tree is extracted from the parsed
//!    document rather than from `SelectionField::selection_set` (see `selection.rs`);
//!  * nested pass-throughs resolve lists, objects, aliases, and union members.

use std::sync::{Arc, Mutex};

use async_graphql::{Request, Variables};
use fluree_db_graphql::error::Result as GqlResult;
use fluree_db_graphql::limits::Limits;
use fluree_db_graphql::runtime::{build_schema, ExecutorData, RootExecutor, RootRequest};
use fluree_db_graphql::schema::model::{
    Direction, EnumType, Field, FieldType, InterfaceType, ObjectType, Provenance, RootField,
    RootKind, Scalar, SchemaModel, UnionType,
};
use fluree_db_graphql::selection::{self, Selection};
use serde_json::json;

const INF: Provenance = Provenance::Inferred;

fn scalar_field(name: &str, iri: &str, s: Scalar, list: bool) -> Field {
    Field {
        name: name.to_string(),
        iri: iri.to_string(),
        direction: Direction::Forward,
        ty: FieldType::Scalar(s),
        list,
        non_null: false,
        description: None,
        language_tagged: false,
        provenance: INF,
    }
}

fn ref_field(name: &str, iri: &str, ty: FieldType, list: bool) -> Field {
    Field {
        name: name.to_string(),
        iri: iri.to_string(),
        direction: Direction::Forward,
        ty,
        list,
        non_null: false,
        description: None,
        language_tagged: false,
        provenance: INF,
    }
}

fn root(name: &str, type_name: &str, kind: RootKind) -> RootField {
    RootField {
        name: name.to_string(),
        class_iri: format!("http://example.org/{type_name}"),
        type_name: type_name.to_string(),
        kind,
        description: None,
        provenance: INF,
    }
}

/// A model exercising every construct the builders can emit.
fn fixture_model() -> SchemaModel {
    let agent_fields = vec![
        Field::id_field(INF),
        scalar_field(
            "name",
            "http://xmlns.com/foaf/0.1/name",
            Scalar::String,
            false,
        ),
    ];

    let person = ObjectType {
        name: "Person".to_string(),
        iri: "http://example.org/Person".to_string(),
        description: Some("A person.".to_string()),
        implements: vec!["Agent".to_string()],
        fields: vec![
            Field::id_field(INF),
            scalar_field(
                "name",
                "http://xmlns.com/foaf/0.1/name",
                Scalar::String,
                false,
            ),
            scalar_field("age", "http://example.org/age", Scalar::Int, false),
            scalar_field(
                "birthday",
                "http://example.org/birthday",
                Scalar::Date,
                false,
            ),
            ref_field(
                "knows",
                "http://xmlns.com/foaf/0.1/knows",
                FieldType::Object("Person".to_string()),
                true,
            ),
            Field {
                direction: Direction::Reverse,
                ..ref_field(
                    "employees",
                    "http://example.org/worksFor",
                    FieldType::Object("Person".to_string()),
                    true,
                )
            },
            ref_field(
                "status",
                "http://example.org/status",
                FieldType::Enum("Status".to_string()),
                false,
            ),
        ],
        provenance: INF,
    };

    let organization = ObjectType {
        name: "Organization".to_string(),
        iri: "http://example.org/Organization".to_string(),
        description: None,
        implements: vec!["Agent".to_string()],
        fields: agent_fields.clone(),
        provenance: INF,
    };

    let document = ObjectType {
        name: "Document".to_string(),
        iri: "http://example.org/Document".to_string(),
        description: None,
        implements: vec![],
        fields: vec![
            Field::id_field(INF),
            scalar_field(
                "title",
                "http://purl.org/dc/terms/title",
                Scalar::String,
                false,
            ),
            ref_field(
                "owner",
                "http://example.org/owner",
                FieldType::Union("Owner".to_string()),
                false,
            ),
        ],
        provenance: INF,
    };

    let mut model = SchemaModel {
        objects: vec![person, organization, document],
        interfaces: vec![InterfaceType {
            name: "Agent".to_string(),
            iri: "http://example.org/Agent".to_string(),
            description: None,
            implements: vec![],
            fields: agent_fields,
            provenance: INF,
        }],
        unions: vec![UnionType {
            name: "Owner".to_string(),
            description: None,
            members: vec!["Person".to_string(), "Organization".to_string()],
            provenance: INF,
        }],
        enums: vec![EnumType {
            name: "Status".to_string(),
            description: None,
            values: vec![
                (
                    "ACTIVE".to_string(),
                    "http://example.org/Active".to_string(),
                ),
                (
                    "RETIRED".to_string(),
                    "http://example.org/Retired".to_string(),
                ),
            ],
            iri_valued: true,
            provenance: INF,
        }],
        query_fields: vec![
            root("person", "Person", RootKind::Single),
            root("persons", "Person", RootKind::List),
            root("persons_count", "Person", RootKind::Count),
            root("documents", "Document", RootKind::List),
        ],
        warnings: vec![],
    };
    model.sort();
    model
}

/// Records what the root resolver was handed, and replays canned JSON.
struct StubExecutor {
    seen: Mutex<Vec<RootRequest>>,
    responses: Vec<(String, serde_json::Value)>,
}

impl StubExecutor {
    fn new(responses: Vec<(&str, serde_json::Value)>) -> Arc<Self> {
        Arc::new(StubExecutor {
            seen: Mutex::new(Vec::new()),
            responses: responses
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        })
    }

    fn selection(&self, root_field: &str) -> Selection {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.root.name == root_field)
            .unwrap_or_else(|| panic!("root field `{root_field}` was never resolved"))
            .selection
            .clone()
    }
}

#[async_trait::async_trait]
impl RootExecutor for StubExecutor {
    async fn resolve(&self, request: RootRequest) -> GqlResult<serde_json::Value> {
        let key = request.root.name.clone();
        self.seen.lock().unwrap().push(request);
        Ok(self
            .responses
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or(serde_json::Value::Null))
    }
}

async fn run(
    query: &str,
    variables: Variables,
    executor: Arc<StubExecutor>,
) -> async_graphql::Response {
    let model = fixture_model();
    // The schema is executor-agnostic — that is what lets one registration be
    // cached and shared — so the executor rides on the request instead.
    let schema = build_schema(&model, &[], &Limits::default()).expect("schema builds");
    let doc = async_graphql::parser::parse_query(query).expect("document parses");
    let op =
        selection::extract(&doc, None, &variables, &Limits::default()).expect("selection extracts");
    schema
        .execute(
            Request::new(query)
                .variables(variables)
                .data(Arc::new(op))
                .data(executor as ExecutorData),
        )
        .await
}

#[test]
fn sdl_renders_every_construct() {
    let sdl = fluree_db_graphql::sdl(&fixture_model()).expect("sdl renders");

    for expected in [
        "interface Agent",
        "type Person implements Agent",
        "type Organization implements Agent",
        "union Owner = Organization | Person",
        "enum Status",
        "scalar Date",
        "input PersonFilter",
        "input PersonOrder",
        "input PersonNestedOrder",
        "input StringFilter",
        "enum SortDirection",
    ] {
        assert!(
            sdl.contains(expected),
            "SDL is missing `{expected}`:\n{sdl}"
        );
    }

    // Root field signatures, including the shaping arguments on a nested list field.
    assert!(sdl.contains("person(id: ID!): Person"), "{sdl}");
    assert!(
        sdl.contains(
            "persons(where: PersonFilter, limit: Int, offset: Int, orderBy: PersonOrder): [Person!]"
        ),
        "{sdl}"
    );
    assert!(
        sdl.contains("persons_count(where: PersonFilter): Int!"),
        "{sdl}"
    );
    // A nested list takes the permissive order input: it sorts one subject's
    // values, so a multi-valued key is allowed there but not at the root.
    assert!(
        sdl.contains(
            "knows(where: PersonFilter, limit: Int, offset: Int, orderBy: PersonNestedOrder): \
             [Person!]"
        ),
        "{sdl}"
    );

    // Recursive and cross-type filter inputs.
    assert!(sdl.contains("AND: [PersonFilter!]"), "{sdl}");
    assert!(sdl.contains("knows: PersonFilter"), "{sdl}");
    // Object-valued fields are never a sort key, in either position.
    assert!(!sdl.contains("knows: SortDirection"), "{sdl}");
    // The root input takes only single-valued keys; the nested one also takes
    // the multi-valued `employees`.
    let root_order = sdl
        .split("input PersonOrder")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    assert!(root_order.contains("name: SortDirection"), "{root_order}");
    assert!(!root_order.contains("employees"), "{root_order}");
    let nested_order = sdl
        .split("input PersonNestedOrder")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    assert!(
        nested_order.contains("name: SortDirection"),
        "{nested_order}"
    );
    // Regex operators only on String.
    assert!(sdl.contains("IRE: String"), "{sdl}");
    let int_filter = sdl
        .split("input IntFilter")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    assert!(
        !int_filter.contains("RE:"),
        "IntFilter should have no regex ops: {int_filter}"
    );
    assert!(int_filter.contains("GTE: Int"), "{int_filter}");
}

#[tokio::test]
async fn root_resolver_receives_the_whole_selection_subtree() {
    let executor = StubExecutor::new(vec![("persons", json!([]))]);
    let resp = run(
        "query {
          persons(limit: 2) {
            id
            fullName: name
            knows(limit: 1) { id name }
          }
        }",
        Variables::default(),
        executor.clone(),
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);

    let sel = executor.selection("persons");
    assert_eq!(sel.response_key, "persons");
    assert_eq!(
        sel.argument("limit").map(ToString::to_string),
        Some("2".to_string())
    );
    let keys: Vec<_> = sel
        .children
        .iter()
        .map(|c| c.response_key.as_str())
        .collect();
    assert_eq!(keys, ["id", "fullName", "knows"]);
    // The alias is separate from the schema field name; lowering needs both.
    let aliased = &sel.children[1];
    assert_eq!(aliased.name, "name");
    assert_eq!(aliased.response_key, "fullName");
    // Nested args reach the executor, which is what makes per-level limits possible.
    let knows = &sel.children[2];
    assert_eq!(
        knows.argument("limit").map(ToString::to_string),
        Some("1".to_string())
    );
    assert_eq!(knows.children.len(), 2);
}

#[tokio::test]
async fn fragment_type_conditions_survive_extraction() {
    // This is the case async-graphql's own resolver-facing selection API loses:
    // both `SelectionField::selection_set` and `Lookahead` flatten fragments and
    // discard `on Person` / `on Organization`.
    let executor = StubExecutor::new(vec![("documents", json!([]))]);
    let resp = run(
        "query {
          documents {
            id
            owner {
              ... on Person { name age }
              ...OrgFields
            }
          }
        }
        fragment OrgFields on Organization { name }",
        Variables::default(),
        executor.clone(),
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);

    let owner = executor
        .selection("documents")
        .children
        .into_iter()
        .find(|c| c.name == "owner")
        .unwrap();
    let conditions: Vec<_> = owner
        .children
        .iter()
        .map(|c| (c.name.as_str(), c.type_condition.as_deref()))
        .collect();
    assert_eq!(
        conditions,
        [
            ("name", Some("Person")),
            ("age", Some("Person")),
            ("name", Some("Organization")),
        ]
    );
}

#[tokio::test]
async fn pass_through_resolvers_materialise_nested_json() {
    let executor = StubExecutor::new(vec![(
        "persons",
        json!([
            {
                "id": "ex:alice",
                "fullName": "Alice",
                "age": 34,
                "knows": [
                    { "id": "ex:bob", "name": "Bob" },
                    { "id": "ex:carol", "name": "Carol" }
                ]
            },
            { "id": "ex:dave", "fullName": "Dave", "age": null, "knows": [] }
        ]),
    )]);

    let resp = run(
        "query {
          persons {
            id
            fullName: name
            age
            knows { id name }
          }
        }",
        Variables::default(),
        executor,
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    assert_eq!(
        resp.data.into_json().unwrap(),
        json!({
            "persons": [
                {
                    "id": "ex:alice",
                    "fullName": "Alice",
                    "age": 34,
                    "knows": [
                        { "id": "ex:bob", "name": "Bob" },
                        { "id": "ex:carol", "name": "Carol" }
                    ]
                },
                { "id": "ex:dave", "fullName": "Dave", "age": null, "knows": [] }
            ]
        })
    );
}

#[tokio::test]
async fn union_members_resolve_from_typename() {
    let executor = StubExecutor::new(vec![(
        "documents",
        json!([
            { "id": "ex:d1", "owner": { "__typename": "Person", "name": "Alice", "age": 34 } },
            { "id": "ex:d2", "owner": { "__typename": "Organization", "name": "Acme" } },
            { "id": "ex:d3", "owner": null }
        ]),
    )]);

    let resp = run(
        "query {
          documents {
            id
            owner {
              __typename
              ... on Person { name age }
              ... on Organization { name }
            }
          }
        }",
        Variables::default(),
        executor,
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    assert_eq!(
        resp.data.into_json().unwrap(),
        json!({
            "documents": [
                { "id": "ex:d1", "owner": { "__typename": "Person", "name": "Alice", "age": 34 } },
                { "id": "ex:d2", "owner": { "__typename": "Organization", "name": "Acme" } },
                { "id": "ex:d3", "owner": null }
            ]
        })
    );
}

#[tokio::test]
async fn count_and_single_roots() {
    let executor = StubExecutor::new(vec![
        ("persons_count", json!(7)),
        ("person", json!({ "name": "Alice" })),
    ]);
    let resp = run(
        "query { persons_count(where: { age: { GT: 30 } }) person(id: \"ex:alice\") { name } }",
        Variables::default(),
        executor.clone(),
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    assert_eq!(
        resp.data.into_json().unwrap(),
        json!({ "persons_count": 7, "person": { "name": "Alice" } })
    );

    // The `where` argument arrives as a structured value, not a string.
    let filter = executor.selection("persons_count");
    assert_eq!(
        filter.argument("where").map(ToString::to_string),
        Some("{age: {GT: 30}}".to_string())
    );
}

#[tokio::test]
async fn variables_are_substituted_including_defaults() {
    let executor = StubExecutor::new(vec![("persons", json!([]))]);
    let resp = run(
        "query Q($limit: Int = 5, $min: Int) {
          persons(limit: $limit, where: { age: { GTE: $min } }) { id }
        }",
        Variables::from_json(json!({ "min": 21 })),
        executor.clone(),
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);

    let sel = executor.selection("persons");
    assert_eq!(
        sel.argument("limit").map(ToString::to_string),
        Some("5".to_string()),
        "the variable default should be applied"
    );
    assert_eq!(
        sel.argument("where").map(ToString::to_string),
        Some("{age: {GTE: 21}}".to_string())
    );
}

#[tokio::test]
async fn introspection_round_trips() {
    let executor = StubExecutor::new(vec![]);
    let resp = run(
        "query {
          __schema { queryType { name } types { name kind } }
          person: __type(name: \"Person\") {
            kind
            interfaces { name }
            fields { name type { kind ofType { kind name } } }
          }
          owner: __type(name: \"Owner\") { kind possibleTypes { name } }
        }",
        Variables::default(),
        executor,
    )
    .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);

    let data = resp.data.into_json().unwrap();
    assert_eq!(data["__schema"]["queryType"]["name"], "Query");
    let type_names: Vec<&str> = data["__schema"]["types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for expected in [
        "Person",
        "Organization",
        "Document",
        "Agent",
        "Owner",
        "Status",
        "Date",
        "PersonFilter",
        "SortDirection",
    ] {
        assert!(
            type_names.contains(&expected),
            "introspection is missing {expected}"
        );
    }

    assert_eq!(data["person"]["kind"], "OBJECT");
    assert_eq!(data["person"]["interfaces"][0]["name"], "Agent");
    assert_eq!(data["owner"]["kind"], "UNION");
    let mut possible: Vec<&str> = data["owner"]["possibleTypes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    possible.sort_unstable();
    assert_eq!(possible, ["Organization", "Person"]);

    // `knows: [Person!]` — a nullable list of non-null Person.
    let knows = data["person"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "knows")
        .unwrap();
    assert_eq!(knows["type"]["kind"], "LIST");
    assert_eq!(knows["type"]["ofType"]["kind"], "NON_NULL");
}

#[test]
fn cyclic_fragments_are_rejected_by_extraction() {
    let query = "query { persons { ...A } }
        fragment A on Person { id ...B }
        fragment B on Person { ...A }";
    let doc = async_graphql::parser::parse_query(query).unwrap();
    let err =
        selection::extract(&doc, None, &Variables::default(), &Limits::default()).unwrap_err();
    assert!(err.to_string().contains("cyclic"), "{err}");
}

#[test]
fn unknown_variable_is_reported() {
    let query = "query { persons(limit: $nope) { id } }";
    let doc = async_graphql::parser::parse_query(query).unwrap();
    let err =
        selection::extract(&doc, None, &Variables::default(), &Limits::default()).unwrap_err();
    assert!(err.to_string().contains("$nope"), "{err}");
}
