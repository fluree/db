//! GraphQL → JSON-LD query.
//!
//! These assert on the lowered **document**, not on results: the point is that a
//! GraphQL query becomes the JSON-LD query a user would have written by hand.

use async_graphql::Variables;
use fluree_db_core::ValueTypeTag;
use fluree_db_graphql::lower::shape::{FieldSource, RootShape};
use fluree_db_graphql::lower::{self, Lowered};
use fluree_db_graphql::naming::Namer;
use fluree_db_graphql::schema::inferred::{self, ClassObservation, PropertyObservation};
use fluree_db_graphql::schema::model::SchemaModel;
use fluree_db_graphql::selection;
use serde_json::{json, Value as Json};

const EX: &str = "http://example.org/";

fn prop(iri: &str, tags: &[ValueTypeTag]) -> PropertyObservation {
    PropertyObservation {
        iri: iri.to_string(),
        datatypes: tags.to_vec(),
        has_language_tags: false,
        ref_classes: Vec::new(),
    }
}

fn ref_prop(iri: &str, targets: &[&str]) -> PropertyObservation {
    PropertyObservation {
        iri: iri.to_string(),
        datatypes: vec![ValueTypeTag::JSON_LD_ID],
        has_language_tags: false,
        ref_classes: targets.iter().map(|t| (*t).to_string()).collect(),
    }
}

/// Person (name, age, knows→Person, owner→Person|Company) and Company (name).
fn model() -> SchemaModel {
    inferred::build(
        &[
            ClassObservation {
                iri: format!("{EX}Person"),
                count: 5,
                properties: vec![
                    prop(&format!("{EX}name"), &[ValueTypeTag::STRING]),
                    prop(&format!("{EX}age"), &[ValueTypeTag::INT]),
                    ref_prop(&format!("{EX}knows"), &[&format!("{EX}Person")]),
                    ref_prop(
                        &format!("{EX}affiliate"),
                        &[&format!("{EX}Person"), &format!("{EX}Company")],
                    ),
                ],
            },
            ClassObservation {
                iri: format!("{EX}Company"),
                count: 2,
                properties: vec![prop(&format!("{EX}name"), &[ValueTypeTag::STRING])],
            },
        ],
        &namer(),
    )
}

fn namer() -> Namer {
    Namer::new([("ex".to_string(), EX.to_string())], None)
}

fn lower_query(model: &SchemaModel, document: &str, root_field: &str) -> Lowered {
    lower_with_vars(model, document, root_field, Variables::default())
}

fn lower_with_vars(
    model: &SchemaModel,
    document: &str,
    root_field: &str,
    variables: Variables,
) -> Lowered {
    let doc = async_graphql::parser::parse_query(document).expect("document parses");
    let op = selection::extract(&doc, None, &variables).expect("selection extracts");
    let selection = op
        .selections
        .iter()
        .find(|s| s.name == root_field)
        .expect("root field selected");
    let root = model
        .query_fields
        .iter()
        .find(|r| r.name == root_field)
        .expect("root field exists");
    lower::lower(model, root, selection, &namer()).expect("lowers")
}

fn lower_err(model: &SchemaModel, document: &str, root_field: &str) -> String {
    let doc = async_graphql::parser::parse_query(document).expect("document parses");
    let op = selection::extract(&doc, None, &Variables::default()).expect("extracts");
    let selection = op
        .selections
        .iter()
        .find(|s| s.name == root_field)
        .expect("root field selected");
    let root = model
        .query_fields
        .iter()
        .find(|r| r.name == root_field)
        .expect("root field exists");
    lower::lower(model, root, selection, &namer())
        .expect_err("should not lower")
        .to_string()
}

/// The `where` clause, so tests can assert on it without restating the select.
fn where_of(q: &Json) -> &Json {
    &q["where"]
}

#[test]
fn a_list_root_becomes_a_typed_subject_expansion() {
    let m = model();
    let Lowered { query, shape } = lower_query(&m, "{ persons { id name age } }", "persons");

    assert_eq!(
        query,
        json!({
            "@context": {},
            "select": {
                "?_gql0": [
                    "@id",
                    format!("{EX}age"),
                    format!("{EX}name"),
                ]
            },
            "where": [ { "@id": "?_gql0", "@type": format!("{EX}Person") } ]
        })
    );

    // The shape carries the response keys the JSON-LD result does not.
    let keys: Vec<&str> = shape
        .node
        .common
        .iter()
        .map(|f| f.response_key.as_str())
        .collect();
    assert_eq!(keys, ["id", "name", "age"]);
    assert_eq!(shape.node.common[0].source, FieldSource::Id);
    assert_eq!(
        shape.node.common[1].source,
        FieldSource::Property(format!("{EX}name"))
    );
    // Tier 1 fields are lists.
    assert!(!shape.node.common[0].list);
    assert!(shape.node.common[1].list);
}

#[test]
fn aliases_live_in_the_shape_not_the_query() {
    let m = model();
    let Lowered { query, shape } = lower_query(&m, "{ persons { who: name } }", "persons");

    // The query asks for the predicate; nothing about the alias reaches it.
    assert_eq!(query["select"]["?_gql0"], json!([format!("{EX}name")]));
    assert_eq!(shape.node.common[0].response_key, "who");
    assert_eq!(
        shape.node.common[0].source,
        FieldSource::Property(format!("{EX}name"))
    );
}

#[test]
fn nested_objects_become_a_nested_select_spec() {
    let m = model();
    let Lowered { query, shape } = lower_query(
        &m,
        "{ persons { name knows { id name knows { id } } } }",
        "persons",
    );

    assert_eq!(
        query["select"]["?_gql0"],
        json!([
            {
                format!("{EX}knows"): [
                    "@id",
                    { format!("{EX}knows"): ["@id"] },
                    format!("{EX}name"),
                ]
            },
            format!("{EX}name"),
        ])
    );

    let knows = shape.node.common[1].child.as_ref().expect("nested shape");
    assert_eq!(knows.common[0].response_key, "id");
    assert!(
        knows.common[2].child.is_some(),
        "the second level nests too"
    );
}

#[test]
fn repeated_predicates_across_fragments_merge_into_one_request() {
    let m = model();
    let Lowered { query, shape } = lower_query(
        &m,
        "{ persons {
             affiliate {
               ... on Person { name }
               ... on Company { name }
             }
           } }",
        "persons",
    );

    // Both members read ex:name, so the query asks for it once — and for @type,
    // which is what lets the shape pick the right fragment per node.
    assert_eq!(
        query["select"]["?_gql0"],
        json!([{ format!("{EX}affiliate"): ["@type", format!("{EX}name")] }])
    );

    let affiliate = shape.node.common[0].child.as_ref().unwrap();
    assert!(affiliate.common.is_empty());
    assert!(affiliate.needs_type());
    let conditions: Vec<(&str, &str)> = affiliate
        .conditional
        .iter()
        .map(|c| (c.type_name.as_str(), c.class_iri.as_str()))
        .collect();
    assert_eq!(
        conditions,
        [
            ("Person", format!("{EX}Person").as_str()),
            ("Company", format!("{EX}Company").as_str()),
        ]
    );
}

#[test]
fn typename_requests_the_subject_type() {
    let m = model();
    let Lowered { query, shape } = lower_query(&m, "{ persons { __typename id } }", "persons");
    assert_eq!(query["select"]["?_gql0"], json!(["@id", "@type"]));
    assert_eq!(shape.node.common[0].source, FieldSource::Typename);
}

#[test]
fn scalar_filters_bind_the_field_and_constrain_the_binding() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { name: { RE: "^A" }, age: { GTE: 18, LT: 65 } }) { id } }"#,
        "persons",
    );

    // Entries lower in the order the document wrote them, so the JSON-LD query
    // reads like the GraphQL one.
    assert_eq!(
        where_of(&query),
        &json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            { "@id": "?_gql0", format!("{EX}name"): "?_gql1" },
            ["filter", "(regex ?_gql1 \"^A\")"],
            { "@id": "?_gql0", format!("{EX}age"): "?_gql2" },
            ["filter", "(>= ?_gql2 18)"],
            ["filter", "(< ?_gql2 65)"]
        ])
    );
}

#[test]
fn regex_case_insensitivity_and_negation() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { name: { IRE: "smith" } }) { id } }"#,
        "persons",
    );
    assert_eq!(
        where_of(&query)[2],
        json!(["filter", "(regex ?_gql1 \"(?i)smith\")"])
    );

    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { name: { NRE: "^tmp" } }) { id } }"#,
        "persons",
    );
    assert_eq!(
        where_of(&query)[2],
        json!(["filter", "(not (regex ?_gql1 \"^tmp\"))"])
    );
}

#[test]
fn in_and_not_in_use_the_bracketed_list_form() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { name: { IN: ["Alice", "Bo\"b"] }, age: { NIN: [1, 2] } }) { id } }"#,
        "persons",
    );
    let w = where_of(&query);
    assert_eq!(
        w[2],
        json!(["filter", "(in ?_gql1 [\"Alice\" \"Bo\\\"b\"])"])
    );
    assert_eq!(w[4], json!(["filter", "(not-in ?_gql2 [1 2])"]));
}

#[test]
fn iri_valued_filters_use_values_patterns() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { id: { EQ: "http://example.org/alice" } }) { id } }"#,
        "persons",
    );
    // `id` is the subject itself, so it binds no extra triple.
    assert_eq!(
        where_of(&query),
        &json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            ["values", ["?_gql0", [{ "@id": format!("{EX}alice") }]]]
        ])
    );

    // Operators the S-expression language cannot express on IRIs are refused
    // rather than silently mis-lowered.
    let err = lower_err(
        &m,
        r#"{ persons(where: { id: { NEQ: "http://example.org/alice" } }) { id } }"#,
        "persons",
    );
    assert!(err.contains("`NEQ` is not supported"), "{err}");
}

#[test]
fn nested_object_filters_join_through_the_reference() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { knows: { name: { EQ: "Bob" } } }) { id } }"#,
        "persons",
    );
    assert_eq!(
        where_of(&query),
        &json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            { "@id": "?_gql0", format!("{EX}knows"): "?_gql1" },
            { "@id": "?_gql1", format!("{EX}name"): "?_gql2" },
            ["filter", "(= ?_gql2 \"Bob\")"]
        ])
    );
}

#[test]
fn combinators_map_to_union_and_not_exists() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: {
              OR: [{ age: { LT: 18 } }, { age: { GT: 65 } }],
              NOT: { name: { EQ: "Anonymous" } }
            }) { id } }"#,
        "persons",
    );
    assert_eq!(
        where_of(&query),
        &json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            ["union",
              [
                { "@id": "?_gql0", format!("{EX}age"): "?_gql1" },
                ["filter", "(< ?_gql1 18)"]
              ],
              [
                { "@id": "?_gql0", format!("{EX}age"): "?_gql2" },
                ["filter", "(> ?_gql2 65)"]
              ]
            ],
            ["not-exists",
                { "@id": "?_gql0", format!("{EX}name"): "?_gql3" },
                ["filter", "(= ?_gql3 \"Anonymous\")"]
            ]
        ])
    );
}

#[test]
fn exists_false_negates_instead_of_binding() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        "{ persons(where: { age: { EXISTS: false } }) { id } }",
        "persons",
    );
    assert_eq!(
        where_of(&query)[1],
        json!(["not-exists", { "@id": "?_gql0", format!("{EX}age"): "?_gql1" }])
    );

    // `EXISTS: true` is just the binding.
    let Lowered { query, .. } = lower_query(
        &m,
        "{ persons(where: { age: { EXISTS: true } }) { id } }",
        "persons",
    );
    assert_eq!(
        where_of(&query),
        &json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            { "@id": "?_gql0", format!("{EX}age"): "?_gql1" }
        ])
    );

    let err = lower_err(
        &m,
        "{ persons(where: { age: { EXISTS: false, GT: 3 } }) { id } }",
        "persons",
    );
    assert!(err.contains("cannot be combined"), "{err}");

    // A compacted IRI is expanded, because the lowered query carries no context.
    let Lowered { query, .. } = lower_query(
        &m,
        r#"{ persons(where: { id: { EQ: "ex:alice" } }) { id } }"#,
        "persons",
    );
    assert_eq!(
        where_of(&query)[1],
        json!(["values", ["?_gql0", [{ "@id": format!("{EX}alice") }]]])
    );
}

#[test]
fn pagination_and_ordering() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        "{ persons(limit: 20, offset: 40, orderBy: { id: DESC }) { id } }",
        "persons",
    );
    assert_eq!(query["limit"], json!(20));
    assert_eq!(query["offset"], json!(40));
    assert_eq!(query["orderBy"], json!([["desc", "?_gql0"]]));

    // In an inferred schema every other field is multi-valued, so ordering by one
    // would multiply subjects rather than order them.
    let err = lower_err(&m, "{ persons(orderBy: { name: ASC }) { id } }", "persons");
    assert!(err.contains("only `id` is orderable"), "{err}");
}

#[test]
fn variables_reach_the_lowered_query() {
    let m = model();
    let Lowered { query, .. } = lower_with_vars(
        &m,
        "query Q($min: Int!, $n: Int = 3) {
           persons(where: { age: { GTE: $min } }, limit: $n) { id }
         }",
        "persons",
        Variables::from_json(json!({ "min": 21 })),
    );
    assert_eq!(where_of(&query)[2], json!(["filter", "(>= ?_gql1 21)"]));
    assert_eq!(query["limit"], json!(3));
}

#[test]
fn a_single_root_constrains_a_variable_rather_than_expanding_an_iri() {
    let m = model();
    let Lowered { query, shape } =
        lower_query(&m, r#"{ person(id: "ex:alice") { name } }"#, "person");
    // An IRI-constant expansion (`"select": {"<iri>": [...]}`) would return a bare
    // `{"@id": ...}` stub for a subject that does not exist, and would not check
    // that the subject is a Person. Constraining a typed variable yields no row in
    // either case, which is the `null` GraphQL expects.
    assert_eq!(
        query,
        json!({
            "@context": {},
            "select": { "?_gql0": [format!("{EX}name")] },
            "where": [
                { "@id": "?_gql0", "@type": format!("{EX}Person") },
                ["values", ["?_gql0", [{ "@id": format!("{EX}alice") }]]]
            ],
            "limit": 1
        })
    );
    assert_eq!(shape.node.common.len(), 1);
}

#[test]
fn a_count_root_projects_a_distinct_count() {
    let m = model();
    let Lowered { query, shape } = lower_query(
        &m,
        r#"{ persons_count(where: { name: { EQ: "Alice" } }) }"#,
        "persons_count",
    );
    assert_eq!(
        query,
        json!({
            "@context": {},
            "select": ["(as (count-distinct ?_gql0) ?count)"],
            "where": [
                { "@id": "?_gql0", "@type": format!("{EX}Person") },
                { "@id": "?_gql0", format!("{EX}name"): "?_gql1" },
                ["filter", "(= ?_gql1 \"Alice\")"]
            ]
        })
    );
    assert_eq!(
        shape,
        RootShape {
            kind: shape.kind,
            node: Default::default()
        }
    );
}

#[test]
fn nested_field_arguments_become_a_modified_selection() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        "{ persons { knows(limit: 5, offset: 2, orderBy: { name: DESC }) { id name } } }",
        "persons",
    );
    assert_eq!(
        query["select"]["?_gql0"],
        json!([{
            format!("{EX}knows"): {
                "limit": 5,
                "offset": 2,
                "orderBy": [["desc", format!("{EX}name")]],
                "select": ["@id", format!("{EX}name")]
            }
        }])
    );
}

#[test]
fn ordering_a_nested_field_by_id_uses_the_subject_iri() {
    let m = model();
    let Lowered { query, .. } = lower_query(
        &m,
        "{ persons { knows(orderBy: { id: ASC }) { id } } }",
        "persons",
    );
    assert_eq!(
        query["select"]["?_gql0"],
        json!([{ format!("{EX}knows"): { "orderBy": ["@id"], "select": ["@id"] } }])
    );
}

#[test]
fn a_nested_where_is_refused_with_a_reason() {
    let m = model();
    // Filtering a nested level means evaluating a predicate over already
    // materialized values — a different engine from the one answering WHERE.
    let err = lower_err(
        &m,
        r#"{ persons { knows(where: { name: { EQ: "Bob" } }) { id } } }"#,
        "persons",
    );
    assert!(
        err.contains("`where` on the nested field `Person.knows`"),
        "{err}"
    );
    assert!(err.contains("traverse"), "{err}");
}

#[test]
fn a_reverse_field_gets_a_generated_context_term() {
    // Without a term, a reverse selection comes back under the plain predicate
    // IRI — the same key a forward selection of that predicate uses. Selecting
    // both directions of one edge would then collide.
    let mut m = model();
    let person = m.objects.iter_mut().find(|o| o.name == "Person").unwrap();
    person.fields.push(fluree_db_graphql::schema::model::Field {
        name: "knownBy".to_string(),
        iri: format!("{EX}knows"),
        direction: fluree_db_graphql::schema::model::Direction::Reverse,
        ty: fluree_db_graphql::schema::model::FieldType::Object("Person".to_string()),
        list: true,
        non_null: false,
        description: None,
        language_tagged: false,
        provenance: fluree_db_graphql::schema::model::Provenance::Shaped,
    });

    let Lowered { query, shape } =
        lower_query(&m, "{ persons { knows { id } knownBy { id } } }", "persons");

    assert_eq!(
        query["@context"],
        json!({ "_rev0": { "@reverse": format!("{EX}knows") } })
    );
    assert_eq!(
        query["select"]["?_gql0"],
        json!([
            { "_rev0": ["@id"] },
            { format!("{EX}knows"): ["@id"] },
        ])
    );

    // The shape reads each direction from its own key.
    let sources: Vec<&FieldSource> = shape.node.common.iter().map(|f| &f.source).collect();
    assert_eq!(
        sources,
        [
            &FieldSource::Property(format!("{EX}knows")),
            &FieldSource::Property("_rev0".to_string()),
        ]
    );
}
