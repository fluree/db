//! Tier 1: statistics → schema.

use fluree_db_core::ValueTypeTag;
use fluree_db_graphql::naming::Namer;
use fluree_db_graphql::schema::inferred::{self, ClassObservation, PropertyObservation};
use fluree_db_graphql::schema::model::{FieldType, Scalar, SchemaModel};

const EX: &str = "http://example.org/";
const FOAF: &str = "http://xmlns.com/foaf/0.1/";

fn namer() -> Namer {
    Namer::new(
        [
            ("ex".to_string(), EX.to_string()),
            ("foaf".to_string(), FOAF.to_string()),
        ],
        None,
    )
}

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

fn class(iri: &str, count: u64, properties: Vec<PropertyObservation>) -> ClassObservation {
    ClassObservation {
        iri: iri.to_string(),
        count,
        properties,
    }
}

fn field_type(model: &SchemaModel, type_name: &str, field: &str) -> FieldType {
    model
        .fields_of(type_name)
        .unwrap_or_else(|| panic!("no type `{type_name}`"))
        .iter()
        .find(|f| f.name == field)
        .unwrap_or_else(|| panic!("no field `{type_name}.{field}`"))
        .ty
        .clone()
}

#[test]
fn scalars_and_single_target_references() {
    let model = inferred::build(
        &[
            class(
                &format!("{EX}Person"),
                3,
                vec![
                    prop(&format!("{FOAF}name"), &[ValueTypeTag::STRING]),
                    prop(&format!("{EX}age"), &[ValueTypeTag::INT]),
                    prop(&format!("{EX}netWorth"), &[ValueTypeTag::DECIMAL]),
                    prop(&format!("{EX}born"), &[ValueTypeTag::DATE]),
                    ref_prop(&format!("{EX}employer"), &[&format!("{EX}Company")]),
                    // rdf:type is `__typename`, not a field.
                    ref_prop(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        &[&format!("{EX}Person")],
                    ),
                ],
            ),
            class(
                &format!("{EX}Company"),
                2,
                vec![prop(&format!("{FOAF}name"), &[ValueTypeTag::STRING])],
            ),
        ],
        &namer(),
    );

    assert!(model.warnings.is_empty(), "{:?}", model.warnings);
    assert_eq!(
        field_type(&model, "Person", "name"),
        FieldType::Scalar(Scalar::String)
    );
    assert_eq!(
        field_type(&model, "Person", "age"),
        FieldType::Scalar(Scalar::Int)
    );
    assert_eq!(
        field_type(&model, "Person", "netWorth"),
        FieldType::Scalar(Scalar::Decimal)
    );
    assert_eq!(
        field_type(&model, "Person", "born"),
        FieldType::Scalar(Scalar::Date)
    );
    assert_eq!(
        field_type(&model, "Person", "employer"),
        FieldType::Object("Company".to_string())
    );
    assert!(
        model
            .fields_of("Person")
            .unwrap()
            .iter()
            .all(|f| f.name != "type"),
        "rdf:type should not become a field"
    );

    // Tier 1 claims no cardinality: every non-`id` field is a nullable list.
    for f in model.fields_of("Person").unwrap() {
        if f.is_id() {
            assert!(!f.list && f.non_null, "id must stay `ID!`");
        } else {
            assert!(
                f.list && !f.non_null,
                "{} should be a nullable list",
                f.name
            );
        }
    }

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(sdl.contains("name: [String!]"), "{sdl}");
    // Object-valued list fields carry the per-level shaping arguments, with the
    // nested order input (which accepts multi-valued keys, unlike the root one).
    assert!(
        sdl.contains(
            "employer(where: CompanyFilter, limit: Int, offset: Int, \
             orderBy: CompanyNestedOrder): [Company!]"
        ),
        "{sdl}"
    );
    // Only the custom scalars actually referenced get registered.
    assert!(sdl.contains("scalar Decimal"), "{sdl}");
    assert!(sdl.contains("scalar Date"), "{sdl}");
    assert!(!sdl.contains("scalar Time"), "{sdl}");
    assert!(!sdl.contains("scalar JSON"), "{sdl}");
    // Nothing referenced an unexposed type, so `Node` is not emitted.
    assert!(!sdl.contains("type Node"), "{sdl}");
}

#[test]
fn root_fields_are_named_and_pluralised() {
    let model = inferred::build(
        &[
            class(&format!("{EX}Person"), 1, vec![]),
            class(&format!("{EX}Company"), 1, vec![]),
        ],
        &namer(),
    );

    let names: Vec<&str> = model.query_fields.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "companies",
            "companies_count",
            "company",
            "person",
            "persons",
            "persons_count",
        ]
    );
}

#[test]
fn several_target_classes_become_one_shared_union() {
    let model = inferred::build(
        &[
            class(
                &format!("{EX}Document"),
                1,
                vec![
                    ref_prop(
                        &format!("{EX}owner"),
                        &[&format!("{EX}Person"), &format!("{EX}Company")],
                    ),
                    ref_prop(
                        &format!("{EX}author"),
                        &[&format!("{EX}Company"), &format!("{EX}Person")],
                    ),
                ],
            ),
            class(&format!("{EX}Person"), 1, vec![]),
            class(&format!("{EX}Company"), 1, vec![]),
        ],
        &namer(),
    );

    let expected = FieldType::Union("CompanyOrPerson".to_string());
    assert_eq!(field_type(&model, "Document", "owner"), expected);
    // Same target set, same union — not two structurally identical types.
    assert_eq!(field_type(&model, "Document", "author"), expected);
    assert_eq!(model.unions.len(), 1);
    assert_eq!(model.unions[0].members, ["Company", "Person"]);

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(
        sdl.contains("union CompanyOrPerson = Company | Person"),
        "{sdl}"
    );
}

#[test]
fn references_to_unexposed_classes_fall_back_to_node() {
    let model = inferred::build(
        &[
            class(
                &format!("{EX}Document"),
                1,
                vec![
                    // Target has no instances, so it is not a type in this schema.
                    ref_prop(&format!("{EX}draftOf"), &[&format!("{EX}Ghost")]),
                    // No target class recorded at all.
                    ref_prop(&format!("{EX}related"), &[]),
                ],
            ),
            class(&format!("{EX}Ghost"), 0, vec![]),
        ],
        &namer(),
    );

    let node = FieldType::Object("Node".to_string());
    assert_eq!(field_type(&model, "Document", "draftOf"), node);
    assert_eq!(field_type(&model, "Document", "related"), node);

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(sdl.contains("type Node"), "{sdl}");
    // A class with no instances is not a type, and gets no root fields.
    assert!(!sdl.contains("type Ghost"), "{sdl}");
    assert!(model.query_fields.iter().all(|r| r.type_name != "Ghost"));
    // `Node` is a placeholder, so it is not enumerable either.
    assert!(model.query_fields.iter().all(|r| r.type_name != "Node"));
}

#[test]
fn mixed_datatypes_degrade_and_are_reported() {
    let model = inferred::build(
        &[class(
            &format!("{EX}Thing"),
            1,
            vec![
                // One number line: widened, not degraded.
                prop(
                    &format!("{EX}size"),
                    &[ValueTypeTag::INT, ValueTypeTag::LONG],
                ),
                // Unrelated kinds: no common GraphQL type.
                prop(
                    &format!("{EX}value"),
                    &[ValueTypeTag::INT, ValueTypeTag::BOOLEAN],
                ),
                // Both a reference and a literal.
                PropertyObservation {
                    iri: format!("{EX}about"),
                    datatypes: vec![ValueTypeTag::JSON_LD_ID, ValueTypeTag::STRING],
                    has_language_tags: false,
                    ref_classes: vec![format!("{EX}Thing")],
                },
            ],
        )],
        &namer(),
    );

    assert_eq!(
        field_type(&model, "Thing", "size"),
        FieldType::Scalar(Scalar::Long)
    );
    assert_eq!(
        field_type(&model, "Thing", "value"),
        FieldType::Scalar(Scalar::String)
    );
    assert_eq!(
        field_type(&model, "Thing", "about"),
        FieldType::Scalar(Scalar::String)
    );

    // Only the lossy reference/literal mix is worth telling the user about; a
    // widened or stringified literal is still a literal.
    assert_eq!(model.warnings.len(), 1, "{:?}", model.warnings);
    assert!(
        model.warnings[0].contains("Thing.about"),
        "{:?}",
        model.warnings
    );
    assert!(
        model.warnings[0].contains("references and literals"),
        "{:?}",
        model.warnings
    );
}

#[test]
fn colliding_local_names_are_qualified_by_prefix() {
    let model = inferred::build(
        &[
            class(
                &format!("{EX}Person"),
                1,
                vec![
                    prop(&format!("{EX}name"), &[ValueTypeTag::STRING]),
                    prop(&format!("{FOAF}name"), &[ValueTypeTag::STRING]),
                    // A property literally called `id` cannot shadow the subject IRI.
                    prop(&format!("{EX}id"), &[ValueTypeTag::STRING]),
                ],
            ),
            // Two classes with the same local name in different namespaces.
            class(&format!("{FOAF}Person"), 1, vec![]),
        ],
        &namer(),
    );

    let person_fields: Vec<&str> = model
        .fields_of("Person")
        .unwrap()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    // Sorted-IRI assignment order: ex:id, ex:name, then foaf:name. The synthetic
    // `id` holds its name; the property qualifies with its prefix instead.
    assert_eq!(person_fields, ["id", "ex_id", "name", "foaf_name"]);

    let type_names: Vec<&str> = model.objects.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(type_names, ["Person", "foaf_Person"]);

    // The schema still registers: no duplicate names reached async-graphql.
    fluree_db_graphql::sdl(&model).expect("sdl renders");
}

#[test]
fn an_empty_ledger_yields_an_empty_but_valid_schema() {
    let model = inferred::build(&[], &namer());
    assert!(model.objects.is_empty());
    assert!(model.query_fields.is_empty());
    // A `Query` type with no fields is not a legal GraphQL schema; callers must
    // handle this case rather than serving it.
    assert!(fluree_db_graphql::sdl(&model).is_err());
}
