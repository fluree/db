//! Tier 3: a `graphql:Schema` decides what is published.

use std::collections::BTreeMap;

use fluree_db_core::ValueTypeTag;
use fluree_db_graphql::naming::Namer;
use fluree_db_graphql::schema::build;
use fluree_db_graphql::schema::curated::{CuratedSchema, Exposure};
use fluree_db_graphql::schema::inferred::{ClassObservation, PropertyObservation};
use fluree_db_graphql::schema::model::{FieldType, SchemaModel};
use fluree_db_graphql::schema::shaped::{ShapeDescription, ShapedProperty};

const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn namer() -> Namer {
    Namer::new([("ex".to_string(), EX.to_string())], None)
}

fn prop(iri: &str, tags: &[ValueTypeTag]) -> PropertyObservation {
    PropertyObservation {
        iri: format!("{EX}{iri}"),
        datatypes: tags.to_vec(),
        has_language_tags: false,
        ref_classes: Vec::new(),
    }
}

fn ref_prop(iri: &str, targets: &[&str]) -> PropertyObservation {
    PropertyObservation {
        iri: format!("{EX}{iri}"),
        datatypes: vec![ValueTypeTag::JSON_LD_ID],
        has_language_tags: false,
        ref_classes: targets.iter().map(|t| format!("{EX}{t}")).collect(),
    }
}

fn observed(iri: &str, properties: Vec<PropertyObservation>) -> ClassObservation {
    ClassObservation {
        iri: format!("{EX}{iri}"),
        count: 1,
        properties,
    }
}

fn shape(iri: &str, properties: Vec<ShapedProperty>) -> ShapeDescription {
    ShapeDescription {
        class_iri: format!("{EX}{iri}"),
        name: None,
        description: None,
        closed: false,
        properties,
    }
}

/// `interface class → the classes beneath it`.
fn members(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(iface, subs)| {
            (
                format!("{EX}{iface}"),
                subs.iter().map(|s| format!("{EX}{s}")).collect(),
            )
        })
        .collect()
}

fn string_prop(iri: &str) -> ShapedProperty {
    ShapedProperty {
        iri: format!("{EX}{iri}"),
        datatype: Some(format!("{XSD}string")),
        max_count: Some(1),
        ..Default::default()
    }
}

fn curated(entries: &[(&str, Exposure)]) -> CuratedSchema {
    CuratedSchema {
        exposure: entries
            .iter()
            .map(|(iri, e)| (format!("{EX}{iri}"), *e))
            .collect(),
        ..Default::default()
    }
}

fn type_names(model: &SchemaModel) -> Vec<&str> {
    model.objects.iter().map(|o| o.name.as_str()).collect()
}

fn root_names(model: &SchemaModel) -> Vec<&str> {
    model.query_fields.iter().map(|r| r.name.as_str()).collect()
}

fn field_ty(model: &SchemaModel, type_name: &str, field: &str) -> FieldType {
    model
        .fields_of(type_name)
        .unwrap_or_else(|| panic!("no type `{type_name}`"))
        .iter()
        .find(|f| f.name == field)
        .unwrap_or_else(|| panic!("no field `{type_name}.{field}`"))
        .ty
        .clone()
}

fn dataset() -> Vec<ClassObservation> {
    vec![
        observed(
            "Person",
            vec![
                prop("name", &[ValueTypeTag::STRING]),
                ref_prop("employer", &["Company"]),
                ref_prop("audit", &["AuditRecord"]),
            ],
        ),
        observed("Company", vec![prop("name", &[ValueTypeTag::STRING])]),
        observed("AuditRecord", vec![prop("note", &[ValueTypeTag::STRING])]),
    ]
}

#[test]
fn only_listed_classes_are_published() {
    // Tier 3's whole point: an API contract that does not grow a type the
    // moment someone writes an instance.
    let model = build::build_curated(
        &dataset(),
        &[],
        Some(&curated(&[("Person", Exposure::Public)])),
        &namer(),
    );
    assert_eq!(type_names(&model), ["Node", "Person"]);
    assert_eq!(root_names(&model), ["person", "persons", "persons_count"]);
}

#[test]
fn the_three_exposure_levels_differ_where_it_matters() {
    let model = build::build_curated(
        &dataset(),
        &[],
        Some(&curated(&[
            ("Person", Exposure::Public),
            ("Company", Exposure::Protected),
            ("AuditRecord", Exposure::Private),
        ])),
        &namer(),
    );

    // Public and protected are both types; private is not.
    assert_eq!(type_names(&model), ["Company", "Node", "Person"]);

    // Only public gets root fields — a protected class is readable through a
    // reference but cannot be enumerated.
    assert_eq!(root_names(&model), ["person", "persons", "persons_count"]);
    assert_eq!(
        field_ty(&model, "Person", "employer"),
        FieldType::Object("Company".to_string())
    );

    // A reference to a private class degrades to `Node`: the edge stays
    // visible as an IRI without naming a type the caller cannot query.
    assert_eq!(
        field_ty(&model, "Person", "audit"),
        FieldType::Object("Node".to_string())
    );
    assert_eq!(
        model.fields_of("Node").unwrap().len(),
        1,
        "Node exposes only `id`"
    );

    fluree_db_graphql::sdl(&model).expect("sdl renders");
}

#[test]
fn an_unlisted_class_is_absent_even_with_a_shape() {
    let model = build::build_curated(
        &dataset(),
        &[shape("Company", vec![string_prop("name")])],
        Some(&curated(&[("Person", Exposure::Public)])),
        &namer(),
    );
    assert!(
        model.object("Company").is_none(),
        "{:?}",
        type_names(&model)
    );
}

#[test]
fn a_curated_class_needs_neither_instances_nor_a_shape() {
    // Listing a class is itself a declaration of intent to publish it.
    let model = build::build_curated(
        &[],
        &[],
        Some(&curated(&[("Widget", Exposure::Public)])),
        &namer(),
    );
    assert!(model.object("Widget").is_some());
    assert_eq!(
        model.fields_of("Widget").unwrap().len(),
        1,
        "nothing is known about it but its identity"
    );
}

#[test]
fn graphql_name_overrides_the_shape_and_the_iri() {
    let mut c = curated(&[("Person", Exposure::Public)]);
    c.type_names
        .insert(format!("{EX}Person"), "Human".to_string());
    c.plural_names
        .insert(format!("{EX}Person"), "people".to_string());

    let model = build::build_curated(
        &dataset(),
        &[ShapeDescription {
            // `graphql:name` wins over `sh:name`: it is the GraphQL-specific
            // declaration, and this is the GraphQL schema.
            name: Some("Persona".to_string()),
            ..shape("Person", vec![string_prop("name")])
        }],
        Some(&c),
        &namer(),
    );

    assert!(model.object("Human").is_some(), "{:?}", type_names(&model));
    assert_eq!(root_names(&model), ["human", "people", "people_count"]);
}

#[test]
fn an_abstract_class_becomes_an_interface_its_subclasses_implement() {
    let model = build::build_curated(
        &[
            observed("Person", vec![prop("name", &[ValueTypeTag::STRING])]),
            observed("Company", vec![prop("name", &[ValueTypeTag::STRING])]),
            observed("Document", vec![ref_prop("owner", &["Agent"])]),
        ],
        &[
            shape("Agent", vec![string_prop("name")]),
            shape("Person", vec![string_prop("name")]),
            shape("Company", vec![string_prop("name")]),
            shape("Document", vec![]),
        ],
        Some(&CuratedSchema {
            interfaces: vec![format!("{EX}Agent")],
            interface_members: members(&[("Agent", &["Person", "Company"])]),
            ..curated(&[
                ("Agent", Exposure::Public),
                ("Person", Exposure::Public),
                ("Company", Exposure::Public),
                ("Document", Exposure::Public),
            ])
        }),
        &namer(),
    );

    // The abstract class is an interface, not an object: nothing is an instance
    // of it directly, so a concrete type would only ever be empty.
    assert!(model.interface("Agent").is_some());
    assert!(model.object("Agent").is_none());
    // No reference degrades here, so the `Node` placeholder is not emitted.
    assert_eq!(type_names(&model), ["Company", "Document", "Person"]);

    let person = model.object("Person").unwrap();
    assert_eq!(person.implements, ["Agent"]);
    assert_eq!(model.object("Company").unwrap().implements, ["Agent"]);
    assert!(model.object("Document").unwrap().implements.is_empty());

    // A reference to the abstract class is typed as the interface.
    assert_eq!(
        field_ty(&model, "Document", "owner"),
        FieldType::Interface("Agent".to_string())
    );

    // Both concrete types are reachable through it.
    let possible: Vec<&str> = model
        .possible_types("Agent")
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(possible, ["Company", "Person"]);

    // An interface is queryable in its own right.
    assert!(model.query_fields.iter().any(|r| r.name == "agents"));

    let sdl = fluree_db_graphql::sdl(&model).expect("sdl renders");
    assert!(sdl.contains("interface Agent"), "{sdl}");
    assert!(sdl.contains("type Person implements Agent"), "{sdl}");
    assert!(sdl.contains("owner: Agent"), "{sdl}");
}

#[test]
fn a_subclass_missing_an_interface_field_does_not_claim_it() {
    // GraphQL requires an implementor to declare every one of the interface's
    // fields. Claiming the interface anyway would produce a schema that will
    // not register at all, which is a worse outcome than one missing edge.
    let model = build::build_curated(
        &[],
        &[
            shape("Agent", vec![string_prop("name")]),
            // Beneath the interface, but does not declare `name`.
            shape("Ghost", vec![]),
        ],
        Some(&CuratedSchema {
            interfaces: vec![format!("{EX}Agent")],
            interface_members: members(&[("Agent", &["Ghost"])]),
            ..curated(&[("Agent", Exposure::Public), ("Ghost", Exposure::Public)])
        }),
        &namer(),
    );
    assert!(model.object("Ghost").unwrap().implements.is_empty());
    fluree_db_graphql::sdl(&model).expect("sdl still registers");
}

#[test]
fn without_a_curated_schema_nothing_changes() {
    let plain = build::build(&dataset(), &[], &namer());
    let explicit_none = build::build_curated(&dataset(), &[], None, &namer());
    assert_eq!(type_names(&plain), type_names(&explicit_none));
    assert_eq!(root_names(&plain), root_names(&explicit_none));
    // Every class the data holds, none of them interfaces.
    assert_eq!(type_names(&plain), ["AuditRecord", "Company", "Person"]);
    assert!(plain.interfaces.is_empty());
}
