//! Tier 2: SHACL shapes over the inferred schema.

use fluree_db_core::ValueTypeTag;
use fluree_db_graphql::naming::Namer;
use fluree_db_graphql::schema::build;
use fluree_db_graphql::schema::inferred::{ClassObservation, PropertyObservation};
use fluree_db_graphql::schema::model::{Direction, Field, FieldType, Scalar, SchemaModel};
use fluree_db_graphql::schema::shaped::{AllowedValue, ShapeDescription, ShapedProperty};

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

fn observed(iri: &str, count: u64, properties: Vec<PropertyObservation>) -> ClassObservation {
    ClassObservation {
        iri: format!("{EX}{iri}"),
        count,
        properties,
    }
}

fn shaped(iri: &str, properties: Vec<ShapedProperty>) -> ShapeDescription {
    ShapeDescription {
        class_iri: format!("{EX}{iri}"),
        name: None,
        description: None,
        closed: false,
        properties,
    }
}

fn shaped_prop(iri: &str) -> ShapedProperty {
    ShapedProperty {
        iri: format!("{EX}{iri}"),
        ..Default::default()
    }
}

fn field<'a>(model: &'a SchemaModel, type_name: &str, name: &str) -> &'a Field {
    model
        .fields_of(type_name)
        .unwrap_or_else(|| panic!("no type `{type_name}`"))
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field `{type_name}.{name}`"))
}

fn field_names(model: &SchemaModel, type_name: &str) -> Vec<String> {
    model
        .fields_of(type_name)
        .unwrap()
        .iter()
        .map(|f| f.name.clone())
        .collect()
}

#[test]
fn cardinality_comes_from_the_shape_not_the_data() {
    let model = build::build(
        &[observed(
            "Person",
            3,
            vec![
                prop("name", &[ValueTypeTag::STRING]),
                prop("nickname", &[ValueTypeTag::STRING]),
            ],
        )],
        &[shaped(
            "Person",
            vec![
                ShapedProperty {
                    datatype: Some(format!("{XSD}string")),
                    min_count: Some(1),
                    max_count: Some(1),
                    ..shaped_prop("name")
                },
                ShapedProperty {
                    datatype: Some(format!("{XSD}string")),
                    max_count: Some(1),
                    ..shaped_prop("nickname")
                },
            ],
        )],
        &namer(),
    );

    // `sh:minCount 1` + `sh:maxCount 1` is the only thing that can produce a
    // non-null single value; statistics could never justify it.
    let name = field(&model, "Person", "name");
    assert_eq!(name.ty, FieldType::Scalar(Scalar::String));
    assert!(!name.list && name.non_null, "{name:?}");

    let nickname = field(&model, "Person", "nickname");
    assert!(!nickname.list && !nickname.non_null, "{nickname:?}");

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(sdl.contains("name: String!"), "{sdl}");
    assert!(sdl.contains("nickname: String"), "{sdl}");
}

#[test]
fn shaped_and_observed_properties_coexist_unless_the_shape_is_closed() {
    let observation = observed(
        "Person",
        3,
        vec![
            prop("name", &[ValueTypeTag::STRING]),
            prop("secret", &[ValueTypeTag::STRING]),
        ],
    );
    let shape_props = vec![ShapedProperty {
        datatype: Some(format!("{XSD}string")),
        max_count: Some(1),
        ..shaped_prop("name")
    }];

    // Open: the undeclared property survives as an inferred field.
    let open = build::build(
        std::slice::from_ref(&observation),
        &[shaped("Person", shape_props.clone())],
        &namer(),
    );
    assert_eq!(field_names(&open, "Person"), ["id", "name", "secret"]);
    assert!(!field(&open, "Person", "name").list);
    assert!(
        field(&open, "Person", "secret").list,
        "inferred fields stay lists"
    );

    // Closed: it does not.
    let closed_shape = ShapeDescription {
        closed: true,
        ..shaped("Person", shape_props)
    };
    let closed = build::build(&[observation], &[closed_shape], &namer());
    assert_eq!(field_names(&closed, "Person"), ["id", "name"]);
}

#[test]
fn sh_order_and_names_shape_the_rendered_type() {
    let model = build::build(
        &[],
        &[ShapeDescription {
            name: Some("Human".to_string()),
            description: Some("Someone.".to_string()),
            properties: vec![
                ShapedProperty {
                    order: Some(2.0),
                    datatype: Some(format!("{XSD}string")),
                    description: Some("What they go by.".to_string()),
                    ..shaped_prop("name")
                },
                ShapedProperty {
                    order: Some(1.0),
                    name: Some("yearOfBirth".to_string()),
                    datatype: Some(format!("{XSD}integer")),
                    ..shaped_prop("born")
                },
                // No `sh:order`: sorts after everything ordered.
                ShapedProperty {
                    datatype: Some(format!("{XSD}string")),
                    ..shaped_prop("note")
                },
            ],
            ..shaped("Person", vec![])
        }],
        &namer(),
    );

    // `sh:name` renames both the type and the field.
    assert!(model.object("Human").is_some(), "{:?}", model.objects);
    assert_eq!(
        field_names(&model, "Human"),
        ["id", "yearOfBirth", "name", "note"]
    );

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(sdl.contains("Someone."), "{sdl}");
    assert!(sdl.contains("What they go by."), "{sdl}");
    // Root fields follow the renamed type.
    assert!(sdl.contains("humans(where: HumanFilter"), "{sdl}");
}

#[test]
fn a_shaped_class_appears_before_its_first_instance() {
    // A shape describes what a ledger is meant to hold. Waiting for the first
    // instance would mean a client cannot see the schema it is writing against.
    let model = build::build(
        &[],
        &[shaped(
            "Widget",
            vec![ShapedProperty {
                datatype: Some(format!("{XSD}string")),
                ..shaped_prop("label")
            }],
        )],
        &namer(),
    );
    assert!(model.object("Widget").is_some());
    assert!(model.query_fields.iter().any(|r| r.name == "widgets"));
}

#[test]
fn sh_class_types_a_reference_and_falls_back_to_node() {
    let model = build::build(
        &[],
        &[
            shaped(
                "Person",
                vec![
                    ShapedProperty {
                        class: Some(format!("{EX}Company")),
                        max_count: Some(1),
                        ..shaped_prop("employer")
                    },
                    // A class no shape describes and no instance has.
                    ShapedProperty {
                        class: Some(format!("{EX}Ghost")),
                        ..shaped_prop("haunts")
                    },
                    // `sh:nodeKind sh:IRI` with no class.
                    ShapedProperty {
                        node_kind_is_iri: true,
                        ..shaped_prop("seeAlso")
                    },
                ],
            ),
            shaped("Company", vec![]),
        ],
        &namer(),
    );

    assert_eq!(
        field(&model, "Person", "employer").ty,
        FieldType::Object("Company".to_string())
    );
    assert_eq!(
        field(&model, "Person", "haunts").ty,
        FieldType::Object("Node".to_string())
    );
    assert_eq!(
        field(&model, "Person", "seeAlso").ty,
        FieldType::Object("Node".to_string())
    );
    assert!(model.object("Node").is_some());
}

#[test]
fn sh_in_becomes_an_enum() {
    let model = build::build(
        &[],
        &[shaped(
            "Task",
            vec![
                ShapedProperty {
                    max_count: Some(1),
                    allowed_values: vec![
                        AllowedValue::Iri(format!("{EX}Open")),
                        AllowedValue::Iri(format!("{EX}Closed")),
                    ],
                    ..shaped_prop("status")
                },
                ShapedProperty {
                    max_count: Some(1),
                    allowed_values: vec![
                        AllowedValue::String("low".to_string()),
                        AllowedValue::String("high".to_string()),
                    ],
                    ..shaped_prop("priority")
                },
            ],
        )],
        &namer(),
    );

    assert_eq!(
        field(&model, "Task", "status").ty,
        FieldType::Enum("TaskStatusEnum".to_string())
    );
    let status = model
        .enums
        .iter()
        .find(|e| e.name == "TaskStatusEnum")
        .unwrap();
    // The name is the IRI's local part; the underlying value is the whole IRI,
    // because that is what has to go back into a query.
    assert_eq!(
        status.values,
        [
            ("Open".to_string(), format!("{EX}Open")),
            ("Closed".to_string(), format!("{EX}Closed")),
        ]
    );

    let priority = model
        .enums
        .iter()
        .find(|e| e.name == "TaskPriorityEnum")
        .unwrap();
    assert_eq!(
        priority.values,
        [
            ("low".to_string(), "low".to_string()),
            ("high".to_string(), "high".to_string()),
        ]
    );

    let sdl = fluree_db_graphql::sdl(&model).unwrap();
    assert!(sdl.contains("enum TaskStatusEnum"), "{sdl}");
    // An enum-typed field filters through its own enum input.
    assert!(sdl.contains("status: TaskStatusEnumFilter"), "{sdl}");
}

#[test]
fn an_unusable_sh_in_keeps_the_datatype_and_warns() {
    let model = build::build(
        &[],
        &[shaped(
            "Task",
            vec![ShapedProperty {
                datatype: Some(format!("{XSD}string")),
                allowed_values: vec![
                    AllowedValue::String("ok".to_string()),
                    // Sanitizes to nothing usable.
                    AllowedValue::String("///".to_string()),
                ],
                ..shaped_prop("status")
            }],
        )],
        &namer(),
    );
    assert_eq!(
        field(&model, "Task", "status").ty,
        FieldType::Scalar(Scalar::String)
    );
    assert_eq!(model.warnings.len(), 1, "{:?}", model.warnings);
    assert!(
        model.warnings[0].contains("not a usable enum name"),
        "{:?}",
        model.warnings
    );
}

#[test]
fn sh_in_members_that_collide_are_refused() {
    let model = build::build(
        &[],
        &[shaped(
            "Task",
            vec![ShapedProperty {
                datatype: Some(format!("{XSD}string")),
                allowed_values: vec![
                    // Both sanitize to `a_b`, which would make the enum
                    // ambiguous in both directions.
                    AllowedValue::String("a-b".to_string()),
                    AllowedValue::String("a.b".to_string()),
                ],
                ..shaped_prop("status")
            }],
        )],
        &namer(),
    );
    assert_eq!(
        field(&model, "Task", "status").ty,
        FieldType::Scalar(Scalar::String)
    );
    assert!(
        model.warnings[0].contains("collide"),
        "{:?}",
        model.warnings
    );
}

#[test]
fn an_inverse_path_becomes_a_reverse_field() {
    let model = build::build(
        &[],
        &[shaped(
            "Company",
            vec![ShapedProperty {
                direction: Direction::Reverse,
                class: Some(format!("{EX}Person")),
                name: Some("staff".to_string()),
                ..shaped_prop("employer")
            }],
        )],
        &namer(),
    );
    let staff = field(&model, "Company", "staff");
    assert_eq!(staff.direction, Direction::Reverse);
    assert_eq!(staff.iri, format!("{EX}employer"));
}

#[test]
fn a_forward_and_reverse_field_on_one_predicate_get_distinct_names() {
    // Both read `ex:employer`, in opposite directions, so they cannot share a
    // name derived from the IRI.
    let model = build::build(
        &[],
        &[shaped(
            "Person",
            vec![
                ShapedProperty {
                    class: Some(format!("{EX}Person")),
                    ..shaped_prop("employer")
                },
                ShapedProperty {
                    direction: Direction::Reverse,
                    class: Some(format!("{EX}Person")),
                    ..shaped_prop("employer")
                },
            ],
        )],
        &namer(),
    );
    // The derived fallback reads as a direction, not a counter.
    let names = field_names(&model, "Person");
    assert_eq!(names, ["id", "employer", "employer_reverse"]);
    assert_eq!(
        field(&model, "Person", "employer").direction,
        Direction::Forward
    );
    assert_eq!(
        field(&model, "Person", "employer_reverse").direction,
        Direction::Reverse
    );
}

#[test]
fn a_partial_shape_falls_back_to_the_observed_type() {
    // The shape declares the path and its cardinality but says nothing about
    // the values, so the data supplies the type.
    let model = build::build(
        &[observed(
            "Person",
            1,
            vec![prop("age", &[ValueTypeTag::INT])],
        )],
        &[shaped(
            "Person",
            vec![ShapedProperty {
                max_count: Some(1),
                ..shaped_prop("age")
            }],
        )],
        &namer(),
    );
    let age = field(&model, "Person", "age");
    assert_eq!(age.ty, FieldType::Scalar(Scalar::Int));
    assert!(!age.list, "the shape still supplies the cardinality");
}

#[test]
fn a_declared_name_beats_a_derived_one_regardless_of_order() {
    // `ex:Company` sorts before `ex:Person`, so a single-pass allocator would
    // hand `Person` to whichever came first. A declaration always wins.
    let model = build::build(
        &[],
        &[
            shaped("Person", vec![]),
            ShapeDescription {
                name: Some("Person".to_string()),
                ..shaped("Company", vec![])
            },
        ],
        &namer(),
    );
    let by_name: Vec<(&str, &str)> = model
        .objects
        .iter()
        .map(|o| (o.name.as_str(), o.iri.as_str()))
        .collect();
    assert_eq!(
        by_name,
        [
            ("Person", "http://example.org/Company"),
            ("ex_Person", "http://example.org/Person"),
        ]
    );
    // No conflict between *declarations*, so nothing to report.
    assert!(model.warnings.is_empty(), "{:?}", model.warnings);
}

#[test]
fn two_shapes_declaring_one_name_report_the_loser() {
    let model = build::build(
        &[],
        &[
            ShapeDescription {
                name: Some("Agent".to_string()),
                ..shaped("Person", vec![])
            },
            ShapeDescription {
                name: Some("Agent".to_string()),
                ..shaped("Company", vec![])
            },
        ],
        &namer(),
    );
    // `ex:Company` sorts first, so it keeps the declared name.
    assert_eq!(
        model.object("Agent").unwrap().iri,
        "http://example.org/Company"
    );
    assert!(model.object("Person").is_some(), "{:?}", model.objects);
    assert!(
        model
            .warnings
            .iter()
            .any(|w| w.contains("already claimed") && w.contains("Person")),
        "{:?}",
        model.warnings
    );
}

#[test]
fn a_declared_field_name_beats_a_derived_one() {
    let model = build::build(
        &[],
        &[shaped(
            "Person",
            vec![
                // Derived name would be `alias`.
                ShapedProperty {
                    datatype: Some(format!("{XSD}string")),
                    ..shaped_prop("alias")
                },
                // Declares the same name; the declaration takes it.
                ShapedProperty {
                    name: Some("alias".to_string()),
                    datatype: Some(format!("{XSD}string")),
                    ..shaped_prop("nickname")
                },
            ],
        )],
        &namer(),
    );
    assert_eq!(
        field(&model, "Person", "alias").iri,
        format!("{EX}nickname")
    );
    // The loser falls back to the collision rules: `ex:` qualifies it.
    assert_eq!(field_names(&model, "Person"), ["id", "ex_alias", "alias"]);
    assert_eq!(
        field(&model, "Person", "ex_alias").iri,
        format!("{EX}alias")
    );
    assert!(model.warnings.is_empty(), "{:?}", model.warnings);
}

// =============================================================================
// Bootstrap: schema → SHACL
// =============================================================================

#[test]
fn bootstrap_emits_paths_and_types_but_claims_no_cardinality() {
    use fluree_db_graphql::schema::bootstrap;
    use serde_json::{json, Value};

    let model = build::build(
        &[
            observed(
                "Person",
                2,
                vec![
                    prop("name", &[ValueTypeTag::STRING]),
                    prop("age", &[ValueTypeTag::INT]),
                    PropertyObservation {
                        iri: format!("{EX}employer"),
                        datatypes: vec![ValueTypeTag::JSON_LD_ID],
                        has_language_tags: false,
                        ref_classes: vec![format!("{EX}Company")],
                    },
                    // Two target classes: a union, which has no single `sh:class`.
                    PropertyObservation {
                        iri: format!("{EX}related"),
                        datatypes: vec![ValueTypeTag::JSON_LD_ID],
                        has_language_tags: false,
                        ref_classes: vec![format!("{EX}Company"), format!("{EX}Person")],
                    },
                ],
            ),
            observed("Company", 1, vec![]),
        ],
        &[],
        &namer(),
    );

    let shapes = bootstrap::to_shacl(&model);
    let graph = shapes["@graph"].as_array().unwrap();
    let person = graph
        .iter()
        .find(|s| s["sh:targetClass"]["@id"] == format!("{EX}Person"))
        .unwrap();
    assert_eq!(person["@id"], format!("{EX}PersonShape"));
    assert_eq!(person["@type"], "sh:NodeShape");

    let properties: Vec<&Value> = person["sh:property"].as_array().unwrap().iter().collect();
    let by_path = |iri: &str| {
        properties
            .iter()
            .find(|p| p["sh:path"]["@id"] == format!("{EX}{iri}"))
            .copied()
            .unwrap_or_else(|| panic!("no property shape for {iri}"))
    };

    assert_eq!(
        by_path("name")["sh:datatype"],
        json!({ "@id": "xsd:string" })
    );
    // This fixture observed `xsd:int` specifically, which round-trips exactly.
    // A plain JSON integer would have been stored as the unbounded
    // `xsd:integer` and come back as such.
    assert_eq!(by_path("age")["sh:datatype"], json!({ "@id": "xsd:int" }));
    assert_eq!(
        by_path("employer")["sh:class"],
        json!({ "@id": format!("{EX}Company") })
    );
    // A union names no one class; all that can be said is that values are IRIs.
    assert!(by_path("related").get("sh:class").is_none());
    assert_eq!(
        by_path("related")["sh:nodeKind"],
        json!({ "@id": "sh:IRI" })
    );

    // The claims statistics cannot justify are left for the author to add.
    for property in &properties {
        for absent in ["sh:minCount", "sh:maxCount", "sh:in"] {
            assert!(
                property.get(absent).is_none(),
                "bootstrap should not assert {absent}: {property}"
            );
        }
    }
    assert!(person.get("sh:closed").is_none(), "{person}");

    // `id` is the subject IRI, not a property.
    assert!(properties.iter().all(|p| p["sh:path"]["@id"] != "id"));
}

#[test]
fn bootstrap_round_trips_into_the_shaped_tier() {
    use fluree_db_graphql::schema::bootstrap;

    let observations = [observed(
        "Person",
        2,
        vec![prop("name", &[ValueTypeTag::STRING])],
    )];
    let inferred = build::build(&observations, &[], &namer());
    assert!(
        field(&inferred, "Person", "name").list,
        "inferred fields are lists"
    );

    // The emitted shape, read back as tier-2 input, must at least reproduce the
    // types it described — that is what makes it a usable starting point.
    let emitted = bootstrap::to_shacl(&inferred);
    let person_shape = emitted["@graph"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["sh:targetClass"]["@id"] == format!("{EX}Person"))
        .unwrap()
        .clone();
    let properties: Vec<ShapedProperty> = person_shape["sh:property"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| ShapedProperty {
            iri: p["sh:path"]["@id"].as_str().unwrap().to_string(),
            datatype: Some(format!("{XSD}string")),
            // The edit an author would make.
            min_count: Some(1),
            max_count: Some(1),
            ..Default::default()
        })
        .collect();

    let shaped = build::build(&observations, &[shaped("Person", properties)], &namer());
    let name = field(&shaped, "Person", "name");
    assert_eq!(name.ty, FieldType::Scalar(Scalar::String));
    assert!(!name.list && name.non_null);
}
