//! Turn a derived schema back into SHACL, as a starting point for tier 2.
//!
//! The output is deliberately weak: paths and value types, no cardinalities, no
//! `sh:closed`, no `sh:in`. Those are the claims statistics cannot justify, and
//! they are exactly what a person adds by editing — emitting a guess at them
//! would put words in the author's mouth and, since Fluree enforces SHACL once
//! shapes exist, could start rejecting writes that were previously fine.
//!
//! Nothing here transacts. The shapes are printed for the author to read, edit,
//! and apply deliberately.

use serde_json::{json, Map, Value as Json};

use crate::schema::model::{Direction, FieldType, Scalar, SchemaModel};

/// The `@context` the emitted shapes are written against.
pub fn shapes_context() -> Json {
    json!({
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    })
}

/// Emit one `sh:NodeShape` per exposed class, as a JSON-LD document.
pub fn to_shacl(model: &SchemaModel) -> Json {
    let mut graph = Vec::new();
    for object in &model.objects {
        // The `Node` placeholder stands for subjects of unexposed types; it has
        // no class to target.
        if object.iri.is_empty() {
            continue;
        }
        let properties: Vec<Json> = object
            .fields
            .iter()
            .filter(|f| !f.is_id())
            .map(|f| {
                let mut shape = Map::new();
                shape.insert(
                    "sh:path".to_string(),
                    match f.direction {
                        Direction::Forward => json!({ "@id": f.iri }),
                        Direction::Reverse => {
                            json!({ "sh:inversePath": { "@id": f.iri } })
                        }
                    },
                );
                match &f.ty {
                    FieldType::Scalar(scalar) => {
                        shape.insert(
                            "sh:datatype".to_string(),
                            json!({ "@id": datatype_iri(*scalar) }),
                        );
                    }
                    FieldType::Object(name) => {
                        // A reference to a class the schema exposes names that
                        // class; the `Node` placeholder names none, so all that
                        // can be said is that the values are IRIs.
                        match model.object(name).filter(|o| !o.iri.is_empty()) {
                            Some(target) => {
                                shape.insert("sh:class".to_string(), json!({ "@id": target.iri }));
                            }
                            None => {
                                shape.insert("sh:nodeKind".to_string(), json!({ "@id": "sh:IRI" }));
                            }
                        }
                    }
                    // A union or interface has no single `sh:class`; an author
                    // narrowing it should pick one, or use `sh:or`.
                    FieldType::Union(_) | FieldType::Interface(_) => {
                        shape.insert("sh:nodeKind".to_string(), json!({ "@id": "sh:IRI" }));
                    }
                    // An enum only exists because a shape already declared
                    // `sh:in`; re-emitting it would restate the input.
                    FieldType::Enum(_) | FieldType::Id => {}
                }
                if let Some(description) = &f.description {
                    shape.insert("sh:description".to_string(), json!(description));
                }
                Json::Object(shape)
            })
            .collect();

        let mut node_shape = Map::new();
        node_shape.insert("@id".to_string(), json!(shape_iri(&object.iri)));
        node_shape.insert("@type".to_string(), json!("sh:NodeShape"));
        node_shape.insert("sh:targetClass".to_string(), json!({ "@id": object.iri }));
        if let Some(description) = &object.description {
            node_shape.insert("sh:description".to_string(), json!(description));
        }
        node_shape.insert("sh:property".to_string(), Json::Array(properties));
        graph.push(Json::Object(node_shape));
    }

    json!({ "@context": shapes_context(), "@graph": graph })
}

/// `http://example.org/Person` → `http://example.org/PersonShape`.
fn shape_iri(class_iri: &str) -> String {
    format!("{class_iri}Shape")
}

/// The canonical datatype IRI for a scalar.
///
/// Lossy on purpose, and in one direction only: several XSD types map to
/// `String`, so a field that is really `rdf:langString` comes back as
/// `xsd:string`. The emitted shape is a draft to edit, not a description of the
/// data — which is why it is printed rather than transacted.
fn datatype_iri(scalar: Scalar) -> &'static str {
    match scalar {
        Scalar::String => "xsd:string",
        Scalar::Int => "xsd:int",
        Scalar::Long => "xsd:integer",
        Scalar::Float => "xsd:double",
        Scalar::Decimal => "xsd:decimal",
        Scalar::Boolean => "xsd:boolean",
        Scalar::Id => "xsd:anyURI",
        Scalar::DateTime => "xsd:dateTime",
        Scalar::Date => "xsd:date",
        Scalar::Time => "xsd:time",
        Scalar::Json => "rdf:JSON",
    }
}
