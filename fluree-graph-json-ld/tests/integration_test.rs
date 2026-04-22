//! Integration tests for JSON-LD expansion behavior

use fluree_graph_json_ld::{
    compact_fn_with_tracking, compact_iri, expand, normalize_data, parse_context, Container,
    TypeValue,
};
use pretty_assertions::assert_eq;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ============================================================================
// Context Parsing Tests (mirrors context_test.cljc)
// ============================================================================

#[test]
fn test_vocab_with_references() {
    // References to default vocabulary should be concatenated
    let ctx = parse_context(&json!({
        "@vocab": "https://schema.org/",
        "reverseRef": {"@reverse": "isBasedOn"},
        "explicit": "name",
        "dontTouch": "https://example.com/ns#42",
        "id": "@id"
    }))
    .unwrap();

    assert_eq!(ctx.vocab, Some("https://schema.org/".to_string()));
    assert_eq!(
        ctx.get("reverseRef").unwrap().reverse,
        Some("https://schema.org/isBasedOn".to_string())
    );
    assert_eq!(
        ctx.get("explicit").unwrap().id,
        Some("https://schema.org/name".to_string())
    );
    assert_eq!(
        ctx.get("dontTouch").unwrap().id,
        Some("https://example.com/ns#42".to_string())
    );
    assert_eq!(ctx.get("id").unwrap().id, Some("@id".to_string()));
}

#[test]
fn test_dependent_context_two_levels_with_map_val() {
    // from CLR vocabulary
    let ctx = parse_context(&json!({
        "clri": "https://purl.imsglobal.org/spec/clr/vocab#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "UUID": "dtUUID",
        "dtUUID": {"@id": "clri:dtUUID", "@type": "xsd:string"}
    }))
    .unwrap();

    assert_eq!(
        ctx.get("UUID").unwrap().id,
        Some("https://purl.imsglobal.org/spec/clr/vocab#dtUUID".to_string())
    );
    assert_eq!(
        ctx.get("dtUUID").unwrap().type_,
        Some(TypeValue::Iri(
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        ))
    );
}

#[test]
fn test_nested_context_details() {
    // Custom full IRI with type defined
    let ctx = parse_context(&json!({
        "schema": "http://schema.org/",
        "customScalar": {"@id": "http://schema.org/name", "@type": "http://schema.org/Text"}
    }))
    .unwrap();

    let entry = ctx.get("customScalar").unwrap();
    assert_eq!(entry.id, Some("http://schema.org/name".to_string()));
    assert_eq!(
        entry.type_,
        Some(TypeValue::Iri("http://schema.org/Text".to_string()))
    );
}

#[test]
fn test_second_context_relies_on_first() {
    // This scenario happened with security v1 -> v2
    let ctx = parse_context(&json!([
        {"sec": "https://w3id.org/security#"},
        {"EcdsaSecp256k1VerificationKey2019": "sec:EcdsaSecp256k1VerificationKey2019"}
    ]))
    .unwrap();

    assert_eq!(
        ctx.get("EcdsaSecp256k1VerificationKey2019").unwrap().id,
        Some("https://w3id.org/security#EcdsaSecp256k1VerificationKey2019".to_string())
    );
}

#[test]
fn test_multiple_container_values() {
    let ctx = parse_context(&json!({
        "schema": "http://schema.org/",
        "post": {"@id": "schema:blogPost", "@container": ["@index", "@set"]}
    }))
    .unwrap();

    let entry = ctx.get("post").unwrap();
    assert_eq!(
        entry.container,
        Some(vec![Container::Index, Container::Set])
    );
}

#[test]
fn test_relative_vocab_with_base() {
    let ctx = parse_context(&json!({
        "@base": "http://example.com/",
        "@vocab": "ns/",
        "iriProperty": {"@type": "@id"}
    }))
    .unwrap();

    assert_eq!(ctx.vocab, Some("http://example.com/ns/".to_string()));
}

// ============================================================================
// Expansion Tests (mirrors expand_test.cljc)
// ============================================================================

#[test]
fn test_expand_datatype_in_context() {
    let doc = json!({
        "@context": {
            "ical": "http://www.w3.org/2002/12/cal/ical#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "ical:dtstart": {"@type": "xsd:dateTime"}
        },
        "ical:summary": "Lady Gaga Concert",
        "ical:location": "New Orleans Arena, New Orleans, Louisiana, USA",
        "ical:dtstart": "2011-04-09T20:00:00Z"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(
        obj["http://www.w3.org/2002/12/cal/ical#summary"][0]["@value"],
        "Lady Gaga Concert"
    );
    assert_eq!(
        obj["http://www.w3.org/2002/12/cal/ical#dtstart"][0]["@value"],
        "2011-04-09T20:00:00Z"
    );
    assert_eq!(
        obj["http://www.w3.org/2002/12/cal/ical#dtstart"][0]["@type"],
        "http://www.w3.org/2001/XMLSchema#dateTime"
    );
}

#[test]
fn test_expand_nested_child_with_datatypes() {
    let doc = json!({
        "@context": {
            "name": "http://schema.org/name",
            "description": "http://schema.org/description",
            "image": {"@id": "http://schema.org/image", "@type": "@id"},
            "geo": "http://schema.org/geo",
            "latitude": {"@id": "http://schema.org/latitude", "@type": "xsd:float"},
            "longitude": {"@id": "http://schema.org/longitude", "@type": "xsd:float"},
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "name": "The Empire State Building",
        "description": "The Empire State Building is a 102-story landmark in New York City.",
        "image": "http://www.civil.usherbrooke.ca/cours/gci215a/empire-state-building.jpg",
        "geo": {"latitude": "40.75", "longitude": "73.98"}
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // Check image is expanded as @id
    assert_eq!(
        obj["http://schema.org/image"][0]["@id"],
        "http://www.civil.usherbrooke.ca/cours/gci215a/empire-state-building.jpg"
    );

    // Check nested geo
    let geo = &obj["http://schema.org/geo"][0];
    assert_eq!(geo["http://schema.org/latitude"][0]["@value"], "40.75");
    assert_eq!(
        geo["http://schema.org/latitude"][0]["@type"],
        "http://www.w3.org/2001/XMLSchema#float"
    );
}

#[test]
fn test_expand_graph_default() {
    let doc = json!({
        "@context": {
            "dc11": "http://purl.org/dc/elements/1.1/",
            "ex": "http://example.org/vocab#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "ex:contains": {"@type": "@id"}
        },
        "@graph": [
            {
                "@id": "http://example.org/library",
                "@type": "ex:Library",
                "ex:contains": "http://example.org/library/the-republic"
            },
            {
                "@id": "http://example.org/library/the-republic",
                "@type": "ex:Book",
                "dc11:creator": "Plato",
                "dc11:title": "The Republic"
            }
        ]
    });

    let result = expand(&doc).unwrap();
    let arr = result.as_array().unwrap();

    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["@id"], "http://example.org/library");
    assert_eq!(arr[0]["@type"], json!(["http://example.org/vocab#Library"]));
    assert_eq!(arr[1]["@id"], "http://example.org/library/the-republic");
}

#[test]
fn test_expand_graph_named() {
    let doc = json!({
        "@context": {
            "dc11": "http://purl.org/dc/elements/1.1/",
            "ex": "http://example.org/vocab#",
            "ex:contains": {"@type": "@id"}
        },
        "@id": "ex:alexandria",
        "ex:burnedAt": "640 CE",
        "@graph": [
            {
                "@id": "http://example.org/library",
                "@type": "ex:Library"
            }
        ]
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // Named graph retains @id and properties
    assert_eq!(obj["@id"], "http://example.org/vocab#alexandria");
    assert!(obj.contains_key("http://example.org/vocab#burnedAt"));
    assert!(obj.contains_key("@graph"));
}

#[test]
fn test_expand_list_in_context() {
    let doc = json!({
        "@context": {
            "nick": {"@id": "http://xmlns.com/foaf/0.1/nick", "@container": "@list"}
        },
        "@id": "http://example.org/people#joebob",
        "nick": ["joe", "bob", "jaybee"]
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    let nicks = &obj["http://xmlns.com/foaf/0.1/nick"][0];
    let list = nicks["@list"].as_array().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0]["@value"], "joe");
    assert_eq!(list[1]["@value"], "bob");
    assert_eq!(list[2]["@value"], "jaybee");
}

#[test]
fn test_expand_list_inline() {
    let doc = json!({
        "@context": {"foaf": "http://xmlns.com/foaf/0.1/"},
        "@id": "http://example.org/people#joebob",
        "foaf:nick": {"@list": ["joe", "bob", "jaybee"]}
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    let nicks = &obj["http://xmlns.com/foaf/0.1/nick"][0];
    let list = nicks["@list"].as_array().unwrap();
    assert_eq!(list.len(), 3);
}

#[test]
fn test_expand_sequential_values_with_maps() {
    let doc = json!({
        "@context": {
            "gist": "https://ontologies.semanticarts.com/gist/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "gist:CoherentUnit",
        "skos:scopeNote": [
            {"@type": "xsd:string", "@value": "First note"},
            {"@type": "xsd:string", "@value": "Second note"}
        ],
        "@type": "owl:Class"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    let notes = obj["http://www.w3.org/2004/02/skos/core#scopeNote"]
        .as_array()
        .unwrap();
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0]["@type"], "http://www.w3.org/2001/XMLSchema#string");
    assert_eq!(notes[0]["@value"], "First note");
}

#[test]
fn test_expand_language_tag_in_value() {
    let doc = json!({
        "@context": {"ex": "http://example.com/vocab/"},
        "ex:name": "Frank",
        "ex:occupation": {"@value": "Ninja", "@language": "en"}
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["http://example.com/vocab/name"][0]["@value"], "Frank");
    assert_eq!(
        obj["http://example.com/vocab/occupation"][0]["@value"],
        "Ninja"
    );
    assert_eq!(
        obj["http://example.com/vocab/occupation"][0]["@language"],
        "en"
    );
}

#[test]
fn test_expand_language_tag_in_context() {
    let doc = json!({
        "@context": {
            "ex": "http://example.com/vocab/",
            "@language": "en"
        },
        "ex:name": "Frank",
        "ex:age": 33
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // Strings get language tag
    assert_eq!(obj["http://example.com/vocab/name"][0]["@value"], "Frank");
    assert_eq!(obj["http://example.com/vocab/name"][0]["@language"], "en");

    // Numbers don't get language tag
    assert_eq!(obj["http://example.com/vocab/age"][0]["@value"], 33);
    assert!(obj["http://example.com/vocab/age"][0]
        .get("@language")
        .is_none());
}

#[test]
fn test_expand_language_map() {
    let doc = json!({
        "@context": {
            "ex": "http://example.com/vocab/",
            "occupation": {"@id": "ex:occupation", "@container": "@language"}
        },
        "ex:name": "Frank",
        "occupation": {
            "en": "Ninja",
            "ja": "忍者"
        }
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    let occupations = obj["http://example.com/vocab/occupation"]
        .as_array()
        .unwrap();
    assert_eq!(occupations.len(), 2);

    // Check both language entries exist
    let has_en = occupations
        .iter()
        .any(|o| o["@language"] == "en" && o["@value"] == "Ninja");
    let has_ja = occupations
        .iter()
        .any(|o| o["@language"] == "ja" && o["@value"] == "忍者");
    assert!(has_en);
    assert!(has_ja);
}

#[test]
fn test_expand_json_type() {
    let doc = json!({
        "@context": {
            "gr": "http://purl.org/goodrelations/v1#",
            "my:json": {"@type": "@json"}
        },
        "my:json": {"foo": {"bar": [1, false, 9.0, null]}}
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    let json_val = &obj["my:json"][0];
    assert_eq!(json_val["@type"], "@json");
    assert_eq!(json_val["@value"]["foo"]["bar"][0], 1);
}

#[test]
fn test_expand_empty_colon_prefix() {
    let doc = json!({
        "@context": {
            "ex": "http://example.com/vocab/",
            ":": "http://somedomain.org/"
        },
        "ex:name": "Frank",
        ":age": 33
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert!(obj.contains_key("http://example.com/vocab/name"));
    assert!(obj.contains_key("http://somedomain.org/age"));
}

// ============================================================================
// Compaction Tests (mirrors compact_test.cljc)
// ============================================================================

#[test]
fn test_compact_with_vocab() {
    let ctx = parse_context(&json!("https://schema.org")).unwrap();
    assert_eq!(compact_iri("https://schema.org/name", &ctx), "name");
}

#[test]
fn test_compact_fn_usage_tracking() {
    let ctx = parse_context(&json!({
        "schema": "http://schema.org/",
        "REPLACE": "http://schema.org/Person"
    }))
    .unwrap();

    let used = Arc::new(Mutex::new(HashMap::new()));
    let f = compact_fn_with_tracking(&ctx, used.clone());

    assert_eq!(f("http://schema.org/name"), "schema:name");
    {
        let guard = used.lock().unwrap();
        assert_eq!(guard.get("schema"), Some(&"http://schema.org/".to_string()));
    }

    assert_eq!(f("http://schema.org/Person"), "REPLACE");
    {
        let guard = used.lock().unwrap();
        assert_eq!(
            guard.get("REPLACE"),
            Some(&"http://schema.org/Person".to_string())
        );
    }

    // Non-matching IRI
    assert_eq!(
        f("http://example.org/ns#blah"),
        "http://example.org/ns#blah"
    );
    {
        let guard = used.lock().unwrap();
        assert!(!guard.contains_key("http://example.org/ns#blah"));
    }
}

// ============================================================================
// Normalization Tests (mirrors normalize_test.cljc)
// ============================================================================

#[test]
fn test_normalize_complex_nested() {
    let data = json!({
        "1": {"f": {"f": "hi", "F": 5}, "\n": 56.0},
        "10": {},
        "": "empty",
        "a": {},
        "111": [{"e": "yes", "E": "no"}],
        "A": {}
    });

    let result = normalize_data(&data);
    // Keys sorted lexicographically, 56.0 becomes 56
    assert_eq!(
        result,
        r#"{"":"empty","1":{"\n":56,"f":{"F":5,"f":"hi"}},"10":{},"111":[{"E":"no","e":"yes"}],"A":{},"a":{}}"#
    );
}

#[test]
fn test_normalize_scientific_notation() {
    let data = json!({
        "numbers": [333_333_333.333_333_3, 1E30, 4.50, 2e-3, 0.000_000_000_000_000_000_000_000_001]
    });

    let result = normalize_data(&data);
    assert!(result.contains("1e+30"));
    assert!(result.contains("4.5"));
    assert!(result.contains("0.002"));
    assert!(result.contains("1e-27"));
}

// ============================================================================
// Type Sub-Context Tests
// ============================================================================

#[test]
fn test_type_dependent_sub_context() {
    let doc = json!({
        "@context": {
            "id": "@id",
            "type": "@type",
            "VerifiableCredential": {
                "@id": "https://www.w3.org/2018/credentials#VerifiableCredential",
                "@context": {
                    "id": "@id",
                    "type": "@type",
                    "cred": "https://www.w3.org/2018/credentials#",
                    "issuer": {"@id": "cred:issuer", "@type": "@id"}
                }
            }
        },
        "id": "#joebob",
        "type": ["VerifiableCredential"],
        "issuer": "did:for:some-issuer"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "#joebob");
    assert_eq!(
        obj["@type"],
        json!(["https://www.w3.org/2018/credentials#VerifiableCredential"])
    );
    // issuer should be expanded using the type's sub-context
    assert!(obj.contains_key("https://www.w3.org/2018/credentials#issuer"));
    assert_eq!(
        obj["https://www.w3.org/2018/credentials#issuer"][0]["@id"],
        "did:for:some-issuer"
    );
}
