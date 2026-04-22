//! Tests for specific features: @type aliasing and @base relative IRI resolution

use fluree_graph_json_ld::expand;
use serde_json::json;

// ============================================================================
// Custom @type key aliasing tests
// ============================================================================

#[test]
fn test_type_alias_basic() {
    // Context defines "type" as an alias for "@type"
    let doc = json!({
        "@context": {
            "type": "@type",
            "schema": "http://schema.org/"
        },
        "@id": "http://example.org/1",
        "type": "schema:Person"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "http://example.org/1");
    assert_eq!(obj["@type"], json!(["http://schema.org/Person"]));
}

#[test]
fn test_type_alias_with_id_alias() {
    // Context defines both "id" and "type" as aliases
    let doc = json!({
        "@context": {
            "id": "@id",
            "type": "@type",
            "schema": "http://schema.org/"
        },
        "id": "http://example.org/1",
        "type": "schema:Person"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "http://example.org/1");
    assert_eq!(obj["@type"], json!(["http://schema.org/Person"]));
}

#[test]
fn test_type_alias_mixed_usage() {
    // Use both aliased "type" and standard "@type" - aliased key should work
    let doc = json!({
        "@context": {
            "type": "@type",
            "schema": "http://schema.org/"
        },
        "@id": "http://example.org/1",
        "@type": "schema:Person"  // Using standard @type even though "type" is aliased
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // @type should still be recognized
    assert_eq!(obj["@type"], json!(["http://schema.org/Person"]));
}

// ============================================================================
// @base relative IRI resolution tests
// ============================================================================

#[test]
fn test_base_fragment_id() {
    // @base with fragment identifier
    let doc = json!({
        "@context": {
            "@base": "https://example.com/resource"
        },
        "@id": "#fragment"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "https://example.com/resource#fragment");
}

#[test]
fn test_base_with_vocab() {
    // Both @base and @vocab
    let doc = json!({
        "@context": {
            "@base": "https://base.com/",
            "@vocab": "https://vocab.com/",
            "name": {"@type": "@id"}
        },
        "@id": "person/1",
        "name": "relative-link"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // @id uses @base
    assert_eq!(obj["@id"], "https://base.com/person/1");
    // Property uses @vocab
    assert!(obj.contains_key("https://vocab.com/name"));
    // Value with @type: @id uses @base
    assert_eq!(
        obj["https://vocab.com/name"][0]["@id"],
        "https://base.com/relative-link"
    );
}

#[test]
fn test_base_with_nested_context() {
    // @base in a nested context
    let doc = json!({
        "@context": {
            "@base": "https://outer.com/",
            "inner": {
                "@id": "https://example.org/inner",
                "@context": {
                    "@base": "https://inner.com/"
                }
            }
        },
        "@id": "outer-id",
        "inner": {
            "@id": "inner-id"
        }
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // Outer @id uses outer @base
    assert_eq!(obj["@id"], "https://outer.com/outer-id");
}

#[test]
fn test_base_absolute_iri_unchanged() {
    // Absolute IRIs should not be modified by @base
    let doc = json!({
        "@context": {
            "@base": "https://base.com/"
        },
        "@id": "https://absolute.com/resource"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "https://absolute.com/resource");
}

#[test]
fn test_base_empty_resets() {
    // Empty @vocab uses @base
    let doc = json!({
        "@context": {
            "@base": "https://example.com/",
            "@vocab": ""
        },
        "@id": "resource",
        "name": "test"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    // @vocab: "" should use @base
    assert!(obj.contains_key("https://example.com/name"));
}

// ============================================================================
// Tests adapted from legacy expand test cases: base-and-vocab-test
// ============================================================================

#[test]
fn test_base_and_vocab_full() {
    // Scenario: base-and-vocab-test
    let doc = json!({
        "@context": {
            "@base": "https://base.com/base/iri",
            "@vocab": "https://vocab.com/vocab/iri/",
            "iriProperty": {"@type": "@id"}
        },
        "@id": "#joebob",
        "@type": "Joey",
        "name": "Joe Bob",
        "iriProperty": "#a-relative-id"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "https://base.com/base/iri#joebob");
    assert_eq!(obj["@type"], json!(["https://vocab.com/vocab/iri/Joey"]));
    assert!(obj.contains_key("https://vocab.com/vocab/iri/name"));
    assert_eq!(
        obj["https://vocab.com/vocab/iri/name"][0]["@value"],
        "Joe Bob"
    );
    assert_eq!(
        obj["https://vocab.com/vocab/iri/iriProperty"][0]["@id"],
        "https://base.com/base/iri#a-relative-id"
    );
}

#[test]
fn test_relative_vocab_resolves_against_base() {
    // A relative @vocab should resolve against @base
    let doc = json!({
        "@context": {
            "@base": "http://example.com/",
            "@vocab": "ns/",
            "iriProperty": {"@type": "@id"}
        },
        "@id": "#joebob",
        "@type": "Joey",
        "name": "Joe Bob",
        "iriProperty": "#a-relative-id"
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@type"], json!(["http://example.com/ns/Joey"]));
    assert_eq!(obj["@id"], "http://example.com/#joebob");
    assert!(obj.contains_key("http://example.com/ns/name"));
    assert_eq!(
        obj["http://example.com/ns/iriProperty"][0]["@id"],
        "http://example.com/#a-relative-id"
    );
}

// ============================================================================
// Tests adapted from legacy expand test cases: type-sub-context
// ============================================================================

#[test]
fn test_type_sub_context_with_aliased_keys() {
    // Type-dependent sub-context with aliased id/type keys
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
                    "credentialSchema": {
                        "@id": "cred:credentialSchema",
                        "@type": "@id",
                        "@context": {
                            "id": "@id",
                            "type": "@type",
                            "cred": "https://www.w3.org/2018/credentials#"
                        }
                    },
                    "issuer": {"@id": "cred:issuer", "@type": "@id"}
                }
            }
        },
        "id": "#joebob",
        "type": ["VerifiableCredential"],
        "issuer": "did:for:some-issuer",
        "credentialSchema": {"id": "#credSchema", "cred": "Some Cred!"}
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "#joebob");
    assert_eq!(
        obj["@type"],
        json!(["https://www.w3.org/2018/credentials#VerifiableCredential"])
    );
    assert!(obj.contains_key("https://www.w3.org/2018/credentials#issuer"));
    assert_eq!(
        obj["https://www.w3.org/2018/credentials#issuer"][0]["@id"],
        "did:for:some-issuer"
    );
}

// ============================================================================
// Tests adapted from legacy expand test cases: shacl-embedded-nodes (string version)
// ============================================================================

#[test]
fn test_shacl_embedded_nodes() {
    let doc = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "sh": "http://www.w3.org/ns/shacl#",
            "schema": "http://schema.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "ex:UserShape",
        "@type": ["sh:NodeShape"],
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "sh:path": {"@id": "schema:name"},
            "sh:datatype": {"@id": "xsd:string"}
        }]
    });

    let result = expand(&doc).unwrap();
    let obj = result.as_object().unwrap();

    assert_eq!(obj["@id"], "http://example.org/ns/UserShape");
    assert_eq!(
        obj["@type"],
        json!(["http://www.w3.org/ns/shacl#NodeShape"])
    );

    let target_class = &obj["http://www.w3.org/ns/shacl#targetClass"][0];
    assert_eq!(target_class["@id"], "http://example.org/ns/User");

    let property = &obj["http://www.w3.org/ns/shacl#property"][0];
    assert_eq!(
        property["http://www.w3.org/ns/shacl#path"][0]["@id"],
        "http://schema.org/name"
    );
    assert_eq!(
        property["http://www.w3.org/ns/shacl#datatype"][0]["@id"],
        "http://www.w3.org/2001/XMLSchema#string"
    );
}
