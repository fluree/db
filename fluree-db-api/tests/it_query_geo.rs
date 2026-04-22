//! Geospatial query integration tests
//!
//! Tests geo:wktLiteral POINT storage and geof:distance function.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn geo_context() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "geo": "http://www.opengis.net/ont/geosparql#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_cities(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);
    let ctx = geo_context();

    // Insert cities with geo:wktLiteral POINT locations
    // WKT format: POINT(longitude latitude)
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:paris",
                "@type": "ex:City",
                "ex:name": "Paris",
                "ex:location": {
                    "@value": "POINT(2.3522 48.8566)",
                    "@type": "geo:wktLiteral"
                }
            },
            {
                "@id": "ex:london",
                "@type": "ex:City",
                "ex:name": "London",
                "ex:location": {
                    "@value": "POINT(-0.1278 51.5074)",
                    "@type": "geo:wktLiteral"
                }
            },
            {
                "@id": "ex:berlin",
                "@type": "ex:City",
                "ex:name": "Berlin",
                "ex:location": {
                    "@value": "POINT(13.4050 52.5200)",
                    "@type": "geo:wktLiteral"
                }
            },
            {
                "@id": "ex:tokyo",
                "@type": "ex:City",
                "ex:name": "Tokyo",
                "ex:location": {
                    "@value": "POINT(139.6917 35.6895)",
                    "@type": "geo:wktLiteral"
                }
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert cities")
        .ledger
}

#[tokio::test]
async fn geo_point_roundtrip_preserves_wkt_format() {
    // Verify that geo:wktLiteral POINT values are stored and returned correctly
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_cities(&fluree, "geo:roundtrip").await;
    let ctx = geo_context();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?loc"],
        "where": [
            {"@id": "ex:paris", "ex:name": "?name", "ex:location": "?loc"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    // Should return Paris with its location as WKT POINT
    let rows = result.as_array().expect("result should be array");
    assert_eq!(rows.len(), 1, "Should have exactly one result");
    let row = &rows[0];
    assert_eq!(row[0], "Paris");

    // The location is returned as a typed literal: {"@value": "POINT(...)", "@type": "geo:wktLiteral"}
    let loc_obj = row[1]
        .as_object()
        .expect("location should be typed literal object");
    let loc = loc_obj
        .get("@value")
        .expect("should have @value")
        .as_str()
        .expect("@value should be string");
    let loc_type = loc_obj
        .get("@type")
        .expect("should have @type")
        .as_str()
        .expect("@type should be string");

    assert_eq!(loc_type, "geo:wktLiteral");
    assert!(loc.starts_with("POINT("), "Expected POINT WKT, got: {loc}");
    assert!(loc.contains("2.35"), "Expected longitude ~2.35, got: {loc}");
    assert!(
        loc.contains("48.85"),
        "Expected latitude ~48.85, got: {loc}"
    );
}

#[tokio::test]
async fn geof_distance_in_filter_finds_nearby_cities() {
    // Test geof:distance function in a FILTER clause
    // Find all cities within 500km of Paris
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_cities(&fluree, "geo:filter").await;
    let ctx = geo_context();

    // Paris coordinates for reference point - using s-expression filter
    let query = json!({
        "@context": ctx,
        "select": ["?name"],
        "where": [
            {"@id": "?city", "@type": "ex:City", "ex:name": "?name", "ex:location": "?loc"},
            // Filter: distance from Paris < 500km (500000 meters)
            // Paris-London is ~343km, Paris-Berlin is ~878km, Paris-Tokyo is ~9700km
            ["filter", "(< (geo_distance ?loc \"POINT(2.3522 48.8566)\") 500000)"]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    // Should find Paris (0km) and London (~343km), but not Berlin (~878km) or Tokyo (~9700km)
    let rows = result.as_array().expect("result should be array");
    // For single-variable select, each row is the value directly (not a nested array)
    let names: Vec<&str> = rows
        .iter()
        .map(|r| r.as_str().expect("name should be string"))
        .collect();

    assert!(
        names.contains(&"Paris"),
        "Paris should be within 500km of itself"
    );
    assert!(
        names.contains(&"London"),
        "London should be within 500km of Paris"
    );
    assert!(
        !names.contains(&"Berlin"),
        "Berlin should NOT be within 500km of Paris"
    );
    assert!(
        !names.contains(&"Tokyo"),
        "Tokyo should NOT be within 500km of Paris"
    );
}

#[tokio::test]
async fn geof_distance_in_bind_calculates_distances() {
    // Test geof:distance function in a BIND clause to compute actual distances
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_cities(&fluree, "geo:bind").await;
    let ctx = geo_context();

    // Get distance from Paris to London using bind with s-expression
    let query = json!({
        "@context": ctx,
        "select": ["?distance"],
        "where": [
            {"@id": "ex:paris", "ex:location": "?paris_loc"},
            {"@id": "ex:london", "ex:location": "?london_loc"},
            ["bind", "?distance", "(geo_distance ?paris_loc ?london_loc)"]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let rows = result.as_array().expect("result should be array");
    assert_eq!(rows.len(), 1, "Should have exactly one result");
    // For single-variable select, the value is directly in rows[0] (not rows[0][0])
    let distance = rows[0].as_f64().expect("distance should be number");

    // Paris to London is approximately 343.5 km (343500 meters)
    assert!(
        (distance - 343_500.0).abs() < 5000.0,
        "Expected ~343.5km, got {distance} meters"
    );
}

#[tokio::test]
async fn geof_distance_with_literal_wkt_points() {
    // Test geof:distance with literal WKT POINT strings (not from database)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "geo:literal");

    // Insert minimal data just to have a ledger to query against
    let insert = json!({
        "@context": geo_context(),
        "@graph": [{"@id": "ex:placeholder", "ex:name": "test"}]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query using literal WKT points in BIND
    let query = json!({
        "@context": geo_context(),
        "select": ["?distance"],
        "where": [
            {"@id": "ex:placeholder", "ex:name": "?name"},
            // Paris to London using literal WKT strings
            ["bind", "?distance", "(geo_distance \"POINT(2.3522 48.8566)\" \"POINT(-0.1278 51.5074)\")"]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let rows = result.as_array().expect("result should be array");
    assert_eq!(rows.len(), 1, "Should have exactly one result");
    // For single-variable select, the value is directly in rows[0] (not rows[0][0])
    let distance = rows[0].as_f64().expect("distance should be number");
    assert!(
        (distance - 343_500.0).abs() < 5000.0,
        "Expected ~343.5km between Paris and London WKT literals, got {distance} meters"
    );
}

#[tokio::test]
async fn non_point_wkt_stored_as_string() {
    // Test that non-POINT WKT geometries (like LINESTRING) are stored as strings
    // and can be queried back
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "geo:nonpoint");
    let ctx = geo_context();

    // Insert a LINESTRING (not a POINT)
    let insert = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:route",
            "ex:name": "test route",
            "ex:path": {
                "@value": "LINESTRING(0 0, 1 1, 2 2)",
                "@type": "geo:wktLiteral"
            }
        }]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query back the linestring - it should be stored as a string
    let query = json!({
        "@context": ctx,
        "select": ["?name", "?path"],
        "where": [
            {"@id": "ex:route", "ex:name": "?name", "ex:path": "?path"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let rows = result.as_array().expect("result should be array");
    assert_eq!(rows.len(), 1, "Should have exactly one result");
    let row = rows[0].as_array().expect("row should be array");
    assert_eq!(row[0], "test route");
    // LINESTRING is returned as typed literal: {"@value": "...", "@type": "geo:wktLiteral"}
    let path_obj = row[1]
        .as_object()
        .expect("path should be typed literal object");
    let path = path_obj
        .get("@value")
        .expect("should have @value")
        .as_str()
        .expect("@value should be string");
    let path_type = path_obj
        .get("@type")
        .expect("should have @type")
        .as_str()
        .expect("@type should be string");
    assert_eq!(path_type, "geo:wktLiteral");
    assert!(
        path.contains("LINESTRING"),
        "Path should be LINESTRING, got: {path}"
    );
}

#[tokio::test]
async fn geof_distance_via_sparql() {
    // Test geof:distance function via SPARQL query
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_cities(&fluree, "geo:sparql").await;

    // SPARQL query using geof:distance
    let sparql = r"
        PREFIX ex: <http://example.org/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

        SELECT ?distance
        WHERE {
            ex:paris ex:location ?parisLoc .
            ex:london ex:location ?londonLoc .
            BIND(geof:distance(?parisLoc, ?londonLoc) AS ?distance)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let rows = result.as_array().expect("result should be array");
    assert_eq!(rows.len(), 1, "Should have exactly one result");
    // For single-variable select, the value is directly in rows[0] (not rows[0][0])
    let distance = rows[0].as_f64().expect("distance should be number");

    // Paris to London is approximately 343.5 km (343500 meters)
    assert!(
        (distance - 343_500.0).abs() < 5000.0,
        "Expected ~343.5km via SPARQL, got {distance} meters"
    );
}
