//! W3C test manifest vocabulary constants.
//!
//! These are the RDF predicates and types used in the official W3C SPARQL
//! test manifest files (manifest.ttl).

/// Test manifest vocabulary (mf:)
pub mod mf {
    pub const NS: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    pub const MANIFEST: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#Manifest";
    pub const INCLUDE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include";
    pub const ENTRIES: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries";
    pub const NAME: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#name";
    pub const ACTION: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action";
    pub const RESULT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result";

    // Test types
    pub const POSITIVE_SYNTAX_TEST_11: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11";
    pub const NEGATIVE_SYNTAX_TEST_11: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest11";
    pub const POSITIVE_SYNTAX_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest";
    pub const NEGATIVE_SYNTAX_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest";
    pub const QUERY_EVALUATION_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest";
    pub const POSITIVE_UPDATE_SYNTAX_TEST_11: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11";
    pub const NEGATIVE_UPDATE_SYNTAX_TEST_11: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest11";
    pub const UPDATE_EVALUATION_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#UpdateEvaluationTest";
    pub const CSV_RESULT_FORMAT_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#CSVResultFormatTest";
    pub const SERVICE_DESCRIPTION_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#ServiceDescriptionTest";
    pub const PROTOCOL_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#ProtocolTest";
    pub const GRAPH_STORE_PROTOCOL_TEST: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#GraphStoreProtocolTest";
}

/// SPARQL query test vocabulary (qt:)
pub mod qt {
    pub const QUERY: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#query";
    pub const DATA: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#data";
    pub const GRAPH_DATA: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#graphData";
    pub const SERVICE_DATA: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-query#serviceData";
    pub const ENDPOINT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#endpoint";
}

/// SPARQL update test vocabulary (ut:)
pub mod ut {
    pub const DATA: &str = "http://www.w3.org/2009/sparql/tests/test-update#data";
    pub const GRAPH_DATA: &str = "http://www.w3.org/2009/sparql/tests/test-update#graphData";
    pub const GRAPH: &str = "http://www.w3.org/2009/sparql/tests/test-update#graph";
    pub const REQUEST: &str = "http://www.w3.org/2009/sparql/tests/test-update#request";
    pub const RESULT: &str = "http://www.w3.org/2009/sparql/tests/test-update#result";
}

/// SPARQL result set vocabulary (rs:)
pub mod rs {
    pub const RESULT_SET: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#ResultSet";
    pub const RESULT_VARIABLE: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/result-set#resultVariable";
    pub const SOLUTION: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#solution";
    pub const BINDING: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#binding";
    pub const VALUE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#value";
    pub const VARIABLE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#variable";
    pub const INDEX: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#index";
    pub const BOOLEAN: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#boolean";
}

/// RDF test vocabulary (rdft: / dawgt:)
pub mod rdft {
    pub const APPROVAL: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#approval";
    pub const REJECTED: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#Rejected";
    pub const APPROVED: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#Approved";
}

/// Standard RDF vocabulary
pub mod rdf {
    pub const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
}

/// RDFS vocabulary
pub mod rdfs {
    pub const COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
    pub const LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
}
