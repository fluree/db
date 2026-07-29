//! W3C vocabulary IRIs used by the rdf-tests manifests.
//!
//! The RDF syntax suites are typed in the `rdft:` namespace
//! (`http://www.w3.org/ns/rdftest#`), not `mf:` — that is the one structural
//! difference from the SPARQL manifests, whose test types are all `mf:`.

/// Test manifest vocabulary.
pub mod mf {
    pub const NS: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    pub const MANIFEST: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#Manifest";
    pub const ENTRIES: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries";
    pub const INCLUDE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include";
    pub const NAME: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#name";
    pub const ACTION: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action";
    pub const RESULT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result";
    /// Directory IRI the suite's action files are assumed to be published
    /// under. The suites use it to define what a relative IRI in a test file
    /// resolves against.
    pub const ASSUMED_TEST_BASE: &str =
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#assumedTestBase";
}

/// RDF test vocabulary — where the syntax-suite test *types* live.
pub mod rdft {
    pub const NS: &str = "http://www.w3.org/ns/rdftest#";
    pub const APPROVAL: &str = "http://www.w3.org/ns/rdftest#approval";
    pub const REJECTED: &str = "http://www.w3.org/ns/rdftest#Rejected";
    pub const WITHDRAWN: &str = "http://www.w3.org/ns/rdftest#Withdrawn";

    // Turtle
    pub const TURTLE_POSITIVE_SYNTAX: &str =
        "http://www.w3.org/ns/rdftest#TestTurtlePositiveSyntax";
    pub const TURTLE_NEGATIVE_SYNTAX: &str =
        "http://www.w3.org/ns/rdftest#TestTurtleNegativeSyntax";
    pub const TURTLE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTurtleEval";
    pub const TURTLE_NEGATIVE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTurtleNegativeEval";

    // N-Triples
    pub const NTRIPLES_POSITIVE_SYNTAX: &str =
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveSyntax";
    pub const NTRIPLES_NEGATIVE_SYNTAX: &str =
        "http://www.w3.org/ns/rdftest#TestNTriplesNegativeSyntax";
    // TriG
    pub const TRIG_POSITIVE_SYNTAX: &str = "http://www.w3.org/ns/rdftest#TestTrigPositiveSyntax";
    pub const TRIG_NEGATIVE_SYNTAX: &str = "http://www.w3.org/ns/rdftest#TestTrigNegativeSyntax";
    pub const TRIG_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTrigEval";
    pub const TRIG_NEGATIVE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTrigNegativeEval";

    /// RDF 1.2 canonicalization test: parse, serialize in canonical N-Triples,
    /// compare to the gold file byte-for-byte. No canonical writer exists yet.
    pub const NTRIPLES_POSITIVE_C14N: &str =
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveC14N";
}

/// RDF vocabulary.
pub mod rdf {
    pub const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    pub const FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
    pub const REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
    pub const NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
}

/// RDF Schema vocabulary.
pub mod rdfs {
    pub const COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
}
