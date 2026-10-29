//! Ontology and catalog tools for document extraction.
//!
//! An extraction model that reads documents against a caller's ontology sees
//! only part of it: each class's and property's label, its comment, one
//! parent class, the SHACL shapes that target a class and the IRI's local
//! name. It does not see domains, ranges, a second parent, SKOS annotations
//! or other OWL axioms, and a request whose ontology it cannot read at all
//! (no label anywhere, an OWL restriction) used to be refused.
//!
//! This crate mirrors that input policy locally, so a caller can see what the
//! model will and won't read before sending anything:
//!
//! - [`read`] parses an ontology file (Turtle or JSON-LD) into quads and
//!   splits it into ontology and catalog the way a request is split;
//! - [`policy`] is the input policy itself (`ontology-adapter-v1`): what is
//!   adapted (a SKOS label or definition standing in for a missing RDFS one,
//!   a label made from the IRI's local name, blank-node constructs dropped)
//!   and what is not shown;
//! - [`check`] turns that into a report: what would fail, what is read
//!   another way, what is not read, and advice;
//! - [`slice`] cuts a large vocabulary down to the classes reachable from a
//!   seed set;
//! - [`estimate`] approximates the serialized tokens an ontology and catalog
//!   cost per window;
//! - [`write`] serializes the result as Turtle or JSON-LD.

pub mod check;
pub mod estimate;
pub mod policy;
pub mod read;
pub mod slice;
pub mod vocab;
pub mod words;
pub mod write;

pub use check::{check, CheckOptions, Finding, Report};
pub use read::{parse, Parsed, Quad, RdfFormat, ReadError};
