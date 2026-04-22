//! R2RML Pattern Rewriting
//!
//! This module provides functionality to rewrite triple patterns to R2RML patterns
//! when the target graph is backed by an R2RML mapping.
//!
//! # Overview
//!
//! When a GRAPH pattern targets an R2RML graph source, the contained triple
//! patterns should be rewritten to R2RML scan patterns. This allows the query
//! engine to route the patterns to the R2RML operator which will scan the
//! underlying Iceberg tables.
//!
//! # Pattern Conversion
//!
//! Triple patterns are converted as follows:
//!
//! - `?s rdf:type ex:Class` → R2rmlPattern with class_filter="ex:Class"
//! - `?s ex:name ?o` → R2rmlPattern with predicate_filter="ex:name"
//! - `?s ?p ?o` (all variables) → R2rmlPattern with no filters
//!
//! # Limitations
//!
//! - Predicate variables (`?s ?p ?o`) result in full table scans
//! - Subject-bound patterns (`ex:subject ex:name ?o`) are not optimized
//! - Filter patterns are preserved and applied post-R2RML scan

use crate::ir::{Pattern, R2rmlPattern};
use crate::triple::{Ref, Term, TriplePattern};
use fluree_db_core::LedgerSnapshot;

/// Result of rewriting patterns for R2RML.
#[derive(Debug)]
pub struct R2rmlRewriteResult {
    /// Rewritten patterns
    pub patterns: Vec<Pattern>,
    /// Number of triple patterns converted to R2RML
    pub converted_count: usize,
    /// Number of patterns that couldn't be converted (preserved as-is)
    pub unconverted_count: usize,
}

/// Rewrite patterns for an R2RML graph source.
///
/// This function takes patterns from a GRAPH block and converts triple patterns
/// to R2RML patterns when possible. Other pattern types (Filter, Optional, etc.)
/// are processed recursively.
///
/// # Arguments
///
/// * `patterns` - The patterns to rewrite
/// * `graph_source_id` - The graph source alias (e.g., "airlines-gs:main")
/// * `snapshot` - Database for Sid-to-IRI conversion
///
/// # Returns
///
/// A result containing the rewritten patterns and conversion statistics.
pub fn rewrite_patterns_for_r2rml(
    patterns: &[Pattern],
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
) -> R2rmlRewriteResult {
    let mut result_patterns = Vec::with_capacity(patterns.len());
    let mut converted = 0;
    let mut unconverted = 0;

    for pattern in patterns {
        match pattern {
            Pattern::Triple(tp) => {
                if let Some(r2rml_pattern) = convert_triple_to_r2rml(tp, graph_source_id, snapshot)
                {
                    result_patterns.push(Pattern::R2rml(r2rml_pattern));
                    converted += 1;
                } else {
                    // Keep original pattern if conversion fails
                    result_patterns.push(pattern.clone());
                    unconverted += 1;
                }
            }
            Pattern::Optional(inner) => {
                let inner_result = rewrite_patterns_for_r2rml(inner, graph_source_id, snapshot);
                result_patterns.push(Pattern::Optional(inner_result.patterns));
                converted += inner_result.converted_count;
                unconverted += inner_result.unconverted_count;
            }
            Pattern::Union(branches) => {
                let mut new_branches = Vec::with_capacity(branches.len());
                for branch in branches {
                    let branch_result =
                        rewrite_patterns_for_r2rml(branch, graph_source_id, snapshot);
                    new_branches.push(branch_result.patterns);
                    converted += branch_result.converted_count;
                    unconverted += branch_result.unconverted_count;
                }
                result_patterns.push(Pattern::Union(new_branches));
            }
            Pattern::Minus(inner) => {
                let inner_result = rewrite_patterns_for_r2rml(inner, graph_source_id, snapshot);
                result_patterns.push(Pattern::Minus(inner_result.patterns));
                converted += inner_result.converted_count;
                unconverted += inner_result.unconverted_count;
            }
            Pattern::Exists(inner) => {
                let inner_result = rewrite_patterns_for_r2rml(inner, graph_source_id, snapshot);
                result_patterns.push(Pattern::Exists(inner_result.patterns));
                converted += inner_result.converted_count;
                unconverted += inner_result.unconverted_count;
            }
            Pattern::NotExists(inner) => {
                let inner_result = rewrite_patterns_for_r2rml(inner, graph_source_id, snapshot);
                result_patterns.push(Pattern::NotExists(inner_result.patterns));
                converted += inner_result.converted_count;
                unconverted += inner_result.unconverted_count;
            }
            Pattern::Service(sp) => {
                let inner_result =
                    rewrite_patterns_for_r2rml(&sp.patterns, graph_source_id, snapshot);
                result_patterns.push(Pattern::Service(crate::ir::ServicePattern::new(
                    sp.silent,
                    sp.endpoint.clone(),
                    inner_result.patterns,
                )));
                converted += inner_result.converted_count;
                unconverted += inner_result.unconverted_count;
            }
            // Preserve other patterns as-is
            Pattern::Filter(_)
            | Pattern::Bind { .. }
            | Pattern::Values { .. }
            | Pattern::Subquery(_)
            | Pattern::PropertyPath(_)
            | Pattern::IndexSearch(_)
            | Pattern::VectorSearch(_)
            | Pattern::R2rml(_)
            | Pattern::GeoSearch(_)
            | Pattern::S2Search(_)
            | Pattern::Graph { .. } => {
                result_patterns.push(pattern.clone());
            }
        }
    }

    R2rmlRewriteResult {
        patterns: result_patterns,
        converted_count: converted,
        unconverted_count: unconverted,
    }
}

/// Convert a triple pattern to an R2RML pattern.
///
/// Returns `None` if the pattern cannot be converted (e.g., subject is a literal).
pub fn convert_triple_to_r2rml(
    tp: &TriplePattern,
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
) -> Option<R2rmlPattern> {
    // Extract subject variable (must be a variable for basic R2RML support)
    let subject_var = match &tp.s {
        Ref::Var(v) => *v,
        Ref::Sid(_) | Ref::Iri(_) => {
            // Subject is bound - we could support this with a filter,
            // but for now we return None to preserve the original pattern.
            // The GraphOperator will handle this case differently.
            return None;
        }
    };

    // Check if this is an rdf:type pattern
    // Use Term::is_rdf_type() to handle both Term::Sid and Term::Iri
    let is_type_pattern = tp.p.is_rdf_type();

    if is_type_pattern {
        // rdf:type pattern: ?s rdf:type ex:Class
        // Extract the class IRI - handle both Term::Sid (decode) and Term::Iri (use directly)
        let class_filter = match &tp.o {
            Term::Sid(sid) => snapshot.decode_sid(sid),
            Term::Iri(iri) => Some(iri.to_string()),
            Term::Value(fluree_db_core::FlakeValue::Ref(sid)) => snapshot.decode_sid(sid),
            Term::Var(_) => None, // Class is a variable - no filter
            _ => None,
        };

        // For rdf:type, we create an R2RML pattern with class_filter and no object_var
        // (the type binding is implicit in the class_filter)
        let mut pattern = R2rmlPattern::new(graph_source_id, subject_var, None);
        if let Some(class_iri) = class_filter {
            pattern = pattern.with_class(class_iri);
        }
        return Some(pattern);
    }

    // Regular predicate pattern: ?s ex:name ?o
    // Extract predicate IRI filter - handle both Ref::Sid (decode) and Ref::Iri (use directly)
    let predicate_filter = match &tp.p {
        Ref::Sid(sid) => snapshot.decode_sid(sid),
        Ref::Iri(iri) => Some(iri.to_string()),
        Ref::Var(_) => None, // Predicate is variable - no filter
    };

    // Extract object variable
    // If object is bound (constant), don't rewrite - we can't currently push
    // object value constraints into the R2RML scan, so the original pattern
    // needs to be preserved for correct filtering.
    let object_var = match &tp.o {
        Term::Var(v) => Some(*v),
        Term::Sid(_) | Term::Iri(_) | Term::Value(_) => {
            // Object is bound - don't rewrite this pattern.
            // The R2RML scan cannot filter by object value, and rewriting
            // would drop the constraint, returning incorrect results.
            // Preserve the original triple pattern for normal evaluation.
            return None;
        }
    };

    let mut pattern = R2rmlPattern::new(graph_source_id, subject_var, object_var);
    if let Some(pred_iri) = predicate_filter {
        pattern = pattern.with_predicate(pred_iri);
    }

    Some(pattern)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triple::Ref;
    use crate::var_registry::VarId;
    use fluree_db_core::{is_rdf_type, Sid};
    use fluree_vocab::namespaces::RDF;

    #[test]
    fn test_rdf_type_detection() {
        // RDF namespace is code 3 in fluree-vocab
        let rdf_type_sid = Sid::new(RDF, "type");
        assert!(is_rdf_type(&rdf_type_sid));

        let not_type_sid = Sid::new(100, "name");
        assert!(!is_rdf_type(&not_type_sid));
    }

    #[test]
    fn test_convert_variable_only_pattern() {
        // ?s ?p ?o - all variables
        let tp = TriplePattern::new(Ref::Var(VarId(0)), Ref::Var(VarId(1)), Term::Var(VarId(2)));

        // We need to test without a real DB, so we test the logic manually
        // The pattern should have subject_var, object_var, but no filters
        assert!(tp.s.is_var());
        assert!(tp.p.is_var());
        assert!(tp.o.is_var());
    }
}
