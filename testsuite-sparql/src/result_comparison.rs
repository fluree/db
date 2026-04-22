//! Isomorphic comparison of SPARQL query results.
//!
//! Handles blank node equivalence (different labels, same structure)
//! and unordered solution multiset matching for SELECT queries.

use std::collections::HashMap;

use crate::result_format::{RdfTerm, SparqlResults, Triple};

/// Compare two SPARQL result sets for isomorphism.
///
/// - **Solutions**: Same variables, same number of solutions, and each expected
///   solution has a matching actual solution (unordered) with consistent blank
///   node mapping.
/// - **Boolean**: Direct equality.
pub fn are_results_isomorphic(expected: &SparqlResults, actual: &SparqlResults) -> bool {
    match (expected, actual) {
        (
            SparqlResults::Solutions {
                variables: exp_vars,
                solutions: exp_solutions,
            },
            SparqlResults::Solutions {
                variables: act_vars,
                solutions: act_solutions,
            },
        ) => {
            // Variables must match (as sets — order doesn't matter)
            let mut exp_sorted = exp_vars.clone();
            exp_sorted.sort();
            let mut act_sorted = act_vars.clone();
            act_sorted.sort();
            if exp_sorted != act_sorted {
                return false;
            }

            // Same number of solutions
            if exp_solutions.len() != act_solutions.len() {
                return false;
            }

            // Try to find a consistent blank node mapping that matches all solutions.
            // Use a greedy approach with backtracking.
            let mut bnode_map: HashMap<String, String> = HashMap::new();
            let mut used: Vec<bool> = vec![false; act_solutions.len()];

            match_solutions(exp_solutions, act_solutions, 0, &mut bnode_map, &mut used)
        }
        (SparqlResults::Boolean(exp), SparqlResults::Boolean(act)) => exp == act,
        (SparqlResults::Graph(exp), SparqlResults::Graph(act)) => are_graphs_isomorphic(exp, act),
        _ => false, // Type mismatch
    }
}

/// Recursively match expected solutions to actual solutions with backtracking.
///
/// For each expected solution, tries to find an unused actual solution that
/// matches (with consistent blank node mapping).
fn match_solutions(
    expected: &[HashMap<String, RdfTerm>],
    actual: &[HashMap<String, RdfTerm>],
    exp_idx: usize,
    bnode_map: &mut HashMap<String, String>,
    used: &mut [bool],
) -> bool {
    // All expected solutions matched successfully
    if exp_idx >= expected.len() {
        return true;
    }

    let exp_solution = &expected[exp_idx];

    for act_idx in 0..actual.len() {
        if used[act_idx] {
            continue;
        }

        // Try matching this pair
        let saved_map = bnode_map.clone();
        if solution_matches(exp_solution, &actual[act_idx], bnode_map) {
            used[act_idx] = true;
            if match_solutions(expected, actual, exp_idx + 1, bnode_map, used) {
                return true;
            }
            used[act_idx] = false;
        }
        // Backtrack: restore the bnode map
        *bnode_map = saved_map;
    }

    false
}

/// Check if two solutions match, updating the blank node mapping.
fn solution_matches(
    expected: &HashMap<String, RdfTerm>,
    actual: &HashMap<String, RdfTerm>,
    bnode_map: &mut HashMap<String, String>,
) -> bool {
    // Every binding in expected must exist and match in actual (and vice versa)
    if expected.len() != actual.len() {
        return false;
    }

    for (var, exp_term) in expected {
        match actual.get(var) {
            Some(act_term) => {
                if !terms_match(exp_term, act_term, bnode_map) {
                    return false;
                }
            }
            None => return false,
        }
    }

    true
}

/// Check if two RDF terms match, handling blank node isomorphism.
fn terms_match(
    expected: &RdfTerm,
    actual: &RdfTerm,
    bnode_map: &mut HashMap<String, String>,
) -> bool {
    match (expected, actual) {
        (RdfTerm::BlankNode(exp_label), RdfTerm::BlankNode(act_label)) => {
            // Check if we've already mapped this expected bnode
            if let Some(mapped) = bnode_map.get(exp_label) {
                mapped == act_label
            } else {
                // Check that the actual label isn't already mapped to a different expected label
                let already_mapped = bnode_map.values().any(|v| v == act_label);
                if already_mapped {
                    return false;
                }
                bnode_map.insert(exp_label.clone(), act_label.clone());
                true
            }
        }
        (
            RdfTerm::Literal {
                value: ev,
                datatype: ed,
                language: el,
            },
            RdfTerm::Literal {
                value: av,
                datatype: ad,
                language: al,
            },
        ) => {
            if el != al {
                return false;
            }
            let ed_norm = normalize_datatype(ed);
            let ad_norm = normalize_datatype(ad);
            if ed_norm != ad_norm {
                return false;
            }
            // Lexical match (fast path)
            if ev == av {
                return true;
            }
            // For numeric/boolean datatypes, compare by value (W3C allows non-canonical forms)
            if let Some(dt) = ed_norm {
                if is_numeric_datatype(dt) {
                    return numeric_values_equal(ev, av, dt);
                }
                if dt == "http://www.w3.org/2001/XMLSchema#boolean" {
                    return boolean_values_equal(ev, av);
                }
            }
            false
        }
        (RdfTerm::Iri(e), RdfTerm::Iri(a)) => e == a,
        _ => false, // Type mismatch
    }
}

/// Normalize datatype: treat `None` and `Some(xsd:string)` as equivalent.
fn normalize_datatype(dt: &Option<String>) -> Option<&str> {
    match dt.as_deref() {
        None | Some("http://www.w3.org/2001/XMLSchema#string") => None,
        Some(s) => Some(s),
    }
}

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn is_numeric_datatype(dt: &str) -> bool {
    matches!(
        dt.strip_prefix(XSD),
        Some(
            "integer"
                | "decimal"
                | "float"
                | "double"
                | "long"
                | "int"
                | "short"
                | "byte"
                | "nonNegativeInteger"
                | "positiveInteger"
                | "nonPositiveInteger"
                | "negativeInteger"
                | "unsignedLong"
                | "unsignedInt"
                | "unsignedShort"
                | "unsignedByte"
        )
    )
}

/// Compare two boolean literal values by parsed value rather than lexical form.
///
/// XSD boolean has lexical forms: "true", "1" (both true), "false", "0" (both false).
fn boolean_values_equal(a: &str, b: &str) -> bool {
    let parse_bool = |s: &str| -> Option<bool> {
        match s {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        }
    };
    match (parse_bool(a), parse_bool(b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Compare two numeric literal values by parsed value rather than lexical form.
fn numeric_values_equal(a: &str, b: &str, datatype: &str) -> bool {
    let local = datatype.strip_prefix(XSD).unwrap_or(datatype);
    match local {
        "integer" | "long" | "int" | "short" | "byte" | "nonNegativeInteger"
        | "positiveInteger" | "nonPositiveInteger" | "negativeInteger" | "unsignedLong"
        | "unsignedInt" | "unsignedShort" | "unsignedByte" => {
            match (a.parse::<i128>().ok(), b.parse::<i128>().ok()) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }
        "float" | "double" => {
            let (pa, pb) = (a.parse::<f64>(), b.parse::<f64>());
            match (pa, pb) {
                (Ok(fa), Ok(fb)) => {
                    if fa.is_nan() && fb.is_nan() {
                        true
                    } else {
                        fa == fb
                    }
                }
                _ => false,
            }
        }
        "decimal" => {
            // Compare decimal values as rational numbers to avoid f64 precision loss.
            // Covers non-canonical forms like "0" == "0.0", "1" == "1.0", etc.
            match (parse_decimal(a), parse_decimal(b)) {
                (Some((an, ad)), Some((bn, bd))) => {
                    // Cross-multiply to compare: a_num/a_den == b_num/b_den
                    an * bd == bn * ad
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// Parse a decimal string into (numerator, denominator) as i128 values.
///
/// Handles forms like "0", "0.0", "1.50", "-3.14" without floating-point
/// precision loss. Returns None for unparseable strings.
fn parse_decimal(s: &str) -> Option<(i128, i128)> {
    let s = s.trim();
    if let Some(dot_pos) = s.find('.') {
        let frac_digits = s.len() - dot_pos - 1;
        let without_dot: String = s.chars().filter(|c| *c != '.').collect();
        let numerator = without_dot.parse::<i128>().ok()?;
        let denominator = 10i128.checked_pow(frac_digits as u32)?;
        Some((numerator, denominator))
    } else {
        let numerator = s.parse::<i128>().ok()?;
        Some((numerator, 1))
    }
}

// ---------------------------------------------------------------------------
// Graph isomorphism (CONSTRUCT / DESCRIBE)
// ---------------------------------------------------------------------------

/// Check if two RDF graphs (triple sets) are isomorphic.
///
/// Two graphs are isomorphic if there exists a bijective mapping of blank node
/// labels such that the sets of triples become equal.
fn are_graphs_isomorphic(expected: &[Triple], actual: &[Triple]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }

    // Fast path: no blank nodes in either graph — just sort and compare.
    let has_bnodes = |triples: &[Triple]| {
        triples.iter().any(|t| {
            matches!(t.subject, RdfTerm::BlankNode(_))
                || matches!(t.predicate, RdfTerm::BlankNode(_))
                || matches!(t.object, RdfTerm::BlankNode(_))
        })
    };

    if !has_bnodes(expected) && !has_bnodes(actual) {
        let mut exp_sorted: Vec<_> = expected.to_vec();
        let mut act_sorted: Vec<_> = actual.to_vec();
        exp_sorted.sort_by(triple_sort_key);
        act_sorted.sort_by(triple_sort_key);
        return exp_sorted == act_sorted;
    }

    // Slow path: backtracking search for a consistent blank node mapping.
    let mut bnode_map: HashMap<String, String> = HashMap::new();
    let mut used: Vec<bool> = vec![false; actual.len()];
    match_triples(expected, actual, 0, &mut bnode_map, &mut used)
}

/// Deterministic sort key for triples (for the bnode-free fast path).
fn triple_sort_key(a: &Triple, b: &Triple) -> std::cmp::Ordering {
    rdf_term_sort_key(&a.subject, &b.subject)
        .then_with(|| rdf_term_sort_key(&a.predicate, &b.predicate))
        .then_with(|| rdf_term_sort_key(&a.object, &b.object))
}

fn rdf_term_sort_key(a: &RdfTerm, b: &RdfTerm) -> std::cmp::Ordering {
    let discriminant = |t: &RdfTerm| -> u8 {
        match t {
            RdfTerm::BlankNode(_) => 0,
            RdfTerm::Iri(_) => 1,
            RdfTerm::Literal { .. } => 2,
        }
    };
    discriminant(a).cmp(&discriminant(b)).then_with(|| {
        match (a, b) {
            (RdfTerm::Iri(a), RdfTerm::Iri(b)) => a.cmp(b),
            (RdfTerm::BlankNode(a), RdfTerm::BlankNode(b)) => a.cmp(b),
            (
                RdfTerm::Literal {
                    value: av,
                    datatype: ad,
                    language: al,
                },
                RdfTerm::Literal {
                    value: bv,
                    datatype: bd,
                    language: bl,
                },
            ) => av.cmp(bv).then_with(|| ad.cmp(bd)).then_with(|| al.cmp(bl)),
            _ => std::cmp::Ordering::Equal, // different discriminants already handled
        }
    })
}

/// Recursively match expected triples to actual triples with backtracking
/// for blank node isomorphism.
fn match_triples(
    expected: &[Triple],
    actual: &[Triple],
    exp_idx: usize,
    bnode_map: &mut HashMap<String, String>,
    used: &mut [bool],
) -> bool {
    if exp_idx >= expected.len() {
        return true;
    }

    let exp_triple = &expected[exp_idx];

    for act_idx in 0..actual.len() {
        if used[act_idx] {
            continue;
        }

        let saved_map = bnode_map.clone();
        if triple_matches(exp_triple, &actual[act_idx], bnode_map) {
            used[act_idx] = true;
            if match_triples(expected, actual, exp_idx + 1, bnode_map, used) {
                return true;
            }
            used[act_idx] = false;
        }
        *bnode_map = saved_map;
    }

    false
}

/// Check if two triples match under the current blank node mapping.
fn triple_matches(
    expected: &Triple,
    actual: &Triple,
    bnode_map: &mut HashMap<String, String>,
) -> bool {
    terms_match(&expected.subject, &actual.subject, bnode_map)
        && terms_match(&expected.predicate, &actual.predicate, bnode_map)
        && terms_match(&expected.object, &actual.object, bnode_map)
}

/// Format a diff between expected and actual results for error messages.
pub fn format_results_diff(expected: &SparqlResults, actual: &SparqlResults) -> String {
    match (expected, actual) {
        (
            SparqlResults::Solutions {
                variables: exp_vars,
                solutions: exp_solutions,
            },
            SparqlResults::Solutions {
                variables: act_vars,
                solutions: act_solutions,
            },
        ) => {
            let mut msg = String::new();
            msg.push_str(&format!(
                "Expected vars: {:?}\nActual vars:   {:?}\n",
                exp_vars, act_vars,
            ));
            msg.push_str(&format!(
                "Expected {} solution(s), got {}\n",
                exp_solutions.len(),
                act_solutions.len(),
            ));

            // Show first few expected vs actual solutions
            let show_count = 5;
            if !exp_solutions.is_empty() {
                msg.push_str("\nExpected (first few):\n");
                for (i, sol) in exp_solutions.iter().take(show_count).enumerate() {
                    msg.push_str(&format!("  [{i}]: {sol:?}\n"));
                }
            }
            if !act_solutions.is_empty() {
                msg.push_str("\nActual (first few):\n");
                for (i, sol) in act_solutions.iter().take(show_count).enumerate() {
                    msg.push_str(&format!("  [{i}]: {sol:?}\n"));
                }
            }
            msg
        }
        (SparqlResults::Boolean(exp), SparqlResults::Boolean(act)) => {
            format!("Expected: {exp}, Actual: {act}")
        }
        (SparqlResults::Graph(exp), SparqlResults::Graph(act)) => {
            let mut msg = String::new();
            msg.push_str(&format!(
                "Expected {} triple(s), got {}\n",
                exp.len(),
                act.len(),
            ));

            let show_count = 10;
            if !exp.is_empty() {
                msg.push_str("\nExpected (first few):\n");
                for (i, t) in exp.iter().take(show_count).enumerate() {
                    msg.push_str(&format!(
                        "  [{i}]: {:?} {:?} {:?}\n",
                        t.subject, t.predicate, t.object
                    ));
                }
            }
            if !act.is_empty() {
                msg.push_str("\nActual (first few):\n");
                for (i, t) in act.iter().take(show_count).enumerate() {
                    msg.push_str(&format!(
                        "  [{i}]: {:?} {:?} {:?}\n",
                        t.subject, t.predicate, t.object
                    ));
                }
            }
            msg
        }
        _ => format!(
            "Result type mismatch: expected {}, got {}",
            result_type_name(expected),
            result_type_name(actual),
        ),
    }
}

fn result_type_name(r: &SparqlResults) -> &'static str {
    match r {
        SparqlResults::Solutions { .. } => "Solutions",
        SparqlResults::Boolean(_) => "Boolean",
        SparqlResults::Graph(_) => "Graph",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> RdfTerm {
        RdfTerm::Iri(s.to_string())
    }

    fn lit(s: &str) -> RdfTerm {
        RdfTerm::Literal {
            value: s.to_string(),
            datatype: None,
            language: None,
        }
    }

    fn bnode(s: &str) -> RdfTerm {
        RdfTerm::BlankNode(s.to_string())
    }

    #[test]
    fn test_identical_solutions() {
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), iri("http://example.org/a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), iri("http://example.org/a"));
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_unordered_solutions() {
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("a"));
                    m
                },
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("b"));
                    m
                },
            ],
        };
        // Same solutions in reverse order
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("b"));
                    m
                },
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("a"));
                    m
                },
            ],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_blank_node_isomorphism() {
        // Expected: ?x = _:a, ?y = _:a (same bnode)
        // Actual:   ?x = _:z, ?y = _:z (same bnode, different label)
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("a"));
                m.insert("y".into(), bnode("a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("z"));
                m.insert("y".into(), bnode("z"));
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_blank_node_different_structure() {
        // Expected: ?x = _:a, ?y = _:a (same bnode)
        // Actual:   ?x = _:z, ?y = _:w (different bnodes — not isomorphic)
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("a"));
                m.insert("y".into(), bnode("a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("z"));
                m.insert("y".into(), bnode("w"));
                m
            }],
        };
        assert!(!are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_boolean_match() {
        assert!(are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Boolean(true)
        ));
        assert!(!are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Boolean(false)
        ));
    }

    #[test]
    fn test_type_mismatch() {
        assert!(!are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Solutions {
                variables: vec![],
                solutions: vec![],
            }
        ));
    }

    #[test]
    fn test_literal_datatype_normalization() {
        // xsd:string and no datatype should be equivalent
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert(
                    "x".into(),
                    RdfTerm::Literal {
                        value: "hello".into(),
                        datatype: None,
                        language: None,
                    },
                );
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert(
                    "x".into(),
                    RdfTerm::Literal {
                        value: "hello".into(),
                        datatype: Some("http://www.w3.org/2001/XMLSchema#string".to_string()),
                        language: None,
                    },
                );
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }
}
