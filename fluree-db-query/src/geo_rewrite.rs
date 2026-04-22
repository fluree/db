//! Query rewrite pass: geof:distance patterns → Pattern::GeoSearch
//!
//! Detects: Triple(?s, pred, ?loc) → Bind(?dist = geof:distance(?loc, WKT)) → Filter(?dist < radius)
//! Emits: Pattern::GeoSearch with index acceleration
//!
//! This optimization runs for both SPARQL and JSON-LD queries, enabling `geof:distance`
//! patterns to use the accelerated GeoPoint binary index path.

use crate::ir::{Expression, FilterValue, Function, GeoSearchCenter, GeoSearchPattern, Pattern};
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;
use fluree_db_core::geo::try_extract_point;
use fluree_db_core::Sid;
use std::collections::HashMap;

/// Rewrite geof:distance patterns to GeoSearch
///
/// Scans patterns for the SPARQL geo pattern:
/// - Triple(?place, pred, ?loc)
/// - Bind(?dist = geof:distance(?loc, WKT))
/// - Filter(?dist < radius)
///
/// And rewrites to Pattern::GeoSearch for index acceleration.
///
/// # Arguments
///
/// * `patterns` - The pattern list to rewrite
/// * `encode_iri` - Callback to encode IRI strings to Sids
pub fn rewrite_geo_patterns<F>(patterns: Vec<Pattern>, encode_iri: &F) -> Vec<Pattern>
where
    F: Fn(&str) -> Option<Sid>,
{
    rewrite_recursive(patterns, encode_iri)
}

fn rewrite_recursive<F>(patterns: Vec<Pattern>, encode_iri: &F) -> Vec<Pattern>
where
    F: Fn(&str) -> Option<Sid>,
{
    // 1. Recurse into nested scopes first
    let patterns: Vec<Pattern> = patterns
        .into_iter()
        .map(|p| match p {
            Pattern::Optional(inner) => Pattern::Optional(rewrite_recursive(inner, encode_iri)),
            Pattern::Union(branches) => Pattern::Union(
                branches
                    .into_iter()
                    .map(|b| rewrite_recursive(b, encode_iri))
                    .collect(),
            ),
            Pattern::Graph { name, patterns } => Pattern::Graph {
                name,
                patterns: rewrite_recursive(patterns, encode_iri),
            },
            Pattern::Minus(inner) => Pattern::Minus(rewrite_recursive(inner, encode_iri)),
            Pattern::Exists(inner) => Pattern::Exists(rewrite_recursive(inner, encode_iri)),
            Pattern::NotExists(inner) => Pattern::NotExists(rewrite_recursive(inner, encode_iri)),
            other => other,
        })
        .collect();

    // 2. Find candidates within THIS scope
    let candidates = find_geo_candidates(&patterns, encode_iri);

    // 3. Apply rewrites
    if candidates.is_empty() {
        patterns
    } else {
        apply_rewrites(patterns, candidates)
    }
}

/// A candidate for geo rewrite
struct GeoCandidate {
    triple_idx: usize,
    bind_idx: usize,
    filter_idx: usize,
    predicate: Sid,
    subject_var: VarId,
    distance_var: VarId,
    center: GeoSearchCenter,
    radius_meters: f64,
    keep_triple: bool, // true if loc_var is used elsewhere
}

fn find_geo_candidates<F>(patterns: &[Pattern], encode_iri: &F) -> Vec<GeoCandidate>
where
    F: Fn(&str) -> Option<Sid>,
{
    let mut candidates = Vec::new();

    // 1. Build map: object_var -> (idx, TriplePattern)
    let mut obj_var_to_triple: HashMap<VarId, (usize, &TriplePattern)> = HashMap::new();
    for (idx, p) in patterns.iter().enumerate() {
        if let Pattern::Triple(tp) = p {
            if let Term::Var(obj_var) = &tp.o {
                obj_var_to_triple.insert(*obj_var, (idx, tp));
            }
        }
    }

    // 2. Scan for Bind patterns with geof:distance
    for (bind_idx, p) in patterns.iter().enumerate() {
        if let Pattern::Bind {
            var: dist_var,
            expr,
        } = p
        {
            if let Some((loc_var, center)) = extract_distance_call(expr) {
                // 3. Check if loc_var traces to a Triple
                if let Some((triple_idx, tp)) = obj_var_to_triple.get(&loc_var) {
                    // 4. Extract predicate Sid
                    let predicate = match &tp.p {
                        Ref::Sid(sid) => Some(sid.clone()),
                        Ref::Iri(iri) => encode_iri(iri),
                        Ref::Var(_) => None, // Can't rewrite variable predicate
                    };

                    if let Some(predicate) = predicate {
                        // 5. Extract subject var
                        let subject_var = match &tp.s {
                            Ref::Var(v) => Some(*v),
                            _ => None, // Need variable subject for GeoSearch
                        };

                        if let Some(subject_var) = subject_var {
                            // 6. Find matching Filter
                            for (filter_idx, fp) in patterns.iter().enumerate() {
                                if let Pattern::Filter(filter_expr) = fp {
                                    if let Some(radius) =
                                        extract_lt_comparison(filter_expr, *dist_var)
                                    {
                                        // 7. Check loc_var liveness
                                        let keep_triple = is_var_used_elsewhere(
                                            patterns,
                                            loc_var,
                                            *triple_idx,
                                            bind_idx,
                                            filter_idx,
                                        );

                                        candidates.push(GeoCandidate {
                                            triple_idx: *triple_idx,
                                            bind_idx,
                                            filter_idx,
                                            predicate,
                                            subject_var,
                                            distance_var: *dist_var,
                                            center,
                                            radius_meters: radius,
                                            keep_triple,
                                        });
                                        break; // One filter per candidate
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    candidates
}

/// Extract geof:distance call parameters
///
/// Matches: FunctionCall { name: GeofDistance, args: [Var(loc), Const(wkt)] }
/// Returns: (loc_var, GeoSearchCenter::Const { lat, lng })
fn extract_distance_call(expr: &Expression) -> Option<(VarId, GeoSearchCenter)> {
    if let Expression::Call { func, args } = expr {
        // Match GeofDistance function
        if !matches!(func, Function::GeofDistance) {
            return None;
        }

        // Need exactly 2 args
        if args.len() != 2 {
            return None;
        }

        // First arg must be a variable (the location)
        let loc_var = match &args[0] {
            Expression::Var(v) => *v,
            _ => return None,
        };

        // Second arg must be a constant WKT string
        let wkt = match &args[1] {
            Expression::Const(FilterValue::String(s)) => s.as_str(),
            _ => return None,
        };

        // Parse WKT POINT to lat/lng
        let (lat, lng) = try_extract_point(wkt)?;

        Some((loc_var, GeoSearchCenter::Const { lat, lng }))
    } else {
        None
    }
}

/// Extract less-than comparison with a variable
///
/// Matches:
/// - Compare { op: Lt/Le, left: Var(target), right: Const(num) }
/// - Compare { op: Gt/Ge, left: Const(num), right: Var(target) }
///
/// Returns: radius in meters (must be non-negative)
fn extract_lt_comparison(expr: &Expression, target_var: VarId) -> Option<f64> {
    if let Expression::Call { func, args } = expr {
        if args.len() != 2 {
            return None;
        }
        match func {
            Function::Lt | Function::Le => {
                // ?dist < 5000 or ?dist <= 5000
                if matches!(&args[0], Expression::Var(v) if *v == target_var) {
                    return extract_numeric_const(&args[1]).filter(|&r| r >= 0.0);
                }
            }
            Function::Gt | Function::Ge => {
                // 5000 > ?dist or 5000 >= ?dist
                if matches!(&args[1], Expression::Var(v) if *v == target_var) {
                    return extract_numeric_const(&args[0]).filter(|&r| r >= 0.0);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract numeric constant from a Expression
fn extract_numeric_const(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::Const(FilterValue::Long(n)) => Some(*n as f64),
        Expression::Const(FilterValue::Double(n)) => Some(*n),
        _ => None,
    }
}

/// Check if a variable is used elsewhere in patterns
///
/// Returns true if the variable appears in any pattern except the ones being rewritten
fn is_var_used_elsewhere(
    patterns: &[Pattern],
    var: VarId,
    skip_triple: usize,
    skip_bind: usize,
    skip_filter: usize,
) -> bool {
    for (idx, p) in patterns.iter().enumerate() {
        if idx == skip_triple || idx == skip_bind || idx == skip_filter {
            continue;
        }
        if pattern_references_var(p, var) {
            return true;
        }
    }
    false
}

/// Check if a pattern references a variable
fn pattern_references_var(pattern: &Pattern, var: VarId) -> bool {
    match pattern {
        Pattern::Triple(tp) => {
            tp.s.as_var() == Some(var) || tp.p.as_var() == Some(var) || term_is_var(&tp.o, var)
        }
        Pattern::Filter(expr) => expr.variables().contains(&var),
        Pattern::Bind { var: v, expr } => *v == var || expr.variables().contains(&var),
        Pattern::Optional(inner)
        | Pattern::Minus(inner)
        | Pattern::Exists(inner)
        | Pattern::NotExists(inner) => inner.iter().any(|p| pattern_references_var(p, var)),
        Pattern::Union(branches) => branches
            .iter()
            .any(|branch| branch.iter().any(|p| pattern_references_var(p, var))),
        Pattern::Graph { patterns, name } => {
            let name_matches = matches!(name, crate::ir::GraphName::Var(v) if *v == var);
            name_matches || patterns.iter().any(|p| pattern_references_var(p, var))
        }
        Pattern::Values { vars, .. } => vars.contains(&var),
        Pattern::GeoSearch(gsp) => gsp.variables().contains(&var),
        Pattern::S2Search(s2p) => s2p.variables().contains(&var),
        Pattern::PropertyPath(pp) => pp.variables().contains(&var),
        Pattern::Subquery(sq) => sq.variables().contains(&var),
        Pattern::IndexSearch(isp) => isp.variables().contains(&var),
        Pattern::VectorSearch(vsp) => vsp.variables().contains(&var),
        Pattern::Service(sp) => sp.variables().contains(&var),
        Pattern::R2rml(r2rml) => r2rml.variables().contains(&var),
    }
}

fn term_is_var(term: &Term, var: VarId) -> bool {
    matches!(term, Term::Var(v) if *v == var)
}

/// Apply rewrites to the pattern list
fn apply_rewrites(patterns: Vec<Pattern>, mut candidates: Vec<GeoCandidate>) -> Vec<Pattern> {
    // Sort by index descending so we can remove from the back first
    candidates.sort_by(|a, b| {
        let max_a = a.triple_idx.max(a.bind_idx).max(a.filter_idx);
        let max_b = b.triple_idx.max(b.bind_idx).max(b.filter_idx);
        max_b.cmp(&max_a)
    });

    let mut result = patterns;

    for candidate in candidates {
        // Collect indices to remove (in descending order)
        let mut to_remove = vec![candidate.filter_idx, candidate.bind_idx];
        if !candidate.keep_triple {
            to_remove.push(candidate.triple_idx);
        }
        to_remove.sort_by(|a, b| b.cmp(a)); // Descending

        // Remove patterns
        for idx in &to_remove {
            result.remove(*idx);
        }

        // Build GeoSearch pattern
        let geo_search = Pattern::GeoSearch(
            GeoSearchPattern::new(
                candidate.predicate.clone(),
                candidate.center,
                candidate.radius_meters,
                candidate.subject_var,
            )
            .with_distance_var(candidate.distance_var),
        );

        // Insert at the position of the original triple
        let insert_pos = if candidate.keep_triple {
            // Insert before the triple
            candidate.triple_idx
        } else {
            // Triple was removed, adjust for removed patterns before this position
            let removed_before = to_remove
                .iter()
                .filter(|&&idx| idx < candidate.triple_idx)
                .count();
            candidate.triple_idx - removed_before
        };

        result.insert(insert_pos, geo_search);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var_registry::VarRegistry;

    fn mock_encoder(iri: &str) -> Option<Sid> {
        match iri {
            "http://example.org/location" => Some(Sid::new(1, "location")),
            "http://example.org/name" => Some(Sid::new(2, "name")),
            _ => None,
        }
    }

    #[test]
    fn test_extract_distance_call() {
        let mut vars = VarRegistry::new();
        let loc_var = vars.get_or_insert("?loc");

        // geof:distance(?loc, "POINT(2.35 48.86)")
        let expr = Expression::Call {
            func: Function::GeofDistance,
            args: vec![
                Expression::Var(loc_var),
                Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
            ],
        };

        let result = extract_distance_call(&expr);
        assert!(result.is_some());
        let (v, center) = result.unwrap();
        assert_eq!(v, loc_var);
        match center {
            GeoSearchCenter::Const { lat, lng } => {
                assert!((lat - 48.86).abs() < 0.001);
                assert!((lng - 2.35).abs() < 0.001);
            }
            _ => panic!("Expected Const center"),
        }
    }

    #[test]
    fn test_extract_lt_comparison() {
        let mut vars = VarRegistry::new();
        let dist_var = vars.get_or_insert("?dist");
        let other_var = vars.get_or_insert("?other");

        // ?dist < 1000
        let expr = Expression::lt(
            Expression::Var(dist_var),
            Expression::Const(FilterValue::Long(1000)),
        );
        assert_eq!(extract_lt_comparison(&expr, dist_var), Some(1000.0));
        assert_eq!(extract_lt_comparison(&expr, other_var), None);

        // ?dist <= 500.5
        let expr2 = Expression::le(
            Expression::Var(dist_var),
            Expression::Const(FilterValue::Double(500.5)),
        );
        assert_eq!(extract_lt_comparison(&expr2, dist_var), Some(500.5));

        // 2000 > ?dist
        let expr3 = Expression::gt(
            Expression::Const(FilterValue::Long(2000)),
            Expression::Var(dist_var),
        );
        assert_eq!(extract_lt_comparison(&expr3, dist_var), Some(2000.0));

        // Negative radius should fail
        let expr4 = Expression::lt(
            Expression::Var(dist_var),
            Expression::Const(FilterValue::Long(-100)),
        );
        assert_eq!(extract_lt_comparison(&expr4, dist_var), None);
    }

    #[test]
    fn test_basic_rewrite() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        let pred_sid = Sid::new(1, "location");

        // Build: Triple(?place, location, ?loc), Bind(?dist = distance), Filter(?dist < 1000)
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Sid(pred_sid.clone()),
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
        ];

        let result = rewrite_geo_patterns(patterns, &mock_encoder);

        // Should have one GeoSearch pattern
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::GeoSearch(gsp) => {
                assert_eq!(gsp.predicate, pred_sid);
                assert_eq!(gsp.subject_var, place_var);
                assert_eq!(gsp.distance_var, Some(dist_var));
                assert!((gsp.radius_meters - 1000.0).abs() < 0.001);
            }
            _ => panic!("Expected GeoSearch pattern"),
        }
    }

    #[test]
    fn test_loc_var_preserved_when_used() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        let pred_sid = Sid::new(1, "location");

        // Build patterns where ?loc is also used in another pattern
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Sid(pred_sid.clone()),
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
            // ?loc is used here too - Triple should be kept
            Pattern::Filter(Expression::eq(
                Expression::Var(loc_var),
                Expression::Const(FilterValue::String("test".to_string())),
            )),
        ];

        let result = rewrite_geo_patterns(patterns, &mock_encoder);

        // Should have GeoSearch + Triple + extra Filter (Bind and original Filter removed)
        assert_eq!(result.len(), 3);

        // First should be GeoSearch
        assert!(matches!(&result[0], Pattern::GeoSearch(_)));

        // Second should be the original Triple (kept because ?loc is used)
        assert!(matches!(&result[1], Pattern::Triple(_)));

        // Third should be the extra filter
        assert!(matches!(&result[2], Pattern::Filter(_)));
    }

    #[test]
    fn test_no_filter_no_rewrite() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        let pred_sid = Sid::new(1, "location");

        // Build: Triple + Bind without Filter
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Sid(pred_sid.clone()),
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
        ];

        let result = rewrite_geo_patterns(patterns.clone(), &mock_encoder);

        // Should be unchanged - no Filter means no rewrite
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0], Pattern::Triple(_)));
        assert!(matches!(&result[1], Pattern::Bind { .. }));
    }

    #[test]
    fn test_variable_predicate_skipped() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let pred_var = vars.get_or_insert("?pred");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        // Build with variable predicate
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Var(pred_var), // Variable predicate!
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
        ];

        let result = rewrite_geo_patterns(patterns.clone(), &mock_encoder);

        // Should be unchanged - variable predicate can't be rewritten
        assert_eq!(result.len(), 3);
        assert!(matches!(&result[0], Pattern::Triple(_)));
    }

    #[test]
    fn test_iri_predicate_encoded() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        // Build with IRI predicate
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Iri("http://example.org/location".into()), // IRI predicate
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
        ];

        let result = rewrite_geo_patterns(patterns, &mock_encoder);

        // Should be rewritten - IRI gets encoded via mock_encoder
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Pattern::GeoSearch(_)));
    }

    #[test]
    fn test_unknown_iri_skipped() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        // Build with unknown IRI predicate
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Iri("http://unknown.org/prop".into()), // Unknown IRI
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
        ];

        let result = rewrite_geo_patterns(patterns.clone(), &mock_encoder);

        // Should be unchanged - unknown IRI can't be encoded
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_nested_in_optional() {
        let mut vars = VarRegistry::new();
        let place_var = vars.get_or_insert("?place");
        let loc_var = vars.get_or_insert("?loc");
        let dist_var = vars.get_or_insert("?dist");

        let pred_sid = Sid::new(1, "location");

        // Build pattern inside Optional
        let inner_patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(place_var),
                Ref::Sid(pred_sid.clone()),
                Term::Var(loc_var),
            )),
            Pattern::Bind {
                var: dist_var,
                expr: Expression::Call {
                    func: Function::GeofDistance,
                    args: vec![
                        Expression::Var(loc_var),
                        Expression::Const(FilterValue::String("POINT(2.35 48.86)".to_string())),
                    ],
                },
            },
            Pattern::Filter(Expression::lt(
                Expression::Var(dist_var),
                Expression::Const(FilterValue::Long(1000)),
            )),
        ];

        let patterns = vec![Pattern::Optional(inner_patterns)];

        let result = rewrite_geo_patterns(patterns, &mock_encoder);

        // Should have one Optional containing one GeoSearch
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Optional(inner) => {
                assert_eq!(inner.len(), 1);
                assert!(matches!(&inner[0], Pattern::GeoSearch(_)));
            }
            _ => panic!("Expected Optional pattern"),
        }
    }
}
