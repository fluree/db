//! Graph isomorphism over `fluree_graph_ir::Triple`.
//!
//! Lifted from `testsuite-sparql/src/result_comparison.rs` (`are_graphs_isomorphic`,
//! `terms_match`, `match_triples`) and retargeted from that harness's local
//! `RdfTerm` to the shared IR term, with three changes the eval suites need:
//!
//! 1. **Set semantics.** An RDF graph is a set; a document may state the same
//!    triple twice. Both sides are deduplicated before the cardinality check,
//!    which the SPARQL version (comparing CONSTRUCT *results*) did not need.
//! 2. **Ground/blank split.** Triples with no blank node are compared directly
//!    as sets, and only the blank-node-bearing remainder enters the
//!    backtracking search. Turtle eval tests are mostly ground, so this turns
//!    the common case into a sort-and-compare.
//! 3. **O(1) injectivity.** The bijection is tracked with a reverse map
//!    instead of a linear scan over the forward map's values.
//! 4. **Case-insensitive language tags.** RDF 1.1 §3.3 makes a literal's
//!    language tag case-insensitive for term equality — `"chat"@FR` and
//!    `"chat"@fr` are the SAME term. The IR's `Term` compares tags
//!    case-sensitively, so the checker lowercases them on the way in. This
//!    happens in the CHECKER ONLY: the IR is untouched, because the parser's
//!    job is to preserve what the document said and only the comparison needs
//!    the equivalence. A gold `.nt` file may legitimately spell a tag in a
//!    different case than the `.ttl` it is checked against, and that must not
//!    be a failure.

use std::collections::{HashMap, HashSet};

use fluree_graph_ir::{Dataset, Graph, Term, Triple};

/// Whether two graphs are isomorphic: equal after some bijective renaming of
/// blank node labels.
pub fn are_graphs_isomorphic(expected: &[Triple], actual: &[Triple]) -> bool {
    let expected = normalize(expected);
    let actual = normalize(actual);

    if expected.len() != actual.len() {
        return false;
    }

    let (exp_ground, exp_blank) = split_ground(&expected);
    let (act_ground, act_blank) = split_ground(&actual);

    // Ground triples admit no renaming, so they must match exactly.
    if exp_ground != act_ground {
        return false;
    }
    if exp_blank.len() != act_blank.len() {
        return false;
    }
    if exp_blank.is_empty() {
        return true;
    }

    // Match the most-constrained triples first: each step should share a
    // blank node with something already mapped, so a wrong guess is refuted
    // immediately instead of at the end of the search.
    let ordered = order_by_connectivity(&exp_blank);

    let mut forward: HashMap<Term, Term> = HashMap::new();
    let mut reverse: HashMap<Term, Term> = HashMap::new();
    let mut used = vec![false; act_blank.len()];
    match_triples(
        &ordered,
        &act_blank,
        0,
        &mut forward,
        &mut reverse,
        &mut used,
    )
}

/// Sorted, deduplicated triples — the set the graph denotes.
///
/// `list_index` is cleared: it is Fluree ingest metadata on the edge, and two
/// graphs that denote the same RDF must not differ by it. Under
/// [`fluree_graph_turtle::CollectionStyle::Spine`] nothing sets it anyway, so
/// this only matters if the informational default-mode run ever compares.
///
/// Language tags are lowercased here (RDF 1.1 §3.3) — see the module docs.
/// Doing it during normalization rather than in `terms_match` is what makes it
/// apply to the ground-triple set comparison too, which uses plain `Term`
/// equality.
fn normalize(triples: &[Triple]) -> Vec<Triple> {
    let mut out: Vec<Triple> = triples
        .iter()
        .map(|t| {
            Triple::new(
                fold_language(&t.s),
                fold_language(&t.p),
                fold_language(&t.o),
            )
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Lowercase a literal's language tag; every other term is returned as-is.
fn fold_language(term: &Term) -> Term {
    match term {
        Term::Literal {
            value,
            datatype,
            language: Some(tag),
        } => Term::Literal {
            value: value.clone(),
            datatype: datatype.clone(),
            language: Some(tag.to_lowercase().into()),
        },
        other => other.clone(),
    }
}

fn has_blank(t: &Triple) -> bool {
    t.s.is_blank() || t.p.is_blank() || t.o.is_blank()
}

fn split_ground(triples: &[Triple]) -> (Vec<Triple>, Vec<Triple>) {
    triples.iter().cloned().partition(|t| !has_blank(t))
}

fn blanks_of(t: &Triple) -> Vec<&Term> {
    [&t.s, &t.p, &t.o]
        .into_iter()
        .filter(|term| term.is_blank())
        .collect()
}

/// Greedily order triples so each one shares a blank node with an earlier one
/// where possible.
fn order_by_connectivity(triples: &[Triple]) -> Vec<Triple> {
    let mut remaining: Vec<Triple> = triples.to_vec();
    let mut ordered = Vec::with_capacity(remaining.len());
    let mut seen: HashSet<Term> = HashSet::new();

    while !remaining.is_empty() {
        // Prefer a triple touching an already-seen blank node; among those,
        // the one introducing the fewest new ones.
        let pick = remaining
            .iter()
            .enumerate()
            .filter(|(_, t)| blanks_of(t).iter().any(|b| seen.contains(*b)))
            .min_by_key(|(_, t)| blanks_of(t).iter().filter(|b| !seen.contains(**b)).count())
            .map(|(i, _)| i)
            .unwrap_or(0);

        let t = remaining.remove(pick);
        for b in blanks_of(&t) {
            seen.insert(b.clone());
        }
        ordered.push(t);
    }

    ordered
}

fn match_triples(
    expected: &[Triple],
    actual: &[Triple],
    exp_idx: usize,
    forward: &mut HashMap<Term, Term>,
    reverse: &mut HashMap<Term, Term>,
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

        let saved_forward = forward.clone();
        let saved_reverse = reverse.clone();

        if triple_matches(exp_triple, &actual[act_idx], forward, reverse) {
            used[act_idx] = true;
            if match_triples(expected, actual, exp_idx + 1, forward, reverse, used) {
                return true;
            }
            used[act_idx] = false;
        }

        *forward = saved_forward;
        *reverse = saved_reverse;
    }

    false
}

fn triple_matches(
    expected: &Triple,
    actual: &Triple,
    forward: &mut HashMap<Term, Term>,
    reverse: &mut HashMap<Term, Term>,
) -> bool {
    terms_match(&expected.s, &actual.s, forward, reverse)
        && terms_match(&expected.p, &actual.p, forward, reverse)
        && terms_match(&expected.o, &actual.o, forward, reverse)
}

fn terms_match(
    expected: &Term,
    actual: &Term,
    forward: &mut HashMap<Term, Term>,
    reverse: &mut HashMap<Term, Term>,
) -> bool {
    match (expected, actual) {
        (Term::BlankNode(_), Term::BlankNode(_)) => {
            match (forward.get(expected), reverse.get(actual)) {
                (Some(mapped), _) => mapped == actual,
                // `actual` is already the image of a different expected
                // blank — taking it again would break injectivity.
                (None, Some(_)) => false,
                (None, None) => {
                    forward.insert(expected.clone(), actual.clone());
                    reverse.insert(actual.clone(), expected.clone());
                    true
                }
            }
        }
        // IRIs and literals compare by the IR's own term equality, which
        // already accounts for value, datatype and language tag.
        _ => expected == actual,
    }
}

// ---------------------------------------------------------------------------
// Dataset isomorphism (TriG eval)
// ---------------------------------------------------------------------------

/// Whether two datasets are isomorphic.
///
/// Compared graph by graph: the default graphs must be isomorphic, and the
/// named graphs must pair up. IRI-named graphs pair by name. Blank-node-named
/// graphs have no stable name, so they pair by CONTENT — any unused partner
/// whose graph is isomorphic — which is a sound approximation rather than the
/// full thing: a true dataset isomorphism would solve one bijection across
/// graph names AND graph contents simultaneously, so that a blank node
/// appearing both as a graph name and inside a graph is forced to the same
/// image. Nothing in the rdf11 TriG suite needs that, and the approximation
/// only ever accepts, never rejects, a genuinely isomorphic pair. Any test it
/// waves through for the wrong reason would have to have two
/// blank-node-named graphs with interchangeable contents.
pub fn are_datasets_isomorphic(expected: &Dataset, actual: &Dataset) -> bool {
    if !are_graphs_isomorphic(
        expected.default_graph().triples(),
        actual.default_graph().triples(),
    ) {
        return false;
    }

    if expected.named_graph_count() != actual.named_graph_count() {
        return false;
    }

    let mut unmatched: Vec<(&Term, &Graph)> = actual.named_graphs().collect();

    for (name, graph) in expected.named_graphs() {
        let found = if name.is_blank() {
            unmatched.iter().position(|(other, g)| {
                other.is_blank() && are_graphs_isomorphic(graph.triples(), g.triples())
            })
        } else {
            unmatched.iter().position(|(other, g)| {
                *other == name && are_graphs_isomorphic(graph.triples(), g.triples())
            })
        };

        match found {
            Some(i) => {
                unmatched.swap_remove(i);
            }
            None => return false,
        }
    }

    unmatched.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> Term {
        Term::iri(s)
    }
    fn blank(s: &str) -> Term {
        Term::blank(s)
    }
    fn t(s: Term, p: Term, o: Term) -> Triple {
        Triple::new(s, p, o)
    }

    #[test]
    fn ground_graphs_compare_as_sets() {
        let a = vec![
            t(iri("http://a"), iri("http://p"), iri("http://b")),
            t(iri("http://c"), iri("http://p"), iri("http://d")),
        ];
        let b = vec![
            t(iri("http://c"), iri("http://p"), iri("http://d")),
            t(iri("http://a"), iri("http://p"), iri("http://b")),
        ];
        assert!(are_graphs_isomorphic(&a, &b));
    }

    #[test]
    fn a_repeated_triple_does_not_change_the_graph() {
        let once = vec![t(iri("http://a"), iri("http://p"), iri("http://b"))];
        let twice = vec![
            t(iri("http://a"), iri("http://p"), iri("http://b")),
            t(iri("http://a"), iri("http://p"), iri("http://b")),
        ];
        assert!(are_graphs_isomorphic(&once, &twice));
    }

    #[test]
    fn blank_labels_are_renamed_not_compared() {
        let a = vec![
            t(blank("b0"), iri("http://p"), iri("http://o")),
            t(blank("b0"), iri("http://q"), iri("http://o2")),
        ];
        let b = vec![
            t(blank("zzz"), iri("http://q"), iri("http://o2")),
            t(blank("zzz"), iri("http://p"), iri("http://o")),
        ];
        assert!(are_graphs_isomorphic(&a, &b));
    }

    /// The bijection must be INJECTIVE: two distinct blanks on one side may
    /// not collapse onto one blank on the other, even though the triple
    /// counts agree. This is the reverse-map guard specifically; the
    /// structural side of matching is locked by
    /// `a_two_cycle_is_not_two_self_loops`.
    #[test]
    fn distinct_blanks_may_not_collapse() {
        let two_blanks = vec![
            t(blank("b0"), iri("http://p"), iri("http://o")),
            t(blank("b1"), iri("http://q"), iri("http://o")),
        ];
        let one_blank = vec![
            t(blank("x"), iri("http://p"), iri("http://o")),
            t(blank("x"), iri("http://q"), iri("http://o")),
        ];
        assert!(!are_graphs_isomorphic(&two_blanks, &one_blank));
    }

    #[test]
    fn literal_datatype_is_part_of_the_term() {
        let as_string = vec![t(iri("http://a"), iri("http://p"), Term::string("1"))];
        let as_integer = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::typed("1", fluree_graph_ir::Datatype::xsd_integer()),
        )];
        assert!(!are_graphs_isomorphic(&as_string, &as_integer));
    }

    #[test]
    fn a_deeper_blank_chain_still_matches() {
        // ( 1 2 ) style spine: three linked blank nodes.
        let mk = |p: &str| {
            vec![
                t(blank(&format!("{p}0")), iri("http://f"), iri("http://one")),
                t(
                    blank(&format!("{p}0")),
                    iri("http://r"),
                    blank(&format!("{p}1")),
                ),
                t(blank(&format!("{p}1")), iri("http://f"), iri("http://two")),
                t(blank(&format!("{p}1")), iri("http://r"), iri("http://nil")),
            ]
        };
        assert!(are_graphs_isomorphic(&mk("a"), &mk("z")));
    }

    /// The classic refutation: a 2-cycle between two blank nodes and two
    /// self-loops have the same triple count, the same blank-node count and
    /// the same per-node degree, but are not isomorphic. A checker that
    /// matched on counts alone would pass this.
    ///
    /// What this locks is the BLANK-MATCHING logic — that a candidate pairing
    /// is rejected when it cannot be extended consistently across the whole
    /// graph. It is not an injectivity test; injectivity is what
    /// `distinct_blanks_may_not_collapse` guards. Two different properties,
    /// two different tests — do not carry one's expectation onto the other.
    #[test]
    fn a_two_cycle_is_not_two_self_loops() {
        let two_cycle = vec![
            t(blank("a"), iri("http://p"), blank("b")),
            t(blank("b"), iri("http://p"), blank("a")),
        ];
        let two_self_loops = vec![
            t(blank("x"), iri("http://p"), blank("x")),
            t(blank("y"), iri("http://p"), blank("y")),
        ];
        assert!(!are_graphs_isomorphic(&two_cycle, &two_self_loops));
        assert!(!are_graphs_isomorphic(&two_self_loops, &two_cycle));
    }

    /// Disconnected blank components must be matched across components, not
    /// just within the one the search happens to start in.
    #[test]
    fn disconnected_blank_components_are_matched() {
        let a = vec![
            t(blank("a1"), iri("http://p"), blank("a2")),
            t(blank("b1"), iri("http://q"), blank("b2")),
        ];
        // Same two components, relabeled AND listed in the opposite order.
        let b = vec![
            t(blank("z1"), iri("http://q"), blank("z2")),
            t(blank("y1"), iri("http://p"), blank("y2")),
        ];
        assert!(are_graphs_isomorphic(&a, &b));
    }

    /// Two components that are individually the right shape but attached to
    /// the wrong predicates must still be refused.
    #[test]
    fn disconnected_components_must_match_the_right_partner() {
        let a = vec![
            t(blank("a1"), iri("http://p"), blank("a2")),
            t(blank("b1"), iri("http://q"), blank("b2")),
        ];
        let b = vec![
            t(blank("z1"), iri("http://p"), blank("z2")),
            t(blank("y1"), iri("http://p"), blank("y2")),
        ];
        assert!(!are_graphs_isomorphic(&a, &b));
    }

    /// RDF 1.1 §3.3: language tags are case-insensitive for term equality.
    #[test]
    fn language_tags_compare_case_insensitively() {
        let lower = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "fr"),
        )];
        let upper = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "FR"),
        )];
        let mixed = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "fr-CA"),
        )];
        let mixed_other = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "FR-ca"),
        )];

        assert!(are_graphs_isomorphic(&lower, &upper));
        assert!(are_graphs_isomorphic(&mixed, &mixed_other));
    }

    /// Case-folding the tag must not fold DIFFERENT tags together, and must
    /// not touch the literal's value.
    #[test]
    fn language_tag_folding_does_not_erase_real_differences() {
        let fr = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "fr"),
        )];
        let en = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "en"),
        )];
        let fr_ca = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("chat", "fr-CA"),
        )];
        let cased_value = vec![t(
            iri("http://a"),
            iri("http://p"),
            Term::lang_string("CHAT", "fr"),
        )];

        assert!(!are_graphs_isomorphic(&fr, &en));
        assert!(!are_graphs_isomorphic(&fr, &fr_ca));
        assert!(
            !are_graphs_isomorphic(&fr, &cased_value),
            "the literal VALUE stays case-sensitive; only the tag folds"
        );
    }

    #[test]
    fn different_sizes_are_never_isomorphic() {
        let a = vec![t(iri("http://a"), iri("http://p"), iri("http://b"))];
        let b = vec![
            t(iri("http://a"), iri("http://p"), iri("http://b")),
            t(iri("http://a"), iri("http://p"), iri("http://c")),
        ];
        assert!(!are_graphs_isomorphic(&a, &b));
    }
}
