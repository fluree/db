//! Count-only join planner — type-safe IR + query analysis.
//!
//! Replaces the per-shape `detect_*` + `fast_*` pairs with a single planner that
//! analyzes the WHERE clause join graph and composes a count-only computation plan.
//!
//! The IR enforces **key domain safety** (subject vs object keys) and **output kind
//! safety** (scalar vs stream vs key set) at the type level, preventing invalid
//! compositions like "anti-join a subject stream against an object key set."
//!
//! ## Architecture
//!
//! - `try_build_count_plan()` — entry point, analyses `Query` → `Option<CountPlan>`
//! - `CountPlan` — the plan root, wrapping a `CountPlanRoot` + output var
//! - `count_plan_exec.rs` — evaluates a `CountPlan` against a `BinaryIndexStore`

use crate::execute::operator_tree::{detect_count_all_aggregate, validate_simple_triple};
use crate::ir::triple::Ref;
use crate::ir::Query;
use crate::ir::QueryOptions;
use crate::ir::{Expression, Pattern};
use crate::var_registry::VarId;

/// Resolve an EXISTS block from a `Filter(Expression::Exists { .. })` expression.
fn resolve_filter_exists(expr: &Expression) -> Option<(&[Pattern], bool)> {
    match expr {
        Expression::Exists { patterns, negated } => Some((patterns.as_slice(), *negated)),
        _ => None,
    }
}

// ===========================================================================
// Count Plan IR
// ===========================================================================

/// Produces a single scalar count (u64), no per-key breakdown.
#[derive(Debug)]
pub(crate) enum ScalarNode {
    /// Total row count for a predicate from PSOT segment headers.
    /// Maps to: `count_rows_for_predicate_psot`
    TotalRowCount { pred: Ref },

    /// Reduce a stream to its total: `Σ_k count(k)`.
    Sum { source: StreamNode },

    // Kept for: potential optimization — fuse exclusion into the scalar sum traversal
    // instead of building a separate AntiJoin stream node.
    #[expect(dead_code)]
    /// Reduce a stream with exclusion: `Σ_{k ∉ excluded} count(k)`.
    /// `source` and `excluded` must share the same key domain.
    SumExcluding {
        source: StreamNode,
        excluded: KeySetNode,
    },

    // Kept for: potential optimization — fuse inclusion into the scalar sum traversal
    // instead of building a separate SemiJoin stream node.
    #[expect(dead_code)]
    /// Reduce a stream with inclusion: `Σ_{k ∈ filter} count(k)`.
    /// `source` and `filter` must share the same key domain.
    SumFiltered {
        source: StreamNode,
        filter: KeySetNode,
    },

    /// Count rows from POST(pred) grouped by object, summing only objects in the filter set.
    /// Maps to: `sum_post_object_counts_filtered`
    /// Used for: EXISTS on outer triple's object variable.
    PostObjectFilteredSum {
        pred: Ref,
        object_filter: KeySetNode,
    },

    /// `TotalRowCount(pred) - PostObjectFilteredSum(pred, excluded_objects)`.
    /// Maps to: `count_rows_for_predicate_psot - sum_post_object_counts_filtered`
    /// Used for: MINUS on outer triple's object variable.
    TotalMinusPostObjectFilteredSum {
        pred: Ref,
        excluded_objects: KeySetNode,
    },
}

/// Produces a sorted stream of `(key, count)` pairs in a specific key domain.
#[derive(Debug)]
pub(crate) enum StreamNode {
    /// Scan PSOT for predicate → sorted `(subject_id, row_count)` stream.
    /// Maps to: `PsotSubjectCountIter`
    /// Key domain: Subject
    SubjectCountScan { pred: Ref },

    /// N-way inner merge-join: keys where ALL children match, counts multiplied.
    /// All children must share the same key domain (Subject).
    /// Formula: for each key k present in all children → `(k, Π_i count_i(k))`
    StarJoin { children: Vec<StreamNode> },

    /// Required stream with OPTIONAL multipliers.
    /// For each key in `required`: multiply by `max(1, Π per optional group)`.
    /// Each optional group is a set of streams in the same key domain as required.
    /// Formula: `(k, count_req(k) × Π_g max(1, Π_i opt_gi(k)))`
    OptionalJoin {
        required: Box<StreamNode>,
        optional_groups: Vec<Vec<StreamNode>>,
    },

    /// Exclude keys present in `excluded` set, preserving counts.
    /// Key domains of source and excluded must match.
    AntiJoin {
        source: Box<StreamNode>,
        excluded: KeySetNode,
    },

    /// Keep only keys present in `filter` set, preserving counts.
    /// Key domains of source and filter must match.
    SemiJoin {
        source: Box<StreamNode>,
        filter: KeySetNode,
    },
}

/// Produces a materialized set (or sorted vec) of keys for semi/anti join.
#[derive(Debug)]
pub(crate) enum KeySetNode {
    /// Collect all subject IDs for a predicate → `FxHashSet<u64>`.
    /// Maps to: `collect_subjects_for_predicate_set`
    /// Key domain: Subject
    SubjectSet { pred: Ref },

    /// Collect all subject IDs sorted for a predicate → `Vec<u64>`.
    /// Maps to: `collect_subjects_for_predicate_sorted`
    /// Key domain: Subject
    SubjectsSorted { pred: Ref },

    /// Subjects with an object in a given set (for object-chain patterns).
    /// Maps to: `collect_subjects_with_object_in_set`
    /// Key domain: Subject (output), input set is ObjectIri
    SubjectsWithObjectIn {
        pred: Ref,
        object_set: Box<KeySetNode>,
    },

    /// Intersection of multiple sorted key sets → sorted `Vec<u64>`.
    /// All children must share the same key domain.
    /// Maps to: `intersect_many_sorted`
    IntersectSorted { children: Vec<KeySetNode> },
}

/// How the chain tail is weighted for modifier interactions.
#[derive(Debug, Clone)]
pub(crate) enum TailWeight {
    /// Pure inner join chain — no tail modifier.
    None,
    /// OPTIONAL on tail: multiply per-object weights by `max(1, count_tail(vN))`.
    Optional { tail_pred: Ref },
    /// MINUS on tail: exclude objects of pN that appear in `subjects(tail_pred)`.
    Minus { tail_pred: Ref },
    /// EXISTS on tail: keep only objects of pN that appear in `subjects(tail_pred)`.
    Exists { tail_pred: Ref },
}

/// A linear chain fold with explicit hop structure.
///
/// Algorithm (right-to-left):
///   1. Build initial weights from rightmost predicate (+ tail modifier)
///   2. Fold right-to-left through intermediate hops via `PsotSubjectWeightedSumIter`
///   3. Final merge: `POST(head_pred)` objects × weights
///
/// Requires IRI_REF objects at each join point (returns `None` if not).
#[derive(Debug)]
pub(crate) struct ChainFold {
    /// Predicates in chain order: `[p1, p2, ..., pN]` for
    /// `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ...`
    /// Must have `len >= 2`.
    pub predicates: Vec<Ref>,
    /// Modifier on the chain tail variable (the object of the last predicate).
    pub tail_weight: TailWeight,
}

/// Top-level plan root.
#[derive(Debug)]
pub(crate) enum CountPlanRoot {
    /// Scalar result (single count).
    Scalar(ScalarNode),
    /// Chain fold (its own algorithm, produces scalar).
    Chain(ChainFold),
}

/// A complete count-only plan.
#[derive(Debug)]
pub(crate) struct CountPlan {
    pub root: CountPlanRoot,
    pub out_var: VarId,
}

// ===========================================================================
// Planner
// ===========================================================================

/// Attempt to build a count-only plan for the given query.
///
/// Returns `None` if the query doesn't match any supported count-only shape.
/// The caller should fall through to the general pipeline in that case.
///
/// Gate: ungrouped `COUNT(*)`, no DISTINCT, no HAVING, no ORDER BY, no LIMIT/OFFSET.
/// Runtime gating (binary-index store, HEAD query, root policy) happens in the executor.
pub(crate) fn try_build_count_plan(query: &Query, options: &QueryOptions) -> Option<CountPlan> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // Classify all patterns in the WHERE clause.
    let classified = classify_patterns(&query.patterns)?;

    // Must have at least one required triple.
    if classified.required_triples.is_empty() {
        return None;
    }

    // Detect topology of required triples.
    let topology = detect_topology(&classified.required_triples)?;

    // Build the plan based on topology + modifiers.
    let root = match topology {
        Topology::SingleTriple { pred } => {
            if classified.has_any_modifiers() {
                // Single triple with modifiers — build as stream + modifier.
                build_single_triple_with_modifiers(pred, &classified)?
            } else {
                CountPlanRoot::Scalar(ScalarNode::TotalRowCount { pred })
            }
        }
        Topology::Star { triples } => build_star_plan(triples, &classified)?,
        Topology::Chain { predicates, vars } => build_chain_plan(predicates, vars, &classified)?,
    };

    Some(CountPlan { root, out_var })
}

// ---------------------------------------------------------------------------
// Internal: gate check
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Internal: pattern classification
// ---------------------------------------------------------------------------

/// A validated basic triple: `?subject <bound_pred> ?object`.
#[derive(Debug, Clone)]
struct BasicTriple {
    subject_var: VarId,
    pred: Ref,
    object_var: VarId,
}

/// An OPTIONAL block: all triples share a subject var with the outer pattern.
#[derive(Debug)]
struct OptionalBlock {
    triples: Vec<BasicTriple>,
}

/// A MINUS block: triples to anti-join on.
#[derive(Debug)]
struct MinusBlock {
    triples: Vec<BasicTriple>,
}

/// An EXISTS or NOT EXISTS block.
#[derive(Debug)]
struct ExistsBlock {
    triples: Vec<BasicTriple>,
    negated: bool,
}

/// Result of classifying all patterns in the WHERE clause.
#[derive(Debug)]
struct ClassifiedPatterns {
    required_triples: Vec<BasicTriple>,
    optional_blocks: Vec<OptionalBlock>,
    minus_blocks: Vec<MinusBlock>,
    exists_blocks: Vec<ExistsBlock>,
}

impl ClassifiedPatterns {
    fn has_any_modifiers(&self) -> bool {
        !self.optional_blocks.is_empty()
            || !self.minus_blocks.is_empty()
            || !self.exists_blocks.is_empty()
    }
}

/// Classify all patterns in the WHERE clause.
/// Returns `None` if any pattern is unsupported (UNION, BIND, VALUES, etc.).
fn classify_patterns(patterns: &[Pattern]) -> Option<ClassifiedPatterns> {
    let mut required_triples = Vec::new();
    let mut optional_blocks = Vec::new();
    let mut minus_blocks = Vec::new();
    let mut exists_blocks = Vec::new();
    let mut seen_obj: std::collections::HashSet<VarId> = std::collections::HashSet::new();

    for p in patterns {
        match p {
            Pattern::Triple(tp) => {
                let (s, pred, o) = validate_simple_triple(tp)?;
                // Bail on self-loop (s == o needs special handling).
                if s == o {
                    return None;
                }
                if !seen_obj.insert(o) {
                    // Duplicate object var across required triples — could be multicolumn
                    // join shape (?s p1 ?o . ?s p2 ?o). Not handled by count plan yet.
                    return None;
                }
                required_triples.push(BasicTriple {
                    subject_var: s,
                    pred,
                    object_var: o,
                });
            }
            Pattern::Optional(inner) => {
                if inner.is_empty() {
                    return None;
                }
                let block = classify_optional_block(inner, &mut seen_obj)?;
                optional_blocks.push(block);
            }
            Pattern::Minus(inner) => {
                if inner.is_empty() {
                    return None;
                }
                let block = classify_minus_block(inner)?;
                minus_blocks.push(block);
            }
            Pattern::Exists(inner) => {
                let block = classify_exists_block(inner, false)?;
                exists_blocks.push(block);
            }
            Pattern::NotExists(inner) => {
                let block = classify_exists_block(inner, true)?;
                exists_blocks.push(block);
            }
            Pattern::Filter(expr) => {
                // Check if this is an EXISTS/NOT EXISTS filter.
                if let Some((pats, negated)) = resolve_filter_exists(expr) {
                    let block = classify_exists_block(pats, negated)?;
                    exists_blocks.push(block);
                } else {
                    // Non-EXISTS filter — not supported in count plan.
                    return None;
                }
            }
            // Anything else (UNION, BIND, VALUES, SERVICE, subquery, etc.) — bail.
            _ => return None,
        }
    }

    Some(ClassifiedPatterns {
        required_triples,
        optional_blocks,
        minus_blocks,
        exists_blocks,
    })
}

fn classify_optional_block(
    inner: &[Pattern],
    seen_obj: &mut std::collections::HashSet<VarId>,
) -> Option<OptionalBlock> {
    let mut triples = Vec::with_capacity(inner.len());
    let mut group_seen: std::collections::HashSet<VarId> =
        std::collections::HashSet::with_capacity(inner.len());
    let mut subject_var: Option<VarId> = None;

    for pat in inner {
        let Pattern::Triple(tp) = pat else {
            return None;
        };
        let (s, pred, o) = validate_simple_triple(tp)?;
        if s == o {
            return None;
        }
        match subject_var {
            None => subject_var = Some(s),
            Some(existing) if existing != s => return None,
            Some(_) => {}
        }
        if !group_seen.insert(o) {
            return None;
        }
        if !seen_obj.insert(o) {
            return None;
        }
        triples.push(BasicTriple {
            subject_var: s,
            pred,
            object_var: o,
        });
    }

    Some(OptionalBlock { triples })
}

fn classify_minus_block(inner: &[Pattern]) -> Option<MinusBlock> {
    let mut triples = Vec::with_capacity(inner.len());

    for pat in inner {
        let Pattern::Triple(tp) = pat else {
            return None;
        };
        let (s, pred, o) = validate_simple_triple(tp)?;
        triples.push(BasicTriple {
            subject_var: s,
            pred,
            object_var: o,
        });
    }

    Some(MinusBlock { triples })
}

fn classify_exists_block(inner: &[Pattern], negated: bool) -> Option<ExistsBlock> {
    let mut triples = Vec::with_capacity(inner.len());

    for pat in inner {
        let Pattern::Triple(tp) = pat else {
            return None;
        };
        let (s, pred, o) = validate_simple_triple(tp)?;
        triples.push(BasicTriple {
            subject_var: s,
            pred,
            object_var: o,
        });
    }

    Some(ExistsBlock { triples, negated })
}

// ---------------------------------------------------------------------------
// Internal: topology detection
// ---------------------------------------------------------------------------

/// Detected topology of the required triples.
#[derive(Debug)]
enum Topology {
    /// A single triple pattern.
    SingleTriple { pred: Ref },
    /// All triples share the same subject variable (star join).
    Star { triples: Vec<BasicTriple> },
    /// Triples form a linear chain `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ...`
    Chain {
        /// Predicates in chain order.
        predicates: Vec<Ref>,
        /// Variables in chain order: `[v0, v1, v2, ...]` (one more than predicates).
        vars: Vec<VarId>,
    },
}

fn detect_topology(triples: &[BasicTriple]) -> Option<Topology> {
    if triples.is_empty() {
        return None;
    }

    if triples.len() == 1 {
        return Some(Topology::SingleTriple {
            pred: triples[0].pred.clone(),
        });
    }

    // Try star: all share the same subject var.
    let first_subject = triples[0].subject_var;
    if triples.iter().all(|t| t.subject_var == first_subject) {
        return Some(Topology::Star {
            triples: triples.to_vec(),
        });
    }

    // Try chain: triples form a linear path.
    if let Some((preds, vars)) = detect_chain(triples) {
        return Some(Topology::Chain {
            predicates: preds,
            vars,
        });
    }

    // Mixed/unsupported topology.
    None
}

/// Detect an N-hop linear chain from a set of triples.
///
/// Returns `(predicates_in_order, vars_in_order)` where vars has len = preds.len() + 1.
fn detect_chain(triples: &[BasicTriple]) -> Option<(Vec<Ref>, Vec<VarId>)> {
    if triples.len() < 2 {
        return None;
    }

    // Build adjacency: for each triple, we have an edge subject_var -> object_var.
    // A valid chain has exactly 2 endpoints (degree 1) and all others degree 2.
    use std::collections::HashMap;
    let mut degree: HashMap<VarId, usize> = HashMap::new();

    for t in triples {
        *degree.entry(t.subject_var).or_insert(0) += 1;
        *degree.entry(t.object_var).or_insert(0) += 1;
    }

    // Find the start: a variable that appears as subject but not as object of any triple.
    // In a chain ?a <p1> ?b . ?b <p2> ?c, ?a is the start (only a subject).
    let mut subject_set: std::collections::HashSet<VarId> = std::collections::HashSet::new();
    let mut object_set: std::collections::HashSet<VarId> = std::collections::HashSet::new();
    for t in triples {
        subject_set.insert(t.subject_var);
        object_set.insert(t.object_var);
    }

    // Chain start: appears as subject but not as object of any triple.
    let mut starts: Vec<VarId> = subject_set
        .iter()
        .filter(|v| !object_set.contains(v))
        .copied()
        .collect();

    if starts.len() != 1 {
        return None;
    }
    let start = starts.pop().unwrap();

    // Build a subject→triple index for following the chain.
    let mut by_subject: HashMap<VarId, Vec<usize>> = HashMap::new();
    for (i, t) in triples.iter().enumerate() {
        by_subject.entry(t.subject_var).or_default().push(i);
    }

    // Walk the chain from start.
    let mut preds = Vec::with_capacity(triples.len());
    let mut vars = Vec::with_capacity(triples.len() + 1);
    vars.push(start);
    let mut current = start;
    let mut used: std::collections::HashSet<usize> = std::collections::HashSet::new();

    for _ in 0..triples.len() {
        let candidates = by_subject.get(&current)?;
        // Find the one unused triple starting at `current`.
        let mut found = None;
        for &idx in candidates {
            if !used.contains(&idx) {
                if found.is_some() {
                    // Multiple unused triples from same subject → not a linear chain.
                    return None;
                }
                found = Some(idx);
            }
        }
        let idx = found?;
        used.insert(idx);
        let t = &triples[idx];
        preds.push(t.pred.clone());
        vars.push(t.object_var);
        current = t.object_var;
    }

    if used.len() != triples.len() {
        return None;
    }

    Some((preds, vars))
}

// ---------------------------------------------------------------------------
// Internal: plan building
// ---------------------------------------------------------------------------

/// Build a plan for a single required triple + modifiers.
fn build_single_triple_with_modifiers(
    pred: Ref,
    classified: &ClassifiedPatterns,
) -> Option<CountPlanRoot> {
    let subject_var = classified.required_triples[0].subject_var;
    let object_var = classified.required_triples[0].object_var;

    // Check if all MINUS/EXISTS blocks target the object var (object-chain pattern).
    // If so, use POST-based object counting instead of subject-domain streaming.
    if !classified.minus_blocks.is_empty() || !classified.exists_blocks.is_empty() {
        if let Some(plan) = try_build_object_chain_plan(&pred, object_var, classified) {
            return Some(plan);
        }
    }

    // Subject-domain path: verify OPTIONAL blocks share the same subject var.
    for opt in &classified.optional_blocks {
        for t in &opt.triples {
            if t.subject_var != subject_var || t.object_var == subject_var {
                return None;
            }
        }
    }

    let required_stream = StreamNode::SubjectCountScan { pred: pred.clone() };

    // Apply OPTIONAL groups if any.
    let stream = if classified.optional_blocks.is_empty() {
        required_stream
    } else {
        let optional_groups: Vec<Vec<StreamNode>> = classified
            .optional_blocks
            .iter()
            .map(|block| {
                block
                    .triples
                    .iter()
                    .map(|t| StreamNode::SubjectCountScan {
                        pred: t.pred.clone(),
                    })
                    .collect()
            })
            .collect();
        StreamNode::OptionalJoin {
            required: Box::new(required_stream),
            optional_groups,
        }
    };

    apply_modifiers_to_stream(stream, subject_var, classified)
}

/// Try to build an object-chain plan: single outer triple with MINUS/EXISTS
/// targeting the outer triple's object variable.
///
/// Shapes:
/// - `?a <p> ?b . EXISTS { ?b <p2> ?c }` → PostObjectFilteredSum
/// - `?a <p> ?b . MINUS { ?b <p2> ?c }` → TotalMinusPostObjectFilteredSum
/// - `?a <p> ?b . EXISTS { ?b <p2> ?c . ?c <p3> ?d }` → PostObjectFilteredSum with chain keyset
/// - `?a <p> ?b . MINUS { ?b <p2> ?c . ?c <p3> ?d }` → TotalMinusPostObjectFilteredSum
///
/// Returns `None` if the pattern doesn't match or has unsupported combinations
/// (e.g., OPTIONAL on object, mixed subject/object modifiers).
fn try_build_object_chain_plan(
    outer_pred: &Ref,
    outer_object_var: VarId,
    classified: &ClassifiedPatterns,
) -> Option<CountPlanRoot> {
    // No OPTIONAL support on object-chain patterns.
    if !classified.optional_blocks.is_empty() {
        return None;
    }

    // Only one modifier block total for now (single EXISTS or single MINUS).
    let total_blocks = classified.minus_blocks.len() + classified.exists_blocks.len();
    if total_blocks != 1 {
        return None;
    }

    if classified.exists_blocks.len() == 1 {
        let exists = &classified.exists_blocks[0];
        let keyset = build_keyset_for_object_chain_block(&exists.triples, outer_object_var)?;
        if exists.negated {
            // NOT EXISTS → MINUS semantics: total - filtered
            Some(CountPlanRoot::Scalar(
                ScalarNode::TotalMinusPostObjectFilteredSum {
                    pred: outer_pred.clone(),
                    excluded_objects: keyset,
                },
            ))
        } else {
            Some(CountPlanRoot::Scalar(ScalarNode::PostObjectFilteredSum {
                pred: outer_pred.clone(),
                object_filter: keyset,
            }))
        }
    } else {
        let minus = &classified.minus_blocks[0];
        let keyset = build_keyset_for_object_chain_block(&minus.triples, outer_object_var)?;
        Some(CountPlanRoot::Scalar(
            ScalarNode::TotalMinusPostObjectFilteredSum {
                pred: outer_pred.clone(),
                excluded_objects: keyset,
            },
        ))
    }
}

/// Build a plan for a star join topology (all triples share the same subject var).
fn build_star_plan(
    triples: Vec<BasicTriple>,
    classified: &ClassifiedPatterns,
) -> Option<CountPlanRoot> {
    let subject_var = triples[0].subject_var;

    // Verify all OPTIONAL blocks share the same subject var.
    for opt in &classified.optional_blocks {
        for t in &opt.triples {
            if t.subject_var != subject_var {
                return None;
            }
            // Object var must not equal subject var.
            if t.object_var == subject_var {
                return None;
            }
        }
    }

    // Build the required star join stream.
    let children: Vec<StreamNode> = triples
        .iter()
        .map(|t| StreamNode::SubjectCountScan {
            pred: t.pred.clone(),
        })
        .collect();

    let required_stream = if children.len() == 1 {
        children.into_iter().next().unwrap()
    } else {
        StreamNode::StarJoin { children }
    };

    // Apply OPTIONAL groups if any.
    let stream = if classified.optional_blocks.is_empty() {
        required_stream
    } else {
        let optional_groups: Vec<Vec<StreamNode>> = classified
            .optional_blocks
            .iter()
            .map(|block| {
                block
                    .triples
                    .iter()
                    .map(|t| StreamNode::SubjectCountScan {
                        pred: t.pred.clone(),
                    })
                    .collect()
            })
            .collect();
        StreamNode::OptionalJoin {
            required: Box::new(required_stream),
            optional_groups,
        }
    };

    // Apply MINUS/EXISTS modifiers.
    apply_modifiers_to_stream(stream, subject_var, classified)
}

/// Build a plan for a chain topology.
fn build_chain_plan(
    predicates: Vec<Ref>,
    vars: Vec<VarId>,
    classified: &ClassifiedPatterns,
) -> Option<CountPlanRoot> {
    debug_assert!(vars.len() == predicates.len() + 1);
    let tail_var = *vars.last()?;

    // Determine the tail weight from a single modifier block on the tail variable.
    // Only one modifier type is supported per chain; mixed modifiers bail.
    let tail_weight = if !classified.has_any_modifiers() {
        TailWeight::None
    } else if classified.minus_blocks.len() == 1
        && classified.optional_blocks.is_empty()
        && classified.exists_blocks.is_empty()
    {
        let minus = &classified.minus_blocks[0];
        if minus.triples.len() == 1 && minus.triples[0].subject_var == tail_var {
            TailWeight::Minus {
                tail_pred: minus.triples[0].pred.clone(),
            }
        } else {
            return None;
        }
    } else if classified.optional_blocks.len() == 1
        && classified.minus_blocks.is_empty()
        && classified.exists_blocks.is_empty()
    {
        let opt = &classified.optional_blocks[0];
        if opt.triples.len() == 1 && opt.triples[0].subject_var == tail_var {
            TailWeight::Optional {
                tail_pred: opt.triples[0].pred.clone(),
            }
        } else {
            return None;
        }
    } else if classified.exists_blocks.len() == 1
        && classified.minus_blocks.is_empty()
        && classified.optional_blocks.is_empty()
    {
        let exists = &classified.exists_blocks[0];
        if exists.triples.len() == 1 && exists.triples[0].subject_var == tail_var {
            if exists.negated {
                // NOT EXISTS on tail = anti-join = MINUS semantics at tail.
                TailWeight::Minus {
                    tail_pred: exists.triples[0].pred.clone(),
                }
            } else {
                TailWeight::Exists {
                    tail_pred: exists.triples[0].pred.clone(),
                }
            }
        } else {
            return None;
        }
    } else {
        // Multiple or complex modifiers on chains — not supported.
        return None;
    };

    Some(CountPlanRoot::Chain(ChainFold {
        predicates,
        tail_weight,
    }))
}

/// Wrap a stream with MINUS and EXISTS modifiers, then reduce to scalar.
///
/// OPTIONAL blocks should already be applied to the stream before calling this.
fn apply_modifiers_to_stream(
    mut stream: StreamNode,
    subject_var: VarId,
    classified: &ClassifiedPatterns,
) -> Option<CountPlanRoot> {
    // Apply EXISTS (semi-join) blocks.
    for exists in &classified.exists_blocks {
        if exists.negated {
            // NOT EXISTS → anti-join
            let excluded = build_keyset_for_block(&exists.triples, subject_var)?;
            stream = StreamNode::AntiJoin {
                source: Box::new(stream),
                excluded,
            };
        } else {
            // EXISTS → semi-join
            let filter = build_keyset_for_block(&exists.triples, subject_var)?;
            stream = StreamNode::SemiJoin {
                source: Box::new(stream),
                filter,
            };
        }
    }

    // Apply MINUS blocks.
    for minus in &classified.minus_blocks {
        let excluded = build_keyset_for_block(&minus.triples, subject_var)?;
        stream = StreamNode::AntiJoin {
            source: Box::new(stream),
            excluded,
        };
    }

    Some(CountPlanRoot::Scalar(ScalarNode::Sum { source: stream }))
}

/// Build a `KeySetNode` for a modifier block's triples, targeting the outer subject var.
///
/// For a single-triple block on the same subject var: `SubjectSet`.
/// For multi-triple block on the same subject var: `IntersectSorted` of `SubjectsSorted`.
/// For blocks on a different variable (e.g., object-chain): bail (use
/// `build_keyset_for_object_chain_block` instead).
fn build_keyset_for_block(triples: &[BasicTriple], outer_subject: VarId) -> Option<KeySetNode> {
    if triples.is_empty() {
        return None;
    }

    // Check what variable the modifier block targets.
    let modifier_subject = triples[0].subject_var;
    if !triples.iter().all(|t| t.subject_var == modifier_subject) {
        // Not all same subject — might be a chain inside the modifier block.
        // Only handled via `build_keyset_for_object_chain_block`.
        return None;
    }

    if modifier_subject == outer_subject {
        // Same subject as outer pattern — simple subject set.
        if triples.len() == 1 {
            Some(KeySetNode::SubjectSet {
                pred: triples[0].pred.clone(),
            })
        } else {
            let children = triples
                .iter()
                .map(|t| KeySetNode::SubjectsSorted {
                    pred: t.pred.clone(),
                })
                .collect();
            Some(KeySetNode::IntersectSorted { children })
        }
    } else {
        None
    }
}

/// Build a `KeySetNode` for a modifier block that targets the object variable of
/// the outer triple (object-chain pattern).
///
/// The modifier block's "head" variable must be `outer_object_var`. Supported shapes:
/// - Single triple: `?b <p2> ?c` → `SubjectsSorted { pred: p2 }`
/// - Star (same subject): `?b <p2> ?c . ?b <p3> ?d` → `IntersectSorted`
/// - Chain: `?b <p2> ?c . ?c <p3> ?d` → `SubjectsWithObjectIn { p2, SubjectSet(p3) }`
fn build_keyset_for_object_chain_block(
    triples: &[BasicTriple],
    outer_object_var: VarId,
) -> Option<KeySetNode> {
    if triples.is_empty() {
        return None;
    }

    // Check if all triples share the same subject and it's the outer object var.
    let first_subject = triples[0].subject_var;
    let all_same_subject = triples.iter().all(|t| t.subject_var == first_subject);

    if all_same_subject && first_subject == outer_object_var {
        // Star/single on the object var — same as subject-domain keyset.
        if triples.len() == 1 {
            return Some(KeySetNode::SubjectsSorted {
                pred: triples[0].pred.clone(),
            });
        }
        let children = triples
            .iter()
            .map(|t| KeySetNode::SubjectsSorted {
                pred: t.pred.clone(),
            })
            .collect();
        return Some(KeySetNode::IntersectSorted { children });
    }

    // Try chain: triples form a linear path starting from outer_object_var.
    if let Some((preds, vars)) = detect_chain(triples) {
        if vars[0] != outer_object_var {
            return None;
        }
        // Chain `?b <p2> ?c . ?c <p3> ?d . ...`
        // Build inside-out: SubjectSet(last_pred), then SubjectsWithObjectIn for each
        // intermediate hop, right-to-left.
        let mut keyset = KeySetNode::SubjectSet {
            pred: preds.last()?.clone(),
        };
        for p in preds.iter().rev().skip(1) {
            keyset = KeySetNode::SubjectsWithObjectIn {
                pred: p.clone(),
                object_set: Box::new(keyset),
            };
        }
        return Some(keyset);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::AggregateFn;
    use crate::ir::triple::{Term, TriplePattern};
    use crate::ir::QueryOutput;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_sid(id: u16, name: &str) -> Ref {
        Ref::Sid(Sid::new(id, name))
    }

    fn make_query(patterns: Vec<Pattern>, out_var: VarId) -> (Query, QueryOptions) {
        let options = QueryOptions::default();
        let grouping = Some(crate::ir::Grouping::Implicit {
            aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![
                crate::aggregate::AggregateSpec {
                    function: AggregateFn::CountAll,
                    input_var: None,
                    output_var: out_var,
                    distinct: false,
                },
            ])
            .expect("non-empty"),
            having: None,
        });
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![out_var]),
            patterns,
            grouping,
            options: options.clone(),
            post_values: None,
        };
        (query, options)
    }

    #[test]
    fn test_star_join_detection() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o1))),
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p2.clone(), Term::Var(o2))),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect star join");

        let plan = plan.unwrap();
        assert_eq!(plan.out_var, count);
        match &plan.root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::StarJoin { children },
            }) => {
                assert_eq!(children.len(), 2);
            }
            other => panic!("Expected Scalar(Sum(StarJoin)), got {other:?}"),
        }
    }

    #[test]
    fn test_single_triple_no_modifiers() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");
        let count = vars.get_or_insert("?count");

        let pred = make_sid(1, "p1");
        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            pred.clone(),
            Term::Var(o),
        ))];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some());

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::TotalRowCount { .. }) => {}
            other => panic!("Expected TotalRowCount, got {other:?}"),
        }
    }

    #[test]
    fn test_chain_detection() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect chain");

        match &plan.unwrap().root {
            CountPlanRoot::Chain(fold) => {
                assert_eq!(fold.predicates.len(), 2);
                assert!(matches!(fold.tail_weight, TailWeight::None));
            }
            other => panic!("Expected Chain, got {other:?}"),
        }
    }

    #[test]
    fn test_star_with_optional() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o1))),
            Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p2.clone(),
                Term::Var(o2),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect star + OPTIONAL");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source:
                    StreamNode::OptionalJoin {
                        required: _,
                        optional_groups,
                    },
            }) => {
                assert_eq!(optional_groups.len(), 1);
                assert_eq!(optional_groups[0].len(), 1);
            }
            other => panic!("Expected Sum(OptionalJoin), got {other:?}"),
        }
    }

    #[test]
    fn test_star_with_minus() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o1))),
            Pattern::Minus(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p2.clone(),
                Term::Var(o2),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect star + MINUS");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::AntiJoin { .. },
            }) => {}
            other => panic!("Expected Sum(AntiJoin), got {other:?}"),
        }
    }

    #[test]
    fn test_star_with_exists() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let o3 = vars.get_or_insert("?o3");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?s p1 ?o1 . ?s p2 ?o2 . FILTER EXISTS { ?s p3 ?o3 }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o1))),
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p2.clone(), Term::Var(o2))),
            Pattern::Filter(Expression::Exists {
                patterns: vec![Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    p3.clone(),
                    Term::Var(o3),
                ))],
                negated: false,
            }),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect star + EXISTS");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::SemiJoin { source, filter },
            }) => {
                assert!(
                    matches!(source.as_ref(), StreamNode::StarJoin { children } if children.len() == 2)
                );
                assert!(matches!(filter, KeySetNode::SubjectSet { .. }));
            }
            other => panic!("Expected Sum(SemiJoin(StarJoin, SubjectSet)), got {other:?}"),
        }
    }

    #[test]
    fn test_single_triple_with_not_exists() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");
        let o2 = vars.get_or_insert("?o2");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        // ?s p1 ?o . FILTER NOT EXISTS { ?s p2 ?o2 }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o))),
            Pattern::Filter(Expression::Exists {
                patterns: vec![Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    p2.clone(),
                    Term::Var(o2),
                ))],
                negated: true,
            }),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect single + NOT EXISTS");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::AntiJoin { source, excluded },
            }) => {
                assert!(matches!(
                    source.as_ref(),
                    StreamNode::SubjectCountScan { .. }
                ));
                assert!(matches!(excluded, KeySetNode::SubjectSet { .. }));
            }
            other => panic!("Expected Sum(AntiJoin(Scan, SubjectSet)), got {other:?}"),
        }
    }

    #[test]
    fn test_single_triple_with_exists_star() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");
        let o2 = vars.get_or_insert("?o2");
        let o3 = vars.get_or_insert("?o3");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?s p1 ?o . FILTER EXISTS { ?s p2 ?o2 . ?s p3 ?o3 }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o))),
            Pattern::Filter(Expression::Exists {
                patterns: vec![
                    Pattern::Triple(TriplePattern::new(Ref::Var(s), p2.clone(), Term::Var(o2))),
                    Pattern::Triple(TriplePattern::new(Ref::Var(s), p3.clone(), Term::Var(o3))),
                ],
                negated: false,
            }),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect single + EXISTS-star");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::SemiJoin { source, filter },
            }) => {
                assert!(matches!(
                    source.as_ref(),
                    StreamNode::SubjectCountScan { .. }
                ));
                assert!(
                    matches!(filter, KeySetNode::IntersectSorted { children } if children.len() == 2)
                );
            }
            other => panic!("Expected Sum(SemiJoin(Scan, IntersectSorted)), got {other:?}"),
        }
    }

    #[test]
    fn test_star_with_optional_and_minus() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let o3 = vars.get_or_insert("?o3");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?s p1 ?o1 . OPTIONAL { ?s p2 ?o2 } . MINUS { ?s p3 ?o3 }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o1))),
            Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p2.clone(),
                Term::Var(o2),
            ))]),
            Pattern::Minus(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p3.clone(),
                Term::Var(o3),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect single + OPTIONAL + MINUS");

        // Expected: Sum(AntiJoin(OptionalJoin(Scan, [Scan]), SubjectSet))
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::AntiJoin { source, excluded },
            }) => {
                assert!(matches!(source.as_ref(), StreamNode::OptionalJoin { .. }));
                assert!(matches!(excluded, KeySetNode::SubjectSet { .. }));
            }
            other => panic!("Expected Sum(AntiJoin(OptionalJoin, SubjectSet)), got {other:?}"),
        }
    }

    #[test]
    fn test_chain_with_optional_tail() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?a p1 ?b . ?b p2 ?c . OPTIONAL { ?c p3 ?d }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
            Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(c),
                p3.clone(),
                Term::Var(d),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect chain + OPTIONAL tail");

        match &plan.unwrap().root {
            CountPlanRoot::Chain(fold) => {
                assert_eq!(fold.predicates.len(), 2);
                assert!(matches!(fold.tail_weight, TailWeight::Optional { .. }));
            }
            other => panic!("Expected Chain with Optional tail, got {other:?}"),
        }
    }

    #[test]
    fn test_chain_with_minus_tail() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?a p1 ?b . ?b p2 ?c . MINUS { ?c p3 ?d }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
            Pattern::Minus(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(c),
                p3.clone(),
                Term::Var(d),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect chain + MINUS tail");

        match &plan.unwrap().root {
            CountPlanRoot::Chain(fold) => {
                assert_eq!(fold.predicates.len(), 2);
                assert!(matches!(fold.tail_weight, TailWeight::Minus { .. }));
            }
            other => panic!("Expected Chain with Minus tail, got {other:?}"),
        }
    }

    #[test]
    fn test_chain_with_exists_tail() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?a p1 ?b . ?b p2 ?c . FILTER EXISTS { ?c p3 ?d }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
            Pattern::Filter(Expression::Exists {
                patterns: vec![Pattern::Triple(TriplePattern::new(
                    Ref::Var(c),
                    p3.clone(),
                    Term::Var(d),
                ))],
                negated: false,
            }),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect chain + EXISTS tail");

        match &plan.unwrap().root {
            CountPlanRoot::Chain(fold) => {
                assert_eq!(fold.predicates.len(), 2);
                assert!(matches!(fold.tail_weight, TailWeight::Exists { .. }));
            }
            other => panic!("Expected Chain with Exists tail, got {other:?}"),
        }
    }

    #[test]
    fn test_chain_with_not_exists_tail() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?a p1 ?b . ?b p2 ?c . FILTER NOT EXISTS { ?c p3 ?d }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
            Pattern::Filter(Expression::Exists {
                patterns: vec![Pattern::Triple(TriplePattern::new(
                    Ref::Var(c),
                    p3.clone(),
                    Term::Var(d),
                ))],
                negated: true,
            }),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect chain + NOT EXISTS tail");

        match &plan.unwrap().root {
            CountPlanRoot::Chain(fold) => {
                assert_eq!(fold.predicates.len(), 2);
                // NOT EXISTS becomes MINUS semantics at the tail.
                assert!(matches!(fold.tail_weight, TailWeight::Minus { .. }));
            }
            other => panic!("Expected Chain with Minus tail, got {other:?}"),
        }
    }

    #[test]
    fn test_chain_modifier_on_non_tail_rejected() {
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?a p1 ?b . ?b p2 ?c . MINUS { ?b p3 ?d }
        // MINUS targets ?b (not the tail ?c) — should be rejected.
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
            Pattern::Minus(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(b),
                p3.clone(),
                Term::Var(d),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        assert!(
            try_build_count_plan(&query, &options).is_none(),
            "Chain + modifier on non-tail var should be rejected"
        );
    }

    #[test]
    fn test_single_minus_multi_predicate() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");
        let o2 = vars.get_or_insert("?o2");
        let o3 = vars.get_or_insert("?o3");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        // ?s p1 ?o . MINUS { ?s p2 ?o2 . ?s p3 ?o3 }
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p1.clone(), Term::Var(o))),
            Pattern::Minus(vec![
                Pattern::Triple(TriplePattern::new(Ref::Var(s), p2.clone(), Term::Var(o2))),
                Pattern::Triple(TriplePattern::new(Ref::Var(s), p3.clone(), Term::Var(o3))),
            ]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect single + MINUS(star)");

        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::Sum {
                source: StreamNode::AntiJoin { source, excluded },
            }) => {
                assert!(matches!(
                    source.as_ref(),
                    StreamNode::SubjectCountScan { .. }
                ));
                // Multi-predicate MINUS → IntersectSorted of SubjectsSorted
                assert!(
                    matches!(excluded, KeySetNode::IntersectSorted { children } if children.len() == 2)
                );
            }
            other => panic!("Expected Sum(AntiJoin(Scan, IntersectSorted)), got {other:?}"),
        }
    }

    #[test]
    fn test_self_loop_rejected() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let count = vars.get_or_insert("?count");

        let pred = make_sid(1, "p1");
        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            pred.clone(),
            Term::Var(s), // self-loop
        ))];

        let (query, options) = make_query(patterns, count);
        assert!(try_build_count_plan(&query, &options).is_none());
    }

    #[test]
    fn test_unsupported_pattern_rejected() {
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");
        let count = vars.get_or_insert("?count");

        let pred = make_sid(1, "p1");
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(s), pred.clone(), Term::Var(o))),
            Pattern::Bind {
                var: o,
                expr: crate::ir::Expression::Const(crate::ir::FlakeValue::Long(42)),
            },
        ];

        let (query, options) = make_query(patterns, count);
        assert!(try_build_count_plan(&query, &options).is_none());
    }

    // =======================================================================
    // Phase D: Object-chain patterns
    // =======================================================================

    #[test]
    fn test_object_chain_exists_single() {
        // ?a <p1> ?b . EXISTS { ?b <p2> ?c }
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Exists(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(b),
                p2.clone(),
                Term::Var(c),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect object-chain EXISTS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::PostObjectFilteredSum {
                object_filter: KeySetNode::SubjectsSorted { .. },
                ..
            }) => {}
            other => panic!("Expected PostObjectFilteredSum with SubjectsSorted, got {other:?}"),
        }
    }

    #[test]
    fn test_object_chain_minus_single() {
        // ?a <p1> ?b . MINUS { ?b <p2> ?c }
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Minus(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(b),
                p2.clone(),
                Term::Var(c),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect object-chain MINUS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::TotalMinusPostObjectFilteredSum {
                excluded_objects: KeySetNode::SubjectsSorted { .. },
                ..
            }) => {}
            other => panic!(
                "Expected TotalMinusPostObjectFilteredSum with SubjectsSorted, got {other:?}"
            ),
        }
    }

    #[test]
    fn test_object_chain_exists_2hop() {
        // ?a <p1> ?b . EXISTS { ?b <p2> ?c . ?c <p3> ?d }
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Exists(vec![
                Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
                Pattern::Triple(TriplePattern::new(Ref::Var(c), p3.clone(), Term::Var(d))),
            ]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect 2-hop object-chain EXISTS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::PostObjectFilteredSum {
                object_filter: KeySetNode::SubjectsWithObjectIn { object_set, .. },
                ..
            }) => {
                assert!(matches!(object_set.as_ref(), KeySetNode::SubjectSet { .. }));
            }
            other => {
                panic!("Expected PostObjectFilteredSum with SubjectsWithObjectIn, got {other:?}")
            }
        }
    }

    #[test]
    fn test_object_chain_minus_2hop() {
        // ?a <p1> ?b . MINUS { ?b <p2> ?c . ?c <p3> ?d }
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Minus(vec![
                Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
                Pattern::Triple(TriplePattern::new(Ref::Var(c), p3.clone(), Term::Var(d))),
            ]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect 2-hop object-chain MINUS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::TotalMinusPostObjectFilteredSum { .. }) => {}
            other => panic!("Expected TotalMinusPostObjectFilteredSum, got {other:?}"),
        }
    }

    #[test]
    fn test_object_chain_not_exists() {
        // ?a <p1> ?b . NOT EXISTS { ?b <p2> ?c } → same as MINUS
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::NotExists(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(b),
                p2.clone(),
                Term::Var(c),
            ))]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect object-chain NOT EXISTS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::TotalMinusPostObjectFilteredSum { .. }) => {}
            other => panic!("Expected TotalMinusPostObjectFilteredSum, got {other:?}"),
        }
    }

    #[test]
    fn test_object_chain_star_modifier() {
        // ?a <p1> ?b . EXISTS { ?b <p2> ?c . ?b <p3> ?d } — star on object
        let mut vars = VarRegistry::new();
        let a = vars.get_or_insert("?a");
        let b = vars.get_or_insert("?b");
        let c = vars.get_or_insert("?c");
        let d = vars.get_or_insert("?d");
        let count = vars.get_or_insert("?count");

        let p1 = make_sid(1, "p1");
        let p2 = make_sid(2, "p2");
        let p3 = make_sid(3, "p3");

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(a), p1.clone(), Term::Var(b))),
            Pattern::Exists(vec![
                Pattern::Triple(TriplePattern::new(Ref::Var(b), p2.clone(), Term::Var(c))),
                Pattern::Triple(TriplePattern::new(Ref::Var(b), p3.clone(), Term::Var(d))),
            ]),
        ];

        let (query, options) = make_query(patterns, count);
        let plan = try_build_count_plan(&query, &options);
        assert!(plan.is_some(), "Should detect star-on-object EXISTS");
        match &plan.unwrap().root {
            CountPlanRoot::Scalar(ScalarNode::PostObjectFilteredSum {
                object_filter: KeySetNode::IntersectSorted { children },
                ..
            }) => {
                assert_eq!(children.len(), 2);
            }
            other => panic!("Expected PostObjectFilteredSum with IntersectSorted, got {other:?}"),
        }
    }
}
