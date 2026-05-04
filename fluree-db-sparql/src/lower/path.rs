//! Property path lowering.
//!
//! Converts SPARQL property path patterns (e.g., `?s ex:parent+ ?o`) to the
//! query engine's pattern representation. Supports:
//! - one-or-more (`+`) and zero-or-more (`*`) → `PropertyPathPattern`
//! - inverse (`^`) → `TriplePattern` with subject/object swapped
//! - alternative (`|`) → `Union` of triple branches
//! - sequence (`/`) → chain of `TriplePattern` joined by `?__pp{n}` variables

use std::sync::Arc;

use crate::ast::path::PropertyPath as SparqlPropertyPath;
use crate::ast::term::{ObjectTerm, SubjectTerm};
use crate::span::SourceSpan;

use fluree_db_query::ir::triple::{Ref, TriplePattern};
use fluree_db_query::ir::{PathModifier, Pattern, PropertyPathPattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_vocab::rdf::TYPE;

use super::{LowerError, LoweringContext, Result};

/// Maximum number of expanded chains when distributing alternatives in a
/// sequence path. Prevents combinatorial explosion from expressions like
/// `(a|b)/(c|d)/(e|f)/...`.
const MAX_SEQUENCE_EXPANSION: usize = 64;

impl<E: IriEncoder> LoweringContext<'_, E> {
    pub(super) fn lower_property_path(
        &mut self,
        subject: &SubjectTerm,
        path: &SparqlPropertyPath,
        object: &ObjectTerm,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        // Lower subject term (already a Ref, so no literal validation needed)
        let s = self.lower_subject(subject)?;

        // Lower object term and convert to Ref (fails on literal values)
        let o_term = self.lower_object(object)?;
        let o = Ref::try_from(o_term).map_err(|_| {
            LowerError::invalid_property_path(
                "Property path object cannot be a literal value",
                span,
            )
        })?;

        self.lower_path_dispatch(&s, path, &o, span)
    }

    /// Dispatch on path type to produce the appropriate pattern(s).
    fn lower_path_dispatch(
        &mut self,
        s: &Ref,
        path: &SparqlPropertyPath,
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        // Rewrite complex inverses (^(a/b), ^(a|b), ^(^x)) before dispatching.
        let rewritten;
        let effective_path = if let Some(r) = Self::rewrite_inverse_of_complex(path) {
            rewritten = r;
            &rewritten
        } else {
            path
        };

        match effective_path {
            // Transitive: one-or-more / zero-or-more → PropertyPathPattern
            SparqlPropertyPath::OneOrMore { path: inner, .. }
            | SparqlPropertyPath::ZeroOrMore { path: inner, .. } => {
                // Both-constants check only applies to transitive paths
                if s.is_bound() && o.is_bound() {
                    return Err(LowerError::invalid_property_path(
                        "Property path requires at least one variable (cannot have both subject and object as constants)",
                        span,
                    ));
                }
                let iri = self.extract_simple_predicate_iri(inner, span)?;
                let modifier = match effective_path {
                    SparqlPropertyPath::OneOrMore { .. } => PathModifier::OneOrMore,
                    _ => PathModifier::ZeroOrMore,
                };
                let predicate_sid = self
                    .encoder
                    .encode_iri(&iri)
                    .ok_or_else(|| LowerError::unknown_namespace(&iri, span))?;
                let pp = PropertyPathPattern::new(s.clone(), predicate_sid, modifier, o.clone());
                Ok(vec![Pattern::PropertyPath(pp)])
            }

            // Inverse: ^path
            SparqlPropertyPath::Inverse { path: inner, .. } => match inner.as_ref() {
                // Inverse-transitive: ^p+ or ^p* → PropertyPathPattern with swapped s/o
                SparqlPropertyPath::OneOrMore { path: tp_inner, .. }
                | SparqlPropertyPath::ZeroOrMore { path: tp_inner, .. } => {
                    if s.is_bound() && o.is_bound() {
                        return Err(LowerError::invalid_property_path(
                            "Property path requires at least one variable (cannot have both subject and object as constants)",
                            span,
                        ));
                    }
                    let iri = self.extract_simple_predicate_iri(tp_inner, span)?;
                    let modifier = match inner.as_ref() {
                        SparqlPropertyPath::OneOrMore { .. } => PathModifier::OneOrMore,
                        _ => PathModifier::ZeroOrMore,
                    };
                    let predicate_sid = self
                        .encoder
                        .encode_iri(&iri)
                        .ok_or_else(|| LowerError::unknown_namespace(&iri, span))?;
                    // Swap subject/object for inverse traversal
                    let pp =
                        PropertyPathPattern::new(o.clone(), predicate_sid, modifier, s.clone());
                    Ok(vec![Pattern::PropertyPath(pp)])
                }
                // Simple inverse: ^p → Triple with s/o swapped
                _ => {
                    let iri = self.extract_simple_predicate_iri(inner, span)?;
                    let p = Ref::Iri(Arc::from(iri.as_str()));
                    Ok(vec![Pattern::Triple(TriplePattern::new(
                        o.clone(),
                        p,
                        s.clone().into(),
                    ))])
                }
            },

            // Alternative: path|path → Union of triple branches
            SparqlPropertyPath::Alternative { .. } => {
                let mut leaves = Vec::new();
                Self::flatten_alternatives(effective_path, &mut leaves);
                let branches: Vec<Vec<Pattern>> = leaves
                    .into_iter()
                    .map(|leaf| self.lower_alternative_branch(leaf, s, o, span))
                    .collect::<Result<_>>()?;
                Ok(vec![Pattern::Union(branches)])
            }

            // Simple IRI without modifier shouldn't appear as a path pattern
            SparqlPropertyPath::Iri(_) => Err(LowerError::invalid_property_path(
                "Simple predicate should be a triple pattern, not a property path. Use + or * modifier for transitive paths.",
                span,
            )),

            SparqlPropertyPath::A { span: a_span } => Err(LowerError::invalid_property_path(
                "Simple 'a' predicate should be a triple pattern, not a property path. Use + or * modifier for transitive paths.",
                *a_span,
            )),

            // Parsed but not yet supported
            SparqlPropertyPath::ZeroOrOne { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Optional (?) property paths",
                    *op_span,
                ))
            }
            // Sequence: path/path → chain of triple patterns joined by ?__pp{n} variables
            SparqlPropertyPath::Sequence { .. } => {
                let mut steps = Vec::new();
                Self::flatten_sequence(effective_path, &mut steps);
                self.lower_sequence_chain(s, &steps, o, span)
            }
            SparqlPropertyPath::NegatedSet { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Negated property sets (!)",
                    *op_span,
                ))
            }

            // Grouped path - unwrap and recurse
            SparqlPropertyPath::Group { path: inner, .. } => {
                self.lower_path_dispatch(s, inner, o, span)
            }
        }
    }

    /// Flatten a binary `Alternative` tree into a list of leaf paths.
    ///
    /// SPARQL parses `a|b|c` as `Alternative(Alternative(a, b), c)`.
    /// This collects all leaves into a flat vec: `[a, b, c]`.
    fn flatten_alternatives<'p>(
        path: &'p SparqlPropertyPath,
        out: &mut Vec<&'p SparqlPropertyPath>,
    ) {
        match path {
            SparqlPropertyPath::Alternative { left, right, .. } => {
                Self::flatten_alternatives(left, out);
                Self::flatten_alternatives(right, out);
            }
            SparqlPropertyPath::Group { path: inner, .. } => {
                Self::flatten_alternatives(inner, out);
            }
            other => out.push(other),
        }
    }

    /// Flatten a binary `Sequence` tree into a list of leaf paths.
    ///
    /// SPARQL parses `a/b/c` as `Sequence(Sequence(a, b), c)`.
    /// This collects all leaves into a flat vec: `[a, b, c]`.
    fn flatten_sequence<'p>(path: &'p SparqlPropertyPath, out: &mut Vec<&'p SparqlPropertyPath>) {
        match path {
            SparqlPropertyPath::Sequence { left, right, .. } => {
                Self::flatten_sequence(left, out);
                Self::flatten_sequence(right, out);
            }
            SparqlPropertyPath::Group { path: inner, .. } => {
                Self::flatten_sequence(inner, out);
            }
            other => out.push(other),
        }
    }

    /// Lower a sequence path into a chain of triple patterns.
    ///
    /// Each step produces a triple, with adjacent steps joined by generated
    /// intermediate variables (`?__pp0`, `?__pp1`, …).
    ///
    /// Example: `[a, b, c]` with subject `?s` and object `?o`:
    /// ```text
    ///   Triple(?s,     a, ?__pp0)
    ///   Triple(?__pp0, b, ?__pp1)
    ///   Triple(?__pp1, c, ?o)
    /// ```
    ///
    /// Alternative steps (`(a|b)`) are distributed into a `Union` of simple chains.
    fn lower_sequence_chain(
        &mut self,
        s: &Ref,
        steps: &[&SparqlPropertyPath],
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        debug_assert!(
            !steps.is_empty(),
            "BUG: flatten_sequence produced empty step list"
        );

        // Degenerate single-step sequence (shouldn't happen, but guard)
        if steps.len() == 1 {
            return self.lower_sequence_step(steps[0], s, o, span);
        }

        // Build step choices: each step → vec of simple alternatives.
        // Non-alternative steps → single-element vec. Alternative steps → branches.
        let mut step_choices: Vec<Vec<&SparqlPropertyPath>> = Vec::with_capacity(steps.len());
        let mut has_alt = false;

        for step in steps {
            let unwrapped = Self::unwrap_group(step);
            match unwrapped {
                SparqlPropertyPath::Alternative { .. } => {
                    has_alt = true;
                    let leaves = Self::collect_alt_leaves(unwrapped);
                    for leaf in &leaves {
                        Self::validate_simple_sparql_step(leaf, span)?;
                    }
                    step_choices.push(leaves);
                }
                _ => {
                    step_choices.push(vec![unwrapped]);
                }
            }
        }

        if has_alt {
            return self.lower_distributed_sequence(s, &step_choices, o, span);
        }

        // Fast path: no alternatives — generate simple triple chain
        let mut patterns = Vec::with_capacity(steps.len());
        let mut prev = s.clone();

        for (i, step) in steps.iter().enumerate() {
            let is_last = i == steps.len() - 1;
            let next = if is_last {
                o.clone()
            } else {
                let var_name = format!("?__pp{}", self.pp_counter);
                self.pp_counter += 1;
                Ref::Var(self.vars.get_or_insert(&var_name))
            };

            let pat = self.lower_sequence_step_pattern(step, &prev, &next, span)?;
            patterns.push(pat);

            prev = next;
        }

        Ok(patterns)
    }

    /// Recursively strip `Group` wrappers from a SPARQL property path.
    ///
    /// SPARQL AST wraps parenthesized expressions in `Group { path, .. }`.
    /// E.g., `(ex:name|ex:nick)` parses as `Group { path: Alternative { .. } }`.
    fn unwrap_group(path: &SparqlPropertyPath) -> &SparqlPropertyPath {
        match path {
            SparqlPropertyPath::Group { path: inner, .. } => Self::unwrap_group(inner),
            other => other,
        }
    }

    /// Recursively rewrite `Inverse(Sequence/Alternative)` so that `Inverse`
    /// only appears directly around leaf IRIs. Also cancels double-inverse
    /// (`^(^x)` → `x`).
    ///
    /// Rewrite rules (applied recursively):
    ///   `^(^x)`    → `normalize(x)` — double-inverse cancellation
    ///   `^(a/b/c)` → `(^c)/(^b)/(^a)` — reverse sequence, invert each step
    ///   `^(a|b|c)` → `(^a)|(^b)|(^c)` — distribute inverse into each branch
    ///
    /// Returns `Some(rewritten)` if the input was a complex inverse,
    /// `None` if no rewrite was needed (simple/transitive inverse).
    fn rewrite_inverse_of_complex(path: &SparqlPropertyPath) -> Option<SparqlPropertyPath> {
        match path {
            SparqlPropertyPath::Inverse { path: inner, span } => {
                let unwrapped = Self::unwrap_group(inner);
                match unwrapped {
                    // Double inverse: ^(^x) → x (then normalize)
                    SparqlPropertyPath::Inverse {
                        path: inner_inner, ..
                    } => {
                        let cancelled = inner_inner.as_ref().clone();
                        Some(Self::rewrite_inverse_of_complex(&cancelled).unwrap_or(cancelled))
                    }
                    // ^(a/b/c) → (^c)/(^b)/(^a)
                    SparqlPropertyPath::Sequence { .. } => {
                        let mut steps = Vec::new();
                        Self::flatten_sequence(unwrapped, &mut steps);
                        let inv_steps: Vec<SparqlPropertyPath> = steps
                            .into_iter()
                            .rev()
                            .map(|step| {
                                let inv = SparqlPropertyPath::Inverse {
                                    path: Box::new(step.clone()),
                                    span: *span,
                                };
                                Self::rewrite_inverse_of_complex(&inv).unwrap_or(inv)
                            })
                            .collect();
                        Some(Self::build_sequence_from_vec(inv_steps, *span))
                    }
                    // ^(a|b|c) → (^a)|(^b)|(^c)
                    SparqlPropertyPath::Alternative { .. } => {
                        let mut leaves = Vec::new();
                        Self::flatten_alternatives(unwrapped, &mut leaves);
                        let inv_leaves: Vec<SparqlPropertyPath> = leaves
                            .into_iter()
                            .map(|leaf| {
                                let inv = SparqlPropertyPath::Inverse {
                                    path: Box::new(leaf.clone()),
                                    span: *span,
                                };
                                Self::rewrite_inverse_of_complex(&inv).unwrap_or(inv)
                            })
                            .collect();
                        Some(Self::build_alternative_from_vec(inv_leaves, *span))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Build a right-associative `Sequence` binary tree from a vec of paths.
    ///
    /// `[a, b, c]` → `Sequence(a, Sequence(b, c))`
    fn build_sequence_from_vec(
        paths: Vec<SparqlPropertyPath>,
        span: SourceSpan,
    ) -> SparqlPropertyPath {
        debug_assert!(paths.len() >= 2, "BUG: sequence needs at least 2 steps");
        let mut iter = paths.into_iter().rev();
        let mut acc = iter.next().unwrap();
        for step in iter {
            acc = SparqlPropertyPath::Sequence {
                left: Box::new(step),
                right: Box::new(acc),
                span,
            };
        }
        acc
    }

    /// Build a right-associative `Alternative` binary tree from a vec of paths.
    ///
    /// `[a, b, c]` → `Alternative(a, Alternative(b, c))`
    fn build_alternative_from_vec(
        paths: Vec<SparqlPropertyPath>,
        span: SourceSpan,
    ) -> SparqlPropertyPath {
        debug_assert!(
            paths.len() >= 2,
            "BUG: alternative needs at least 2 branches"
        );
        let mut iter = paths.into_iter().rev();
        let mut acc = iter.next().unwrap();
        for branch in iter {
            acc = SparqlPropertyPath::Alternative {
                left: Box::new(branch),
                right: Box::new(acc),
                span,
            };
        }
        acc
    }

    /// Collect the leaf paths from an `Alternative` binary tree.
    fn collect_alt_leaves(path: &SparqlPropertyPath) -> Vec<&SparqlPropertyPath> {
        let mut leaves = Vec::new();
        Self::flatten_alternatives(path, &mut leaves);
        leaves
    }

    /// Validate that a leaf inside an Alternative (within a sequence step) is a
    /// "simple" step: `Iri(_)`, `A`, `Inverse { path: Iri(_) | A }`, or a transitive
    /// modifier (`+`/`*`) applied to a simple predicate (and inverse of those).
    fn validate_simple_sparql_step(step: &SparqlPropertyPath, span: SourceSpan) -> Result<()> {
        let unwrapped = Self::unwrap_group(step);
        match unwrapped {
            SparqlPropertyPath::Iri(_) | SparqlPropertyPath::A { .. } => Ok(()),
            SparqlPropertyPath::OneOrMore { path: inner, .. }
            | SparqlPropertyPath::ZeroOrMore { path: inner, .. } => {
                let inner_unwrapped = Self::unwrap_group(inner);
                match inner_unwrapped {
                    SparqlPropertyPath::Iri(_) | SparqlPropertyPath::A { .. } => Ok(()),
                    other => Err(LowerError::invalid_property_path(
                        format!(
                            "Transitive steps within a sequence must apply +/* to a simple predicate; got {}",
                            sparql_path_name(other),
                        ),
                        span,
                    )),
                }
            }
            SparqlPropertyPath::Inverse { path: inner, .. } => {
                let inner_unwrapped = Self::unwrap_group(inner);
                match inner_unwrapped {
                    SparqlPropertyPath::Iri(_) | SparqlPropertyPath::A { .. } => Ok(()),
                    SparqlPropertyPath::OneOrMore { path: tp_inner, .. }
                    | SparqlPropertyPath::ZeroOrMore { path: tp_inner, .. } => {
                        let tp_unwrapped = Self::unwrap_group(tp_inner);
                        match tp_unwrapped {
                            SparqlPropertyPath::Iri(_) | SparqlPropertyPath::A { .. } => Ok(()),
                            other => Err(LowerError::invalid_property_path(
                                format!(
                                    "Transitive inverse steps within a sequence must apply +/* to a simple predicate; got inverse of {}",
                                    sparql_path_name(other),
                                ),
                                span,
                            )),
                        }
                    }
                    other => Err(LowerError::invalid_property_path(
                        format!(
                            "Alternative steps within a sequence must be simple predicates or \
                             inverse simple predicates (^ex:p); got inverse of {}",
                            sparql_path_name(other),
                        ),
                        span,
                    )),
                }
            }
            other => Err(LowerError::invalid_property_path(
                format!(
                    "Alternative steps within a sequence must be simple predicates or \
                     inverse simple predicates (^ex:p); got {}",
                    sparql_path_name(other),
                ),
                span,
            )),
        }
    }

    /// Compute the Cartesian product of SPARQL step choices.
    fn cartesian_product_sparql<'p>(
        step_choices: &'p [Vec<&'p SparqlPropertyPath>],
    ) -> Vec<Vec<&'p SparqlPropertyPath>> {
        let mut result: Vec<Vec<&SparqlPropertyPath>> = vec![vec![]];
        for choices in step_choices {
            let mut new_result = Vec::new();
            for existing in &result {
                for choice in choices {
                    let mut combo = existing.clone();
                    combo.push(choice);
                    new_result.push(combo);
                }
            }
            result = new_result;
        }
        result
    }

    /// Lower a distributed sequence (containing alternative steps) into a Union.
    ///
    /// Expands the Cartesian product of step choices into individual simple chains,
    /// each becoming a branch of a `Pattern::Union`.
    fn lower_distributed_sequence(
        &mut self,
        s: &Ref,
        step_choices: &[Vec<&SparqlPropertyPath>],
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        let combos = Self::cartesian_product_sparql(step_choices);
        let n = combos.len();
        if n > MAX_SEQUENCE_EXPANSION {
            return Err(LowerError::invalid_property_path(
                format!(
                    "Property path sequence expands to {n} chains (limit {MAX_SEQUENCE_EXPANSION})",
                ),
                span,
            ));
        }

        let branches: Vec<Vec<Pattern>> = combos
            .into_iter()
            .map(|combo| self.lower_simple_sequence_chain(s, &combo, o, span))
            .collect::<Result<_>>()?;

        Ok(vec![Pattern::Union(branches)])
    }

    /// Lower a simple sequence chain (no alternative steps) to a list of triple patterns.
    ///
    /// Each step must be a simple predicate. Adjacent steps are joined by
    /// generated intermediate variables (`?__pp{n}`).
    fn lower_simple_sequence_chain(
        &mut self,
        s: &Ref,
        steps: &[&SparqlPropertyPath],
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        let mut patterns = Vec::with_capacity(steps.len());
        let mut prev = s.clone();

        for (i, step) in steps.iter().enumerate() {
            let is_last = i == steps.len() - 1;
            let next = if is_last {
                o.clone()
            } else {
                let var_name = format!("?__pp{}", self.pp_counter);
                self.pp_counter += 1;
                Ref::Var(self.vars.get_or_insert(&var_name))
            };

            let pat = self.lower_sequence_step_pattern(step, &prev, &next, span)?;
            patterns.push(pat);

            prev = next;
        }

        Ok(patterns)
    }

    /// Lower a single sequence step to a pattern (`Triple` or `PropertyPath`).
    ///
    /// Forward step `p`: `Triple(prev, p, next)`
    /// Inverse step `^p`: `Triple(next, p, prev)` (swapped)
    /// Transitive step `p+`/`p*`: `PropertyPath(prev, p, next)`
    /// Inverse-transitive step `^p+`/`^p*`: `PropertyPath(next, p, prev)` (swapped)
    ///
    /// Note: Alternative steps (`(a|b)`) are handled by distribution in
    /// `lower_sequence_chain` before this method is called.
    fn lower_sequence_step_pattern(
        &mut self,
        step: &SparqlPropertyPath,
        prev: &Ref,
        next: &Ref,
        span: SourceSpan,
    ) -> Result<Pattern> {
        match step {
            SparqlPropertyPath::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                let p = Ref::Iri(Arc::from(expanded.as_str()));
                Ok(Pattern::Triple(TriplePattern::new(
                    prev.clone(),
                    p,
                    next.clone().into(),
                )))
            }
            SparqlPropertyPath::A { .. } => {
                let p = Ref::Iri(Arc::from(TYPE));
                Ok(Pattern::Triple(TriplePattern::new(
                    prev.clone(),
                    p,
                    next.clone().into(),
                )))
            }
            SparqlPropertyPath::Inverse { path: inner, .. } => {
                match inner.as_ref() {
                    SparqlPropertyPath::OneOrMore { path: tp_inner, .. }
                    | SparqlPropertyPath::ZeroOrMore { path: tp_inner, .. } => {
                        // Inverse-transitive: ^p+ or ^p*
                        if prev.is_bound() && next.is_bound() {
                            return Err(LowerError::invalid_property_path(
                                "Property path requires at least one variable (cannot have both subject and object as constants)",
                                span,
                            ));
                        }
                        let iri = self.extract_simple_predicate_iri(tp_inner, span)?;
                        let modifier = match inner.as_ref() {
                            SparqlPropertyPath::OneOrMore { .. } => PathModifier::OneOrMore,
                            _ => PathModifier::ZeroOrMore,
                        };
                        let predicate_sid = self
                            .encoder
                            .encode_iri(&iri)
                            .ok_or_else(|| LowerError::unknown_namespace(&iri, span))?;
                        Ok(Pattern::PropertyPath(PropertyPathPattern::new(
                            next.clone(),
                            predicate_sid,
                            modifier,
                            prev.clone(),
                        )))
                    }
                    _ => {
                        // Simple inverse: ^p → Triple with s/o swapped
                        let iri = self.extract_simple_predicate_iri(inner, span)?;
                        let p = Ref::Iri(Arc::from(iri.as_str()));
                        Ok(Pattern::Triple(TriplePattern::new(
                            next.clone(),
                            p,
                            prev.clone().into(),
                        )))
                    }
                }
            }
            SparqlPropertyPath::OneOrMore { path: inner, .. }
            | SparqlPropertyPath::ZeroOrMore { path: inner, .. } => {
                if prev.is_bound() && next.is_bound() {
                    return Err(LowerError::invalid_property_path(
                        "Property path requires at least one variable (cannot have both subject and object as constants)",
                        span,
                    ));
                }
                let iri = self.extract_simple_predicate_iri(inner, span)?;
                let modifier = match step {
                    SparqlPropertyPath::OneOrMore { .. } => PathModifier::OneOrMore,
                    _ => PathModifier::ZeroOrMore,
                };
                let predicate_sid = self
                    .encoder
                    .encode_iri(&iri)
                    .ok_or_else(|| LowerError::unknown_namespace(&iri, span))?;
                Ok(Pattern::PropertyPath(PropertyPathPattern::new(
                    prev.clone(),
                    predicate_sid,
                    modifier,
                    next.clone(),
                )))
            }
            other => Err(LowerError::invalid_property_path(
                format!(
                    "Sequence (/) steps must be simple predicates, inverse simple predicates \
                     (^ex:p), transitive predicates (ex:p+ or ex:p*), or alternatives of simple \
                     predicates ((ex:a|ex:b)); got {}",
                    sparql_path_name(other),
                ),
                span,
            )),
        }
    }

    /// Lower a degenerate single-step sequence to a pattern list.
    fn lower_sequence_step(
        &mut self,
        step: &SparqlPropertyPath,
        s: &Ref,
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        let pat = self.lower_sequence_step_pattern(step, s, o, span)?;
        Ok(vec![pat])
    }

    /// Lower a single branch of an alternative path to pattern(s).
    ///
    /// Supports simple IRIs (forward), inverse of simple IRIs, and sequence chains.
    fn lower_alternative_branch(
        &mut self,
        leaf: &SparqlPropertyPath,
        s: &Ref,
        o: &Ref,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        match leaf {
            SparqlPropertyPath::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                let p = Ref::Iri(Arc::from(expanded.as_str()));
                Ok(vec![Pattern::Triple(TriplePattern::new(
                    s.clone(),
                    p,
                    o.clone().into(),
                ))])
            }
            SparqlPropertyPath::A { .. } => {
                let p = Ref::Iri(Arc::from(TYPE));
                Ok(vec![Pattern::Triple(TriplePattern::new(
                    s.clone(),
                    p,
                    o.clone().into(),
                ))])
            }
            SparqlPropertyPath::Inverse { path: inner, .. } => {
                let iri = self.extract_simple_predicate_iri(inner, span)?;
                let p = Ref::Iri(Arc::from(iri.as_str()));
                Ok(vec![Pattern::Triple(TriplePattern::new(
                    o.clone(),
                    p,
                    s.clone().into(),
                ))])
            }
            SparqlPropertyPath::Sequence { .. } => {
                let mut steps = Vec::new();
                Self::flatten_sequence(leaf, &mut steps);
                self.lower_sequence_chain(s, &steps, o, span)
            }
            other => Err(LowerError::invalid_property_path(
                format!(
                    "Alternative (|) branches support simple predicates, inverse simple \
                     predicates (^ex:p), or sequence chains (ex:a/ex:b); got {}",
                    sparql_path_name(other),
                ),
                span,
            )),
        }
    }

    /// Extract a simple predicate IRI from a property path.
    ///
    /// The path must be a simple IRI or the `a` keyword.
    fn extract_simple_predicate_iri(
        &mut self,
        path: &SparqlPropertyPath,
        span: SourceSpan,
    ) -> Result<String> {
        match path {
            SparqlPropertyPath::Iri(iri) => self.expand_iri(iri),

            SparqlPropertyPath::A { .. } => {
                // `a` is shorthand for rdf:type
                Ok(TYPE.to_string())
            }

            SparqlPropertyPath::Group { path: inner, .. } => {
                self.extract_simple_predicate_iri(inner, span)
            }

            // Any other path form is not a simple predicate
            _ => Err(LowerError::invalid_property_path(
                "Transitive paths (+, *) require a simple predicate IRI, not a complex path expression",
                span,
            )),
        }
    }
}

/// Human-readable name for a SPARQL property path variant (for error messages).
fn sparql_path_name(path: &SparqlPropertyPath) -> &'static str {
    match path {
        SparqlPropertyPath::Iri(_) => "IRI",
        SparqlPropertyPath::A { .. } => "a (rdf:type)",
        SparqlPropertyPath::Inverse { .. } => "Inverse (^)",
        SparqlPropertyPath::Sequence { .. } => "Sequence (/)",
        SparqlPropertyPath::Alternative { .. } => "Alternative (|)",
        SparqlPropertyPath::ZeroOrMore { .. } => "ZeroOrMore (*)",
        SparqlPropertyPath::OneOrMore { .. } => "OneOrMore (+)",
        SparqlPropertyPath::ZeroOrOne { .. } => "ZeroOrOne (?)",
        SparqlPropertyPath::NegatedSet { .. } => "NegatedSet (!)",
        SparqlPropertyPath::Group { .. } => "Group (())",
    }
}
