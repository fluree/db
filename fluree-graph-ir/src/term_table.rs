//! Term interning shared by the collecting sinks
//!
//! Every sink that materializes events into IR needs the same three things:
//! a table mapping [`TermId`] to [`Term`], stable identity for labeled blank
//! nodes, and statement-scoped recycling of literal slots. This module holds
//! that machinery once so [`GraphCollectorSink`](crate::GraphCollectorSink)
//! and [`DatasetCollectorSink`](crate::DatasetCollectorSink) cannot drift
//! apart on term lifetime — the invariant that makes recycling sound.

use crate::sink::TermId;
use crate::{Datatype, LiteralValue, Term};
use std::collections::HashMap;

/// The term table behind a collecting sink.
///
/// Term ids fall into two lifetime classes and this type is what enforces the
/// difference; see [`TermId`] for the contract producers must honor.
#[derive(Debug, Default)]
pub(crate) struct TermTable {
    /// Terms indexed by TermId
    terms: Vec<Term>,
    /// Counter for generating blank node IDs
    blank_counter: u32,
    /// Cache for blank node labels to TermId mapping
    blank_labels: HashMap<String, TermId>,
    /// Slots in `terms` that hold literals, in mint order.
    ///
    /// Literals are the only statement-scoped terms (see [`TermId`]): they
    /// are minted per occurrence and never deduplicated, while IRI and blank
    /// ids are cached by producers for the whole parse. `emit_*` clones a
    /// term into its graph, so once the statement ends its literal slots hold
    /// nothing anyone can still reference and may be overwritten.
    literal_slots: Vec<u32>,
    /// How far into `literal_slots` the current statement has consumed.
    /// Reset to 0 by [`TermTable::end_statement`], which is what turns the
    /// list into a per-statement ring instead of a per-document one.
    literal_cursor: usize,
}

impl TermTable {
    /// Create an empty table
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// How many term slots are live.
    ///
    /// With recycling this tracks the widest single statement, not the
    /// document — which is the property the sinks' recycling tests assert,
    /// and the only reason this accessor exists.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.terms.len()
    }

    /// Get a term by its ID
    pub(crate) fn get(&self, id: TermId) -> &Term {
        &self.terms[id.index() as usize]
    }

    /// Intern an IRI term and return its ID
    pub(crate) fn iri(&mut self, iri: &str) -> TermId {
        self.add_term(Term::iri(iri))
    }

    /// Intern a blank node and return its ID.
    ///
    /// A labeled blank keeps one id for the whole session, so repeated
    /// references to `_:x` are the same node. An anonymous blank mints a
    /// fresh label in a namespace no user-written label can occupy.
    pub(crate) fn blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            Some(l) => {
                // Check if we've seen this label before
                if let Some(&id) = self.blank_labels.get(l) {
                    return id;
                }

                // Create new blank node with this label
                let id = self.add_term(Term::blank(l));
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                // Anonymous blank node (`[ … ]`, collection spine nodes,
                // reifiers) — unique counter-based label. The leading '-'
                // keeps the minted namespace disjoint from every
                // user-written label: Turtle's BLANK_NODE_LABEL must start
                // with PN_CHARS_U | [0-9], so `_:-b1` can never lex, while
                // '-' stays legal medially so the label still serializes.
                //
                // Without it a document's own `_:b1` and the first mint are
                // the same `BlankId` and their nodes silently MERGE. Matches
                // `FlakeSink`/`ImportSink`, which carry the same fix for the
                // same reason.
                self.blank_counter += 1;
                let label = format!("-b{}", self.blank_counter);
                self.add_term(Term::blank(label))
            }
        }
    }

    /// Intern a literal from its lexical form
    pub(crate) fn literal(
        &mut self,
        value: &str,
        datatype: Datatype,
        language: Option<&str>,
    ) -> TermId {
        let term = match language {
            Some(lang) => Term::lang_string(value, lang),
            None if datatype.is_xsd_string() => Term::string(value),
            None => Term::typed(value, datatype),
        };
        self.add_literal_term(term)
    }

    /// Intern a literal from a native value
    pub(crate) fn literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        let term = Term::Literal {
            value,
            datatype,
            language: None,
        };
        self.add_literal_term(term)
    }

    /// Retire the current statement's literal slots for reuse.
    ///
    /// Called at both statement outcomes — commit and abort — because the
    /// slots are dead either way. Recycling is sound because `emit_*` has
    /// already cloned every term it needed, and because producers only cache
    /// IRI and blank ids across statements.
    pub(crate) fn end_statement(&mut self) {
        self.debug_assert_retiring_slots_are_literals();
        self.literal_cursor = 0;
    }

    /// Add a term and return its ID
    fn add_term(&mut self, term: Term) -> TermId {
        let id = TermId::new(self.terms.len() as u32);
        self.terms.push(term);
        id
    }

    /// Debug-only invariant: every slot the current statement is about to
    /// retire still holds a literal.
    ///
    /// `literal_slots` may only ever contain slots minted by
    /// [`Self::add_literal_term`]. If an IRI or blank term is found in one,
    /// something has routed a session-scoped term through the literal lane,
    /// and retiring the slot would hand a producer-cached id to the next
    /// literal — corrupting the graph silently and in a way that still looks
    /// well-formed. Fail loudly in debug builds instead; compiled out of
    /// release.
    fn debug_assert_retiring_slots_are_literals(&self) {
        if cfg!(debug_assertions) {
            for &slot in &self.literal_slots[..self.literal_cursor] {
                debug_assert!(
                    matches!(self.terms[slot as usize], Term::Literal { .. }),
                    "retiring non-literal slot {slot}: {:?} — literal_slots is polluted, \
                     recycling it would clobber a producer-cached term id",
                    self.terms[slot as usize]
                );
            }
        }
    }

    /// Add a literal term, reusing a slot retired by
    /// [`Self::end_statement`] when one is available.
    ///
    /// Without this, the term table grows by one entry per literal
    /// *occurrence* for the length of the document; with it, the high-water
    /// mark is the widest single statement. Producers that never delimit
    /// statements (the JSON-LD adapter) simply never recycle and keep
    /// today's behavior.
    fn add_literal_term(&mut self, term: Term) -> TermId {
        if let Some(&slot) = self.literal_slots.get(self.literal_cursor) {
            self.literal_cursor += 1;
            self.terms[slot as usize] = term;
            return TermId::new(slot);
        }
        let id = self.add_term(term);
        self.literal_slots.push(id.index());
        self.literal_cursor = self.literal_slots.len();
        id
    }
}
