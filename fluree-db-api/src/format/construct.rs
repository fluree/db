//! CONSTRUCT query output formatter
//!
//! Transforms query results into JSON-LD graph format using the Graph IR:
//! 1. Instantiate template patterns with result bindings -> Graph (expanded IRIs)
//! 2. Format Graph to JSON-LD (compacting at output time)
//!
//! The Graph IR stores all IRIs in expanded form. Compaction to prefixed form
//! happens only at the final JSON-LD formatting step.

use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::triple::{Ref, Term};
use fluree_db_query::ir::ConstructTemplate;
use fluree_db_query::{Batch, VarId};
use fluree_graph_format::{format_jsonld_dataset, JsonLdFormatConfig};
use fluree_graph_ir::{BlankId, Dataset, Datatype, LiteralValue, Term as IrTerm, Triple};
use fluree_vocab::{geo, rdf, xsd};
use rustc_hash::FxHashMap;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Format CONSTRUCT query results as JSON-LD graph
///
/// This function:
/// 1. Instantiates the CONSTRUCT template with query bindings to produce a Graph
/// 2. Formats the Graph to JSON-LD using the original @context for compaction
///
/// # Arguments
///
/// * `result` - Query result with construct_template populated
/// * `compactor` - IRI compactor for decoding Sids and compacting output
///
/// # Returns
///
/// JSON-LD graph: `{"@context": ..., "@graph": [...]}`
pub fn format(result: &QueryResult, compactor: &IriCompactor) -> Result<JsonValue> {
    // 1. Build the dataset from template instantiation
    let mut dataset = instantiate_construct_graph(result, compactor)?;

    // Sort for deterministic output, and apply RDF set semantics: a CONSTRUCT
    // result is a graph, so a template instantiated to the same (s, p, o) by
    // several solution rows contributes one triple (SPARQL 1.1 §16.2).
    // `Term`'s equality compares value, datatype AND language, so this is real
    // RDF term identity — `"1"^^xsd:integer` and `"1"^^xsd:string` stay
    // distinct. Caveat: a NaN `xsd:double` object never compares equal to
    // itself, so repeated NaN triples are not collapsed.
    dataset.canonicalize();

    // 2. Format to JSON-LD using CONSTRUCT parity settings.
    //    Use the precomputed ContextCompactor so we don't rebuild the
    //    reverse lookup for every IRI.
    let ctx_compactor = compactor.ctx_compactor().clone();
    let ctx_compactor_id = ctx_compactor.clone();
    let config = JsonLdFormatConfig::construct_parity(
        result.orig_context.clone(),
        // Predicates and @type values: vocab=true (allow @vocab compaction)
        move |iri| ctx_compactor.compact_vocab(iri),
    )
    // Node identifiers (@id): vocab=false (do NOT compact via @vocab)
    .with_id_compactor(move |iri| ctx_compactor_id.compact_id(iri));

    // CONSTRUCT output singleton wrapping isn't semantically important for us.
    // We keep a single consistent policy (currently: always use arrays).

    Ok(format_jsonld_dataset(&dataset, &config))
}

/// Instantiate CONSTRUCT template patterns with query bindings.
///
/// Produces a Dataset with EXPANDED IRIs (not compact): the default graph,
/// plus a named graph for each graph a template `GRAPH` block writes into,
/// each carrying the reifier attachments of its triples. Compaction and
/// serialization happen at the final output step, not here.
pub(super) fn instantiate_construct_graph(
    result: &QueryResult,
    compactor: &IriCompactor,
) -> Result<Dataset> {
    let template = result
        .output
        .construct_template()
        .ok_or_else(|| FormatError::InvalidBinding("CONSTRUCT missing template".into()))?;

    let rows: usize = result.batches.iter().map(Batch::len).sum();
    if rows == 0 {
        return Ok(Dataset::new());
    }

    let mut terms = TermResolver {
        result,
        compactor,
        iris: FxHashMap::default(),
        datatypes: FxHashMap::default(),
    };
    // Template constants resolve once, not once per solution row.
    let slot_of = |terms: &mut TermResolver<'_>, r: &Ref, position| match r {
        Ref::Var(v) => Ok(terms_slot(template, *v)),
        constant => terms.constant_ref(constant, position).map(Slot::Const),
    };
    // Each pattern's slots, graph, and the reifiers attached to it.
    let mut patterns = Vec::with_capacity(template.patterns.len());
    for (i, pattern) in template.patterns.iter().enumerate() {
        let s = slot_of(&mut terms, &pattern.s, Position::Subject)?;
        let p = slot_of(&mut terms, &pattern.p, Position::Predicate)?;
        let o = match &pattern.o {
            Term::Var(v) => terms_slot(template, *v),
            constant => Slot::Const(terms.constant_object(constant, pattern.dtc.as_ref())?),
        };
        let graph = match template.graph(i) {
            Some(g) => Some(slot_of(&mut terms, g, Position::Graph)?),
            None => None,
        };
        patterns.push(([s, p, o], graph, Vec::new()));
    }
    for r in &template.reifications {
        let reifier = slot_of(&mut terms, &r.reifier, Position::Subject)?;
        patterns[r.triple].2.push(reifier);
    }

    let mut dataset = Dataset::new();
    if !template.names_graphs() {
        dataset.default.reserve(rows.saturating_mul(patterns.len()));
    }

    // Monotonic counter for minting fresh per-solution template blank-node
    // labels; shared across all rows so the labels are globally distinct.
    let mut bnode_counter: usize = 0;
    // Fresh blank node per template blank-node variable for ONE solution row:
    // minted lazily on first use, shared by every template triple in the row,
    // and — via the row-global `bnode_counter` — distinct from every other
    // row's blanks.
    let mut row_bnodes: HashMap<VarId, BlankId> = HashMap::new();
    let mut reifiers: Vec<IrTerm> = Vec::new();

    for batch in &result.batches {
        for row in 0..batch.len() {
            row_bnodes.clear();
            let mut resolve = |slot: &Slot, position| -> Result<Option<IrTerm>> {
                Ok(match slot {
                    Slot::Const(term) => term.clone(),
                    Slot::Blank(v) => Some(IrTerm::BlankNode(row_blank(
                        *v,
                        &mut row_bnodes,
                        &mut bnode_counter,
                    ))),
                    Slot::Var(v) => match batch.get(row, *v) {
                        Some(binding) => terms.binding(binding, position)?,
                        None => None,
                    },
                })
            };
            'pattern: for (slots, graph_slot, reifier_slots) in &patterns {
                let mut triple: [Option<IrTerm>; 3] = [None, None, None];
                for (i, (slot, position)) in slots.iter().zip(POSITIONS).enumerate() {
                    // Skip if any term is unbound (incomplete triple)
                    let Some(term) = resolve(slot, position)? else {
                        continue 'pattern;
                    };
                    triple[i] = Some(term);
                }
                let graph = match graph_slot {
                    Some(slot) => match resolve(slot, Position::Graph)? {
                        Some(name) => Some(name),
                        // An unbound graph name writes nothing.
                        None => continue 'pattern,
                    },
                    None => None,
                };
                // Reifiers bound on this row; an unbound one attaches nothing.
                reifiers.clear();
                for slot in reifier_slots {
                    if let Some(r) = resolve(slot, Position::Subject)? {
                        reifiers.push(r);
                    }
                }
                let [Some(s), Some(p), Some(o)] = triple else {
                    unreachable!("every position was filled above")
                };
                let g = dataset.graph_mut(graph.as_ref());
                for r in reifiers.drain(..) {
                    g.add_reification(s.clone(), p.clone(), o.clone(), r);
                }
                g.add(Triple::new(s, p, o));
            }
        }
    }

    Ok(dataset)
}

/// A template position: a constant resolved up front, a variable bound per
/// row, or a template blank node minted per row.
enum Slot {
    Const(Option<IrTerm>),
    Var(VarId),
    Blank(VarId),
}

fn terms_slot(template: &ConstructTemplate, v: VarId) -> Slot {
    if template.bnode_vars.contains(&v) {
        Slot::Blank(v)
    } else {
        Slot::Var(v)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    Subject,
    Predicate,
    Object,
    /// A template graph name: an IRI or blank node, like a subject.
    Graph,
}

const POSITIONS: [Position; 3] = [Position::Subject, Position::Predicate, Position::Object];

/// Mint (or reuse) the fresh blank node bound to a template blank-node variable
/// within the current solution row.
///
/// The first time a given variable is seen in a row it takes the next global
/// counter value (so blanks are distinct across rows); later uses in the same
/// row reuse it (so `[ :p ?a ; :q ?b ]` links both triples to a single node).
///
/// The counter is scoped to ONE CONSTRUCT execution: two separate CONSTRUCT
/// queries both start at `cst0`, so a caller merging multiple `@graph` outputs
/// into one graph must standardize their blanks apart first — the standard RDF
/// blank-node scoping rule (labels are document/result-scoped), not a Fluree
/// quirk; it applies equally to `fdb-`/`b{n}` labels.
fn row_blank(
    var_id: VarId,
    row_bnodes: &mut HashMap<VarId, BlankId>,
    bnode_counter: &mut usize,
) -> BlankId {
    if let Some(existing) = row_bnodes.get(&var_id) {
        return existing.clone();
    }
    // Reserved `cst` (construct-solution-template) prefix keeps minted labels
    // disjoint from every SANCTIONED blank-node producer: stored/import blanks
    // (`fdb-…`), the graph-ir sink's unlabeled blanks (`b{n}`), and `BNODE()`
    // (`_:fdb-{uuid}` no-arg / `_:b{hex}` with a label) are all prefix-disjoint
    // from `cst`. A remote SERVICE blank is passed through verbatim and so is NOT
    // disjoint by construction — but in practice it cannot carry a `cst…` label:
    // external SPARQL endpoints are rejected (`fluree-db-query` service.rs) and
    // Fluree-to-Fluree remotes only ever emit `fdb-` blanks, so that one case is
    // held disjoint by runtime enforcement rather than by the namespace itself.
    // This is what stops a mixed template (`[ :p ?dataBlank ]`) from silently
    // merging a minted blank with a data blank — a merge the isomorphism-based
    // W3C CONSTRUCT suite cannot catch.
    let blank = BlankId::new(format!("cst{}", *bnode_counter));
    *bnode_counter += 1;
    row_bnodes.insert(var_id, blank.clone());
    blank
}

/// Turns template constants and row bindings into IR terms, with EXPANDED
/// IRIs (via Sid decoding, not compaction).
struct TermResolver<'a> {
    result: &'a QueryResult,
    compactor: &'a IriCompactor,
    /// Decoded IRIs by Sid: a subject, predicate or reference that recurs
    /// across rows shares one `Arc<str>` instead of a fresh copy per use.
    iris: FxHashMap<Sid, Arc<str>>,
    datatypes: FxHashMap<Sid, Datatype>,
}

impl TermResolver<'_> {
    fn sid_iri(&mut self, sid: &Sid) -> Result<Arc<str>> {
        if let Some(iri) = self.iris.get(sid) {
            return Ok(Arc::clone(iri));
        }
        let iri = self.compactor.decode_sid_shared(sid)?;
        self.iris.insert(sid.clone(), Arc::clone(&iri));
        Ok(iri)
    }

    fn datatype(&mut self, sid: &Sid) -> Result<Datatype> {
        if let Some(dt) = self.datatypes.get(sid) {
            return Ok(dt.clone());
        }
        let dt = Datatype::from_iri(&*self.sid_iri(sid)?);
        self.datatypes.insert(sid.clone(), dt.clone());
        Ok(dt)
    }

    fn constant_ref(&mut self, r: &Ref, position: Position) -> Result<Option<IrTerm>> {
        match r {
            Ref::Var(_) => unreachable!("variables are not constants"),
            Ref::Sid(sid) => Ok(Some(IrTerm::Iri(self.sid_iri(sid)?))),
            Ref::Iri(iri) => Ok(named(iri, position)),
        }
    }

    fn constant_object(
        &mut self,
        term: &Term,
        dtc: Option<&DatatypeConstraint>,
    ) -> Result<Option<IrTerm>> {
        match term {
            Term::Var(_) => unreachable!("variables are not constants"),
            Term::Sid(sid) => Ok(Some(IrTerm::Iri(self.sid_iri(sid)?))),
            Term::Iri(iri) => Ok(named(iri, Position::Object)),
            // A typed or language-tagged template literal carries its
            // datatype / tag in the pattern's constraint.
            Term::Value(fv) => match dtc {
                Some(dtc) => self.literal(fv, dtc),
                None => flake_value_to_ir_term(fv),
            },
        }
    }

    /// A row binding at `position`: subjects and predicates take IRIs and
    /// blank nodes only (predicates not even blank nodes), objects anything.
    fn binding(&mut self, binding: &Binding, position: Position) -> Result<Option<IrTerm>> {
        if binding.is_encoded() {
            let materialized = super::materialize::materialize_binding(self.result, binding)?;
            return self.binding(&materialized, position);
        }
        match binding {
            Binding::Unbound | Binding::Poisoned => Ok(None),
            Binding::Sid { sid, .. } => Ok(Some(IrTerm::Iri(self.sid_iri(sid)?))),
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => Ok(named(iri, position)),
            Binding::Lit { val, dtc, .. } => match position {
                Position::Object => self.literal(val, dtc),
                Position::Subject | Position::Predicate | Position::Graph => Ok(None),
            },
            Binding::EncodedLit { .. }
            | Binding::EncodedSid { .. }
            | Binding::EncodedPid { .. } => {
                unreachable!(
                    "Encoded bindings should have been materialized before CONSTRUCT IR conversion"
                )
            }
            // GROUP BY + CONSTRUCT is not supported (semantics undefined)
            Binding::Grouped(_) => Err(FormatError::InvalidBinding(
                "CONSTRUCT does not support GROUP BY (Binding::Grouped encountered)".to_string(),
            )),
            Binding::Path { .. } | Binding::Rel(_) | Binding::List(_) | Binding::Map(_) => {
                Err(FormatError::InvalidBinding(
                    "CONSTRUCT does not support path/list values".to_string(),
                ))
            }
        }
    }

    fn literal(&mut self, val: &FlakeValue, dtc: &DatatypeConstraint) -> Result<Option<IrTerm>> {
        let (datatype, language) = match (val, dtc.lang_tag()) {
            (FlakeValue::String(_), Some(tag)) => {
                (Datatype::rdf_lang_string(), Some(Arc::from(tag)))
            }
            // @json values are rdf:JSON whatever the constraint says.
            (FlakeValue::Json(_), _) => (Datatype::from_iri(rdf::JSON), None),
            _ => (self.datatype(dtc.datatype())?, None),
        };
        Ok(literal_value(val)?.map(|value| IrTerm::Literal {
            value,
            datatype,
            language,
        }))
    }
}

/// An IRI carried as text; `_:` marks a blank node, which cannot be a predicate.
fn named(iri: &Arc<str>, position: Position) -> Option<IrTerm> {
    match iri.strip_prefix("_:") {
        Some(_) if position == Position::Predicate => None,
        Some(label) => Some(IrTerm::BlankNode(BlankId::new(label))),
        None => Some(IrTerm::Iri(Arc::clone(iri))),
    }
}

/// The value half of a literal. Numbers and booleans keep their native form;
/// everything else is its lexical string.
fn literal_value(val: &FlakeValue) -> Result<Option<LiteralValue>> {
    Ok(Some(match val {
        FlakeValue::String(s) => LiteralValue::String(Arc::from(s.as_str())),
        FlakeValue::Long(n) => LiteralValue::Integer(*n),
        FlakeValue::Double(d) => LiteralValue::Double(*d),
        FlakeValue::Boolean(b) => LiteralValue::Boolean(*b),
        FlakeValue::Json(json) => LiteralValue::String(Arc::from(json.as_str())),
        FlakeValue::Null => return Ok(None),
        FlakeValue::Vector(_) => {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT formatting does not support fluree:vector literals yet".to_string(),
            ))
        }
        // Invariant: a literal never holds a reference.
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "a literal value cannot be a reference".to_string(),
            ))
        }
        FlakeValue::BigInt(n) => LiteralValue::String(Arc::from(n.to_string())),
        FlakeValue::Decimal(d) => LiteralValue::String(Arc::from(d.to_plain_string())),
        FlakeValue::DateTime(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::Date(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::Time(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GYear(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GYearMonth(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GMonth(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GDay(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GMonthDay(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::YearMonthDuration(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::DayTimeDuration(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::Duration(v) => LiteralValue::String(Arc::from(v.to_string())),
        FlakeValue::GeoPoint(v) => LiteralValue::String(Arc::from(v.to_string())),
    }))
}

/// Convert an untyped FlakeValue constant to an IR Term, with the datatype
/// its value implies.
fn flake_value_to_ir_term(val: &FlakeValue) -> Result<Option<IrTerm>> {
    let datatype = match val {
        FlakeValue::String(_) => Datatype::xsd_string(),
        FlakeValue::Long(_) | FlakeValue::BigInt(_) => Datatype::xsd_integer(),
        FlakeValue::Double(_) => Datatype::xsd_double(),
        FlakeValue::Boolean(_) => Datatype::xsd_boolean(),
        FlakeValue::Json(_) => Datatype::from_iri(rdf::JSON),
        FlakeValue::Decimal(_) => Datatype::xsd_decimal(),
        FlakeValue::DateTime(_) => Datatype::xsd_date_time(),
        FlakeValue::Date(_) => Datatype::xsd_date(),
        FlakeValue::Time(_) => Datatype::from_iri(xsd::TIME),
        FlakeValue::GYear(_) => Datatype::from_iri(xsd::G_YEAR),
        FlakeValue::GYearMonth(_) => Datatype::from_iri(xsd::G_YEAR_MONTH),
        FlakeValue::GMonth(_) => Datatype::from_iri(xsd::G_MONTH),
        FlakeValue::GDay(_) => Datatype::from_iri(xsd::G_DAY),
        FlakeValue::GMonthDay(_) => Datatype::from_iri(xsd::G_MONTH_DAY),
        FlakeValue::YearMonthDuration(_) => Datatype::from_iri(xsd::YEAR_MONTH_DURATION),
        FlakeValue::DayTimeDuration(_) => Datatype::from_iri(xsd::DAY_TIME_DURATION),
        FlakeValue::Duration(_) => Datatype::from_iri(xsd::DURATION),
        FlakeValue::GeoPoint(_) => Datatype::from_iri(geo::WKT_LITERAL),
        FlakeValue::Vector(_) | FlakeValue::Null | FlakeValue::Ref(_) => {
            return literal_value(val).map(|_| None)
        }
    };
    Ok(literal_value(val)?.map(|value| IrTerm::Literal {
        value,
        datatype,
        language: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::datatype::iri as dt_iri;

    // Note: Full integration tests require QueryResult with populated batches.
    // Unit tests for the helper functions can use mock data.

    #[test]
    fn test_flake_value_to_ir_term_string() {
        let result = flake_value_to_ir_term(&FlakeValue::String("hello".to_string()))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::String(s) if s.as_ref() == "hello"));
                assert!(datatype.is_xsd_string());
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_integer() {
        let result = flake_value_to_ir_term(&FlakeValue::Long(42))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::Integer(42)));
                assert_eq!(datatype.as_iri(), dt_iri::XSD_INTEGER);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_double() {
        let result = flake_value_to_ir_term(&FlakeValue::Double(3.13))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(
                    matches!(value, LiteralValue::Double(d) if (d - 3.13).abs() < f64::EPSILON)
                );
                assert_eq!(datatype.as_iri(), dt_iri::XSD_DOUBLE);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_boolean() {
        let result = flake_value_to_ir_term(&FlakeValue::Boolean(true))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::Boolean(true)));
                assert_eq!(datatype.as_iri(), dt_iri::XSD_BOOLEAN);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_null() {
        let result = flake_value_to_ir_term(&FlakeValue::Null).unwrap();
        assert!(result.is_none());
    }
}
