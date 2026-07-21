//! Bulk materialization driver for `fluree materialize`.
//!
//! Streams a virtual (R2RML-over-Iceberg) graph source through the whole-graph
//! enumerator in [`fluree_db_r2rml::materialize`], emitting every
//! `(subject, predicate, object)` triple to a [`TripleObserver`]. This is the
//! provider-backed counterpart to the in-memory reference driver
//! [`fluree_db_r2rml::materialize::enumerate_from_batches`]: identical dims-first
//! ordering and the same per-batch `index_batch` / `emit_batch` calls, but the
//! batches stream from the provider's `scan_table` instead of being held in
//! memory.
//!
//! The observer is where the twin build plugs in — an N-Triples collector for a
//! parity diff, or (later chunks) an ingest-sink adapter that streams the
//! triples into the native import pipeline.

use futures::StreamExt;

use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
use fluree_db_r2rml::materialize::{
    emit_batch, plan, MaterializeStats, ParentIndexSet, TripleObserver,
};
use fluree_db_r2rml::mapping::TriplesMap;
use fluree_db_r2rml::{R2rmlError, RdfTerm};
use fluree_db_transact::import_sink::ImportSink;
use fluree_graph_ir::{Datatype, GraphSink, TermId};
use fluree_vocab::UnresolvedDatatypeConstraint;

/// An error raised while materializing a virtual graph source.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    /// Reading the source (catalog / table scan) failed.
    #[error("scan error: {0}")]
    Scan(#[from] fluree_db_query::error::QueryError),
    /// The mapping or a term failed to materialize.
    #[error(transparent)]
    Mapping(#[from] R2rmlError),
    /// A TriplesMap had no table name (SQL-query logical tables are unsupported
    /// for Iceberg sources, so this should not occur in practice).
    #[error("TriplesMap '{0}' has no logical table name")]
    NoTable(String),
}

/// Columns a scan of `tm` must project: the TriplesMap's own referenced columns
/// (subject template + every predicate-object map, including foreign-key child
/// columns) plus any parent join-key columns needed to index `tm` as a
/// foreign-key parent.
fn scan_projection(tm: &TriplesMap, parents: &ParentIndexSet) -> Vec<String> {
    let mut cols: Vec<String> = tm
        .referenced_columns()
        .into_iter()
        .map(String::from)
        .collect();
    cols.extend(parents.needed_parent_columns(&tm.iri));
    cols.sort();
    cols.dedup();
    cols
}

/// Enumerate every triple of a virtual R2RML graph source, streaming each
/// logical table through the whole-graph enumerator and emitting to `observer`.
/// Returns the [`MaterializeStats`] for the run (triple counts and the
/// per-`(child, predicate)` foreign-key edge counts used by the parity gate).
///
/// Ordering is dims-first: foreign-key parents are scanned — and their
/// key → subject index built — before the children that reference them; cyclic
/// and self-referential parents are fully pre-indexed in a first pass. A parent
/// (dimension) table that is only ever a parent is therefore scanned twice
/// (once to index, once to emit) only when it is cyclic/self-referential;
/// otherwise it is scanned once and indexed lazily during its own emit pass.
///
/// `scan_table` is called with `as_of_t = None`; the Iceberg provider pins the
/// snapshot latest-at-first-touch, which — held across a whole build via one
/// shared provider — gives the per-table build watermark (DEC-003 §C-4). The
/// explicit build-scoped pin and watermark capture land in a later chunk.
pub async fn materialize_graph<P>(
    provider: &P,
    graph_source_id: &str,
    observer: &mut dyn TripleObserver,
) -> Result<MaterializeStats, MaterializeError>
where
    P: R2rmlProvider + R2rmlTableProvider,
{
    let mapping = provider.compiled_mapping(graph_source_id, None).await?;
    let mut parents = ParentIndexSet::new(&mapping)?;
    let materialization = plan(&mapping);

    // Pass 1 — pre-index parents that cannot be indexed lazily (cyclic /
    // self-referential). Scan for the index only; no triples emitted yet.
    for tm_iri in &materialization.preindex {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            parents.index_batch(tm, &batch)?;
        }
    }

    // Pass 2 — emit in dims-first order; lazily index a parent during its own
    // emit pass unless it was pre-indexed above.
    let mut stats = MaterializeStats::default();
    for tm_iri in &materialization.emit_order {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let lazy_index =
            !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if lazy_index {
                parents.index_batch(tm, &batch)?;
            }
            emit_batch(tm, &batch, &parents, observer, &mut stats)?;
        }
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Ingestion adapter: materialized triples → ImportSink (native bulk pipeline)
// ---------------------------------------------------------------------------

/// A [`TripleObserver`] that streams materialized triples into an [`ImportSink`],
/// interning terms and encoding flakes exactly as the Turtle / JSON-LD import
/// path does. This is the bridge from the whole-graph enumerator to the native
/// bulk ingestion pipeline.
///
/// Term, datatype, and language fidelity is preserved by routing every literal
/// through the SAME `ImportSink::term_literal` the parsers use: it parses the
/// lexical value into the correct typed `FlakeValue` from the datatype IRI (see
/// `convert_string_literal` in `fluree-db-transact`), so a materialized
/// `"9.99"^^xsd:decimal` interns to a decimal flake and a `"hola"@es` to a
/// lang-tagged string — identical to the same literal arriving from Turtle.
///
/// The caller owns the sink's lifecycle (construction, optional spool context,
/// and `finish()`), so one observer drives one chunk's worth of triples into one
/// commit. Any encoding failure is captured inside the sink and surfaced at
/// `finish()`, matching the parser→sink contract.
pub struct ImportSinkObserver<'a, 'ns> {
    sink: &'a mut ImportSink<'ns>,
}

impl<'a, 'ns> ImportSinkObserver<'a, 'ns> {
    /// Wrap a mutable [`ImportSink`].
    pub fn new(sink: &'a mut ImportSink<'ns>) -> Self {
        Self { sink }
    }
}

/// Resolve a literal's datatype IRI and optional language tag for
/// `ImportSink::term_literal`. This is the one place literal fidelity could
/// silently narrow — lang tag vs explicit datatype vs the implicit
/// `xsd:string` — so it is factored out and unit-tested. A `LangTag` reports
/// its datatype as `rdf:langString`; an `Explicit` reports its own IRI; the
/// absence of a constraint is a plain `xsd:string`.
fn literal_sink_args(dtc: Option<&UnresolvedDatatypeConstraint>) -> (&str, Option<&str>) {
    match dtc {
        Some(c) => (c.datatype_iri(), c.lang_tag()),
        None => (fluree_vocab::xsd::STRING, None),
    }
}

/// Intern one materialized RDF term into the sink, returning its `TermId`.
fn intern_term(sink: &mut ImportSink, term: &RdfTerm) -> TermId {
    match term {
        RdfTerm::Iri(iri) => sink.term_iri(iri),
        RdfTerm::BlankNode(label) => sink.term_blank(Some(label.as_str())),
        RdfTerm::Literal { value, dtc } => {
            let (dt_iri, lang) = literal_sink_args(dtc.as_ref());
            sink.term_literal(value, Datatype::from_iri(dt_iri), lang)
        }
    }
}

impl TripleObserver for ImportSinkObserver<'_, '_> {
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
        let s = intern_term(self.sink, subject);
        let p = self.sink.term_iri(predicate);
        let o = intern_term(self.sink, object);
        self.sink.emit_triple(s, p, o);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::literal_sink_args;
    use fluree_vocab::UnresolvedDatatypeConstraint as Dtc;

    #[test]
    fn plain_literal_is_xsd_string_no_lang() {
        assert_eq!(literal_sink_args(None), (fluree_vocab::xsd::STRING, None));
    }

    #[test]
    fn lang_tagged_literal_uses_langstring_and_tag() {
        let dtc = Dtc::LangTag("es".into());
        assert_eq!(
            literal_sink_args(Some(&dtc)),
            (fluree_vocab::rdf::LANG_STRING, Some("es")),
            "a language tag must intern as rdf:langString + the tag"
        );
    }

    #[test]
    fn explicit_datatype_passes_through_without_lang() {
        let dtc = Dtc::Explicit(fluree_vocab::xsd::DECIMAL.into());
        assert_eq!(
            literal_sink_args(Some(&dtc)),
            (fluree_vocab::xsd::DECIMAL, None),
            "an explicit datatype must reach term_literal so the value types correctly"
        );
    }
}
