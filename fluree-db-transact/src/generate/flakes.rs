//! Flake generation from triple templates
//!
//! This module provides `FlakeGenerator` for materializing triple templates
//! with variable bindings into concrete flakes.

use crate::error::{Result, TransactError};
use crate::ir::{TemplateTerm, TripleTemplate};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_query::{Batch, Binding};
use fluree_vocab::namespaces::{FLUREE_DB, JSON_LD, OGC_GEO, RDF, XSD};
use once_cell::sync::Lazy;
use std::collections::HashMap;

// Well-known datatype SIDs, cached to avoid per-call Arc<str> allocation.
// Clone is ~5ns (atomic Arc bump) vs ~30-50ns for Sid::new().

pub(crate) static DT_ID: Lazy<Sid> = Lazy::new(|| Sid::new(JSON_LD, "id"));
pub(crate) static DT_BOOLEAN: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "boolean"));
pub(crate) static DT_INTEGER: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "integer"));
pub(crate) static DT_DOUBLE: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "double"));
pub(crate) static DT_DECIMAL: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "decimal"));
pub(crate) static DT_DATE_TIME: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "dateTime"));
pub(crate) static DT_DATE: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "date"));
pub(crate) static DT_TIME: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "time"));
pub(crate) static DT_STRING: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "string"));
pub(crate) static DT_JSON: Lazy<Sid> = Lazy::new(|| Sid::new(RDF, "JSON"));
pub(crate) static DT_LANG_STRING: Lazy<Sid> = Lazy::new(|| Sid::new(RDF, "langString"));
pub(crate) static DT_VECTOR: Lazy<Sid> = Lazy::new(|| Sid::new(FLUREE_DB, "embeddingVector"));
pub(crate) static DT_G_YEAR: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "gYear"));
pub(crate) static DT_G_YEAR_MONTH: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "gYearMonth"));
pub(crate) static DT_G_MONTH: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "gMonth"));
pub(crate) static DT_G_DAY: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "gDay"));
pub(crate) static DT_G_MONTH_DAY: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "gMonthDay"));
pub(crate) static DT_YEAR_MONTH_DURATION: Lazy<Sid> =
    Lazy::new(|| Sid::new(XSD, "yearMonthDuration"));
pub(crate) static DT_DAY_TIME_DURATION: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "dayTimeDuration"));
pub(crate) static DT_DURATION: Lazy<Sid> = Lazy::new(|| Sid::new(XSD, "duration"));
pub(crate) static DT_WKT_LITERAL: Lazy<Sid> = Lazy::new(|| Sid::new(OGC_GEO, "wktLiteral"));

/// Generates flakes from triple templates
///
/// The generator materializes templates by substituting variable bindings with
/// concrete values. When a variable is unbound or poisoned (from OPTIONAL), the
/// entire flake is silently skipped rather than producing an error.
///
/// This follows SPARQL UPDATE semantics where unbound variables in templates
/// simply produce no output for that row of bindings.
pub struct FlakeGenerator<'a> {
    /// Transaction time for generated flakes
    t: i64,

    /// Namespace registry for encoding IRIs and skolemizing blank nodes
    ns_registry: &'a mut NamespaceRegistry,

    /// Transaction ID for blank node skolemization
    txn_id: String,

    /// Graph ID to Sid mapping for named graphs
    ///
    /// When a template has a `graph_id`, this map provides the corresponding
    /// graph Sid for `Flake::new_in_graph()`. Graph IDs 0 (default) and 1 (txn-meta)
    /// are reserved and should not appear in this map.
    graph_sids: HashMap<u16, Sid>,
}

impl<'a> FlakeGenerator<'a> {
    /// Create a new flake generator
    ///
    /// # Arguments
    /// * `t` - Transaction time for generated flakes
    /// * `ns_registry` - Namespace registry for encoding IRIs
    /// * `txn_id` - Transaction ID for blank node skolemization
    pub fn new(t: i64, ns_registry: &'a mut NamespaceRegistry, txn_id: String) -> Self {
        Self {
            t,
            ns_registry,
            txn_id,
            graph_sids: HashMap::new(),
        }
    }

    /// Set the graph Sid mapping for named graph support.
    ///
    /// The map should contain entries for user-defined named graphs (g_id >= 2).
    /// Templates with a `graph_id` matching an entry in this map will produce
    /// flakes in the corresponding named graph.
    pub fn with_graph_sids(mut self, graph_sids: HashMap<u16, Sid>) -> Self {
        self.graph_sids = graph_sids;
        self
    }

    /// Generate assertion flakes from insert templates
    pub fn generate_assertions(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
    ) -> Result<Vec<Flake>> {
        self.generate_flakes(templates, bindings, true)
    }

    /// Generate retraction flakes from delete templates
    pub fn generate_retractions(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
    ) -> Result<Vec<Flake>> {
        self.generate_flakes(templates, bindings, false)
    }

    /// Generate flakes from templates with given operation flag
    fn generate_flakes(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
        op: bool,
    ) -> Result<Vec<Flake>> {
        let mut flakes = Vec::new();

        // Row count semantics:
        // - INSERT without WHERE produces an "empty bindings" batch (0 vars, 0 rows). We still need
        //   to materialize templates once, so treat it as a single empty row.
        // - UPDATE/UPSERT where WHERE matches nothing produces an empty batch with a non-empty
        //   schema (vars present but 0 rows). In that case, there are **zero solution rows** and
        //   templates must produce **zero flakes** (no-op).
        let row_count = if bindings.is_empty() {
            usize::from(bindings.schema().is_empty())
        } else {
            bindings.len()
        };

        for row_idx in 0..row_count {
            for template in templates {
                flakes.extend(self.materialize_template(template, bindings, row_idx, op)?);
            }
        }

        Ok(flakes)
    }

    /// Materialize a single template with bindings into a flake
    fn materialize_template(
        &mut self,
        template: &TripleTemplate,
        bindings: &Batch,
        row_idx: usize,
        op: bool,
    ) -> Result<Option<Flake>> {
        // Resolve each component
        let s = self.resolve_subject(&template.subject, bindings, row_idx)?;
        let p = self.resolve_predicate(&template.predicate, bindings, row_idx)?;
        let explicit_dt = template
            .dtc
            .as_ref()
            .map(fluree_db_core::DatatypeConstraint::datatype);
        let (o, dt) = self.resolve_object(&template.object, explicit_dt, bindings, row_idx)?;

        let bound_lang = match &template.object {
            TemplateTerm::Var(var_id) => match bindings.get(row_idx, *var_id) {
                Some(Binding::Lit { dtc, .. }) => {
                    dtc.lang_tag().map(std::string::ToString::to_string)
                }
                _ => None,
            },
            _ => None,
        };

        let template_lang = template
            .dtc
            .as_ref()
            .and_then(|d| d.lang_tag())
            .map(std::string::ToString::to_string);

        // Language-tagged literals use rdf:langString datatype.
        let dt = if template_lang.is_some() || bound_lang.is_some() {
            dt.map(|_| DT_LANG_STRING.clone())
        } else {
            dt
        };

        // If any component is None (unbound variable), skip this flake
        let (s, p, o, dt) = match (s, p, o, dt) {
            (Some(s), Some(p), Some(o), Some(dt)) => (s, p, o, dt),
            _ => return Ok(None),
        };

        // Create metadata if language tag or list_index is present
        let meta_lang = template_lang.or(bound_lang);
        let meta = match (&meta_lang, &template.list_index) {
            (Some(lang), Some(idx)) => {
                // Both language and list_index
                Some(FlakeMeta {
                    lang: Some(lang.clone()),
                    i: Some(*idx),
                })
            }
            (Some(lang), None) => Some(FlakeMeta::with_lang(lang)),
            (None, Some(idx)) => Some(FlakeMeta::with_index(*idx)),
            (None, None) => None,
        };

        // Create flake in named graph if template has graph_id
        let flake = if let Some(g_id) = template.graph_id {
            let g_sid = self.graph_sids.get(&g_id).ok_or_else(|| {
                TransactError::FlakeGeneration(format!(
                    "template references graph_id {g_id} but no graph Sid was provided; \
                     this indicates a bug in graph delta/sid wiring"
                ))
            })?;
            Flake::new_in_graph(g_sid.clone(), s, p, o, dt, self.t, op, meta)
        } else {
            Flake::new(s, p, o, dt, self.t, op, meta)
        };

        Ok(Some(flake))
    }

    /// Resolve a subject term
    fn resolve_subject(
        &mut self,
        term: &TemplateTerm,
        bindings: &Batch,
        row: usize,
    ) -> Result<Option<Sid>> {
        match term {
            TemplateTerm::Sid(sid) => Ok(Some(sid.clone())),
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{var_id:?}")));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid { sid, .. } => Ok(Some(sid.clone())),
                        Binding::IriMatch { primary_sid, .. } => Ok(Some(primary_sid.clone())),
                        Binding::Unbound | Binding::Poisoned => Ok(None),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Subject cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Lit { .. } => Err(TransactError::InvalidTerm(
                            "Subject must be a Sid, not a literal".to_string(),
                        )),
                        Binding::EncodedLit { .. } => Err(TransactError::InvalidTerm(
                            "Subject must be a Sid; EncodedLit must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedSid { .. } => Err(TransactError::InvalidTerm(
                            "Subject must be a Sid; EncodedSid must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedPid { .. } => Err(TransactError::InvalidTerm(
                            "Subject must be a Sid; EncodedPid cannot be used as subject".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from graph source cannot be used as subject for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok(None)
                }
            }
            TemplateTerm::BlankNode(label) => {
                let sid = self.skolemize_blank_node(label);
                Ok(Some(sid))
            }
            TemplateTerm::Value(_) => Err(TransactError::InvalidTerm(
                "Subject cannot be a literal value".to_string(),
            )),
        }
    }

    /// Resolve a predicate term
    fn resolve_predicate(
        &mut self,
        term: &TemplateTerm,
        bindings: &Batch,
        row: usize,
    ) -> Result<Option<Sid>> {
        match term {
            TemplateTerm::Sid(sid) => Ok(Some(sid.clone())),
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{var_id:?}")));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid { sid, .. } => Ok(Some(sid.clone())),
                        Binding::IriMatch { primary_sid, .. } => Ok(Some(primary_sid.clone())),
                        Binding::Unbound | Binding::Poisoned => Ok(None),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Predicate cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Lit { .. } => Err(TransactError::InvalidTerm(
                            "Predicate must be a Sid, not a literal".to_string(),
                        )),
                        Binding::EncodedLit { .. } => Err(TransactError::InvalidTerm(
                            "Predicate must be a Sid; EncodedLit must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedSid { .. } => Err(TransactError::InvalidTerm(
                            "Predicate must be a Sid; EncodedSid must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedPid { .. } => Err(TransactError::InvalidTerm(
                            "Predicate must be a Sid; EncodedPid must be materialized before flake generation".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from graph source cannot be used as predicate for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok(None)
                }
            }
            TemplateTerm::BlankNode(_) => Err(TransactError::InvalidTerm(
                "Predicate cannot be a blank node".to_string(),
            )),
            TemplateTerm::Value(_) => Err(TransactError::InvalidTerm(
                "Predicate cannot be a literal value".to_string(),
            )),
        }
    }

    /// Resolve an object term
    fn resolve_object(
        &mut self,
        term: &TemplateTerm,
        explicit_dt: Option<&Sid>,
        bindings: &Batch,
        row: usize,
    ) -> Result<(Option<FlakeValue>, Option<Sid>)> {
        match term {
            TemplateTerm::Sid(sid) => {
                // Reference type
                Ok((Some(FlakeValue::Ref(sid.clone())), Some(DT_ID.clone())))
            }
            TemplateTerm::Value(val) => {
                let dt = explicit_dt.cloned().unwrap_or_else(|| infer_datatype(val));
                Ok((Some(val.clone()), Some(dt)))
            }
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{var_id:?}")));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid { sid, .. } => {
                            Ok((Some(FlakeValue::Ref(sid.clone())), Some(DT_ID.clone())))
                        }
                        Binding::IriMatch { primary_sid, .. } => {
                            Ok((Some(FlakeValue::Ref(primary_sid.clone())), Some(DT_ID.clone())))
                        }
                        Binding::Lit { val, dtc, .. } => {
                            Ok((Some(val.clone()), Some(dtc.datatype().clone())))
                        }
                        Binding::EncodedLit { .. } => Err(TransactError::InvalidTerm(
                            "EncodedLit must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedSid { .. } => Err(TransactError::InvalidTerm(
                            "EncodedSid must be materialized before flake generation".to_string(),
                        )),
                        Binding::EncodedPid { .. } => Err(TransactError::InvalidTerm(
                            "EncodedPid must be materialized before flake generation".to_string(),
                        )),
                        Binding::Unbound | Binding::Poisoned => Ok((None, None)),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Object cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from graph source cannot be used as object for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok((None, None))
                }
            }
            TemplateTerm::BlankNode(label) => {
                let sid = self.skolemize_blank_node(label);
                Ok((Some(FlakeValue::Ref(sid)), Some(DT_ID.clone())))
            }
        }
    }

    /// Skolemize a blank node to a Sid
    ///
    /// Creates a unique Sid for a blank node label within this transaction.
    /// The format is: `_:fdb-{txn_id}-{label}` where label is the user's
    /// blank node identifier stripped of the `_:` prefix.
    fn skolemize_blank_node(&mut self, label: &str) -> Sid {
        let local = label.trim_start_matches("_:");
        // Combine txn_id and local label to create a unique ID
        let unique_id = format!("{}-{}", self.txn_id, local);
        self.ns_registry.blank_node_sid(&unique_id)
    }
}

/// Infer datatype from a FlakeValue
///
/// Returns the appropriate XSD/RDF datatype Sid for the given value.
/// This is used both for flake generation and for VALUES clause binding conversion.
pub fn infer_datatype(val: &FlakeValue) -> Sid {
    match val {
        FlakeValue::Ref(_) => DT_ID.clone(),
        FlakeValue::Boolean(_) => DT_BOOLEAN.clone(),
        // JSON-LD default for integral numbers is xsd:integer.
        // Preserve explicit xsd:long only when the source literal is explicitly typed.
        FlakeValue::Long(_) => DT_INTEGER.clone(),
        FlakeValue::Double(_) => DT_DOUBLE.clone(),
        FlakeValue::BigInt(_) => DT_INTEGER.clone(),
        FlakeValue::Decimal(_) => DT_DECIMAL.clone(),
        FlakeValue::DateTime(_) => DT_DATE_TIME.clone(),
        FlakeValue::Date(_) => DT_DATE.clone(),
        FlakeValue::Time(_) => DT_TIME.clone(),
        FlakeValue::String(_) => DT_STRING.clone(),
        FlakeValue::Json(_) => DT_JSON.clone(),
        FlakeValue::Vector(_) => DT_VECTOR.clone(),
        FlakeValue::GYear(_) => DT_G_YEAR.clone(),
        FlakeValue::GYearMonth(_) => DT_G_YEAR_MONTH.clone(),
        FlakeValue::GMonth(_) => DT_G_MONTH.clone(),
        FlakeValue::GDay(_) => DT_G_DAY.clone(),
        FlakeValue::GMonthDay(_) => DT_G_MONTH_DAY.clone(),
        FlakeValue::YearMonthDuration(_) => DT_YEAR_MONTH_DURATION.clone(),
        FlakeValue::DayTimeDuration(_) => DT_DAY_TIME_DURATION.clone(),
        FlakeValue::Duration(_) => DT_DURATION.clone(),
        FlakeValue::GeoPoint(_) => DT_WKT_LITERAL.clone(),
        // Null isn't a standard RDF literal; treat as xsd:string for now (MVP).
        FlakeValue::Null => DT_STRING.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::DatatypeConstraint;
    use fluree_db_query::VarId;
    use std::sync::Arc;

    fn make_empty_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::new([]);
        Batch::empty(schema).unwrap()
    }

    #[test]
    fn test_generate_simple_assertion() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        assert!(flakes[0].op);
        assert_eq!(flakes[0].s.name.as_ref(), "ex:alice");
        assert_eq!(flakes[0].p.name.as_ref(), "ex:name");
    }

    #[test]
    fn test_generate_retraction() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_retractions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        assert!(!flakes[0].op);
    }

    #[test]
    fn test_blank_node_skolemization() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn123".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Test".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        // Check that the blank node was skolemized
        assert!(flakes[0].s.name.contains("b1"));
    }

    #[test]
    fn test_infer_datatype() {
        assert_eq!(
            infer_datatype(&FlakeValue::Long(42)).name.as_ref(),
            "integer"
        );
        assert_eq!(
            infer_datatype(&FlakeValue::Double(3.5)).name.as_ref(),
            "double"
        );
        assert_eq!(
            infer_datatype(&FlakeValue::String("test".to_string()))
                .name
                .as_ref(),
            "string"
        );
        assert_eq!(
            infer_datatype(&FlakeValue::Boolean(true)).name.as_ref(),
            "boolean"
        );
    }

    #[test]
    fn test_generate_with_list_index() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        // Create templates with list indices
        let templates = vec![
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("red".to_string())),
            )
            .with_list_index(0),
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("green".to_string())),
            )
            .with_list_index(1),
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("blue".to_string())),
            )
            .with_list_index(2),
        ];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 3);

        // Check list indices are preserved in metadata
        assert_eq!(flakes[0].m.as_ref().and_then(|m| m.i), Some(0));
        assert_eq!(flakes[1].m.as_ref().and_then(|m| m.i), Some(1));
        assert_eq!(flakes[2].m.as_ref().and_then(|m| m.i), Some(2));

        // Check values
        assert_eq!(flakes[0].o, FlakeValue::String("red".to_string()));
        assert_eq!(flakes[1].o, FlakeValue::String("green".to_string()));
        assert_eq!(flakes[2].o, FlakeValue::String("blue".to_string()));
    }

    #[test]
    fn test_generate_with_language_and_list_index() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        // Create template with both language and list_index
        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:names")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )
        .with_dtc(DatatypeConstraint::LangTag(Arc::from("en")))
        .with_list_index(0)];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);

        // Check both language and list_index are present in metadata
        let meta = flakes[0].m.as_ref().expect("should have metadata");
        assert_eq!(meta.lang.as_deref(), Some("en"));
        assert_eq!(meta.i, Some(0));
    }
}
