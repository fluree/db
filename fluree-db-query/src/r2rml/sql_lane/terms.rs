//! From returned columns back to bindings, and from bindings to key-set
//! literals.
//!
//! The statement returns raw columns under generated names. Per table alias
//! they are regrouped into a `ColumnBatch` carrying the mapping's own column
//! names, so the existing R2RML term materialization and literal encoding
//! run unchanged — datatypes come from `rr:datatype`, never from the SQL type.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::LedgerSnapshot;
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TriplesMap};
use fluree_db_r2rml::materialize::{
    materialize_object_from_batch, materialize_subject_from_batch, reverse_subject_template,
};
use fluree_db_tabular::plan::{Literal, OutputCol};
use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo};

use super::lower::{key_fits, literal_of, AccessInfo, KeyShape, Lowered, RdfClass, TermSource};
use crate::binding::Binding;
use crate::error::{QueryError, Result};
use crate::r2rml::operator::LiteralEncoder;
use crate::var_registry::VarId;

struct AliasTerms {
    alias: String,
    tm: TriplesMap,
    encoder: LiteralEncoder,
    columns: Vec<String>,
    /// Index of each column in the statement's output, by `columns` position.
    output_idx: Vec<usize>,
}

pub(crate) struct Materializer {
    aliases: Vec<AliasTerms>,
    terms: Vec<(VarId, TermSource)>,
}

impl Materializer {
    pub(crate) fn new(
        lowered: &Lowered,
        mapping: &CompiledR2rmlMapping,
        snapshot: &LedgerSnapshot,
    ) -> Result<Self> {
        let mut aliases = Vec::with_capacity(lowered.accesses.len());
        for AccessInfo {
            alias,
            tm_iri,
            columns,
        } in &lowered.accesses
        {
            let tm = mapping.get(tm_iri).cloned().ok_or_else(|| {
                QueryError::Internal(format!("triples map '{tm_iri}' vanished from the mapping"))
            })?;
            let output_idx = columns
                .iter()
                .map(|c| {
                    lowered
                        .outputs
                        .iter()
                        .position(|o| &o.col.alias == alias && &o.col.column == c)
                        .ok_or_else(|| {
                            QueryError::Internal(format!("column {alias}.{c} not projected"))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let encoder = LiteralEncoder::build(&tm, snapshot);
            aliases.push(AliasTerms {
                alias: alias.clone(),
                tm,
                encoder,
                columns: columns.clone(),
                output_idx,
            });
        }
        Ok(Self {
            aliases,
            terms: lowered.terms.clone(),
        })
    }

    /// Regroup one page by alias under the mapping's column names.
    pub(crate) fn split_page(
        &self,
        page: ColumnBatch,
        outputs: &[OutputCol],
    ) -> Result<HashMap<String, ColumnBatch>> {
        let num_rows = page.num_rows;
        let schema = page.schema;
        let mut columns: Vec<Option<Column>> = page.columns.into_iter().map(Some).collect();
        let mut out = HashMap::with_capacity(self.aliases.len());
        for a in &self.aliases {
            let mut fields = Vec::with_capacity(a.columns.len());
            let mut cols = Vec::with_capacity(a.columns.len());
            for (i, (name, out_idx)) in a.columns.iter().zip(&a.output_idx).enumerate() {
                let out_name = &outputs[*out_idx].name;
                let page_idx = schema.index_by_name(out_name).ok_or_else(|| {
                    QueryError::Internal(format!(
                        "statement result lacks column '{out_name}' ({}.{name})",
                        a.alias
                    ))
                })?;
                let col = columns[page_idx].take().ok_or_else(|| {
                    QueryError::Internal(format!("column '{out_name}' claimed twice"))
                })?;
                fields.push(FieldInfo {
                    name: name.clone(),
                    field_type: col.field_type(),
                    nullable: true,
                    field_id: i as i32 + 1,
                });
                cols.push(col);
            }
            let batch = if cols.is_empty() {
                ColumnBatch {
                    schema: Arc::new(BatchSchema::new(Vec::new())),
                    columns: Vec::new(),
                    num_rows,
                }
            } else {
                ColumnBatch::new(Arc::new(BatchSchema::new(fields)), cols)
                    .map_err(|e| QueryError::Internal(format!("regrouping page: {e}")))?
            };
            out.insert(a.alias.clone(), batch);
        }
        Ok(out)
    }

    /// The block's bindings for one row.
    pub(crate) fn row(
        &self,
        batches: &HashMap<String, ColumnBatch>,
        row_idx: usize,
    ) -> Result<Vec<(VarId, Binding)>> {
        let mut out = Vec::with_capacity(self.terms.len());
        for (var, term) in &self.terms {
            let binding = match term {
                TermSource::Constant(t) => {
                    // Any encoder will do for a constant: datatype Sids resolve
                    // the same way for every triples map.
                    self.aliases
                        .first()
                        .map(|a| a.encoder.encode(t))
                        .unwrap_or(Binding::Unbound)
                }
                TermSource::Subject { alias } => {
                    let a = self.alias(alias)?;
                    let batch = &batches[alias];
                    match materialize_subject_from_batch(&a.tm.subject_map, batch, row_idx) {
                        Ok(Some(t)) => a.encoder.encode(&t),
                        _ => Binding::Unbound,
                    }
                }
                TermSource::Object { alias, pom } => {
                    let a = self.alias(alias)?;
                    let batch = &batches[alias];
                    let om = &a.tm.predicate_object_maps[*pom].object_map;
                    match materialize_object_from_batch(om, batch, row_idx) {
                        Ok(Some(t)) => a.encoder.encode(&t),
                        _ => Binding::Unbound,
                    }
                }
            };
            out.push((*var, binding));
        }
        Ok(out)
    }

    fn alias(&self, alias: &str) -> Result<&AliasTerms> {
        self.aliases
            .iter()
            .find(|a| a.alias == alias)
            .ok_or_else(|| QueryError::Internal(format!("unknown alias '{alias}'")))
    }
}

/// The IRI a binding denotes, for seeding a template's key columns.
pub(crate) fn iri_of_binding(b: &Binding, snapshot: Option<&LedgerSnapshot>) -> Option<String> {
    match b {
        Binding::Iri(iri) => Some(iri.to_string()),
        Binding::IriMatch { iri, .. } => Some(iri.to_string()),
        // A namespace-0 Sid carries the full IRI as its name (a VALUES row, a
        // graph-source IRI); anything else needs the snapshot's dictionary.
        Binding::Sid { sid, .. } => snapshot
            .and_then(|s| s.decode_sid(sid))
            .or_else(|| (sid.namespace_code == 0).then(|| sid.name.to_string())),
        _ => None,
    }
}

/// The key-set literals for `binding` under `shape`, in column order, or
/// `None` when the binding cannot match any row (so it need not be sent).
pub(crate) fn seed_values(
    binding: &Binding,
    shape: &KeyShape,
    snapshot: Option<&LedgerSnapshot>,
) -> Option<Vec<Literal>> {
    match shape {
        KeyShape::Template {
            template,
            cols,
            types,
        } => {
            let iri = iri_of_binding(binding, snapshot)?;
            let keys = reverse_subject_template(template, &iri)?;
            cols.iter()
                .zip(types)
                .map(|(c, ty)| {
                    keys.iter()
                        .find(|(col, _)| col == &c.column)
                        .filter(|(_, raw)| key_fits(*ty, raw))
                        .map(|(_, raw)| Literal::TemplateKey(raw.clone()))
                })
                .collect()
        }
        KeyShape::Column { class, .. } => match class {
            RdfClass::Iri => iri_of_binding(binding, snapshot).map(|i| vec![Literal::Str(i)]),
            _ => match binding {
                Binding::Lit { val, dtc, .. } => {
                    let (lit, lclass) = literal_of(val, Some(dtc))?;
                    (&lclass == class).then_some(vec![lit])
                }
                _ => None,
            },
        },
    }
}
