//! Fast-path: label scan + regex filter + rdf:type membership check.
//!
//! Targets shapes like:
//! `SELECT ?s ?label WHERE { ?s rdfs:label ?label . ?s rdf:type <Class> . FILTER regex(?label, "pat") }`
//!
//! Motivation: when the class is very large (millions of subjects) but the label
//! predicate is relatively small, starting from rdf:type causes millions of
//! per-subject label lookups. Scanning the label predicate partition once and
//! applying regex can be far cheaper, then checking rdf:type only for the few
//! regex hits.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    empty_batch, fast_path_store, ref_to_p_id, term_to_ref_s_id, FastPathOperator,
};
use crate::ir::triple::{Ref, Term};
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_types::{BinaryFilter, ColumnProjection, ColumnSet};
use fluree_db_binary_index::{batched_lookup_predicate_refs, BinaryCursor};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId};
use fluree_vocab::xsd;
use regex::{Regex, RegexBuilder};
use std::sync::Arc;

/// Build the fused fast-path operator.
pub fn label_regex_type_operator(
    subject_var: VarId,
    label_var: VarId,
    label_pred: Ref,
    class_term: Term,
    regex_pattern: Arc<str>,
    regex_flags: Arc<str>,
    fallback: Option<crate::operator::BoxedOperator>,
) -> crate::operator::BoxedOperator {
    let schema: Arc<[VarId]> = Arc::from(vec![subject_var, label_var].into_boxed_slice());
    Box::new(FastPathOperator::with_schema(
        Arc::clone(&schema),
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let g_id: GraphId = ctx.binary_g_id;
            tracing::debug!(
                label_pred = ?label_pred,
                class = ?class_term,
                pattern = %regex_pattern,
                flags = %regex_flags,
                "fast-path: label-regex-type"
            );

            let label_p_id = ref_to_p_id(ctx, store.as_ref(), &label_pred)?;

            // Resolve class subject ID.
            let Some(class_s_id) = term_to_ref_s_id(ctx, store.as_ref(), &class_term)? else {
                return Ok(Some(empty_batch(schema.clone())?));
            };

            // Resolve rdf:type predicate ID.
            let rdf_type_p_id = store
                .find_predicate_id(fluree_vocab::rdf::TYPE)
                .ok_or_else(|| {
                    QueryError::execution("rdf:type predicate not in dict".to_string())
                })?;

            // Fast string match when pattern is plain ASCII with no flags/metacharacters.
            let literal_match = regex_flags.is_empty() && is_plain_literal(regex_pattern.as_ref());
            let literal = if literal_match {
                Some(regex_pattern.as_ref().to_string())
            } else {
                None
            };
            // Compile regex once otherwise.
            let re = if literal.is_none() {
                Some(build_regex(&regex_pattern, &regex_flags)?)
            } else {
                None
            };

            // Scan PSOT for label predicate (streaming).
            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::SId);
            needed.insert(ColumnId::OType);
            needed.insert(ColumnId::OKey);
            let projection = ColumnProjection {
                output: needed,
                internal: ColumnSet::EMPTY,
            };

            let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
                return Ok(Some(empty_batch(schema.clone())?));
            };
            let branch = Arc::clone(branch);

            // Full predicate range in PSOT.
            let min_key = RunRecordV2 {
                s_id: SubjectId(0),
                o_key: 0,
                p_id: label_p_id,
                t: 0,
                o_i: 0,
                o_type: 0,
                g_id,
            };
            let max_key = RunRecordV2 {
                s_id: SubjectId(u64::MAX),
                o_key: u64::MAX,
                p_id: label_p_id,
                t: u32::MAX,
                o_i: u32::MAX,
                o_type: u16::MAX,
                g_id,
            };
            let filter = BinaryFilter {
                p_id: Some(label_p_id),
                ..Default::default()
            };
            let mut cursor = BinaryCursor::new(
                Arc::clone(store),
                RunSortOrder::Psot,
                branch,
                &min_key,
                &max_key,
                filter,
                projection,
            );
            cursor.set_to_t(ctx.to_t);

            // Collect matches (expected to be small for typical regex filters).
            const MAX_HITS: usize = 200_000;
            let mut hit_subjects: Vec<u64> = Vec::new();
            let mut hit_labels: Vec<(String, Option<String>)> = Vec::new();

            let mut buf: Vec<u8> = Vec::with_capacity(128);
            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::Internal(format!("binary cursor: {e}")))?
            {
                for i in 0..batch.row_count {
                    let ot_u16 = batch.o_type.get_or(i, 0);
                    let ot = OType::from_u16(ot_u16);
                    if !matches!(ot, OType::XSD_STRING) && !ot.is_lang_string() {
                        continue;
                    }
                    let str_id = batch.o_key.get(i) as u32;
                    buf.clear();
                    let found = store
                        .string_lookup_into(str_id, &mut buf)
                        .map_err(|e| QueryError::Internal(format!("string_lookup_into: {e}")))?;
                    if !found {
                        continue;
                    }
                    let s_ref = std::str::from_utf8(&buf)
                        .map_err(|e| QueryError::Internal(format!("label not utf8: {e}")))?;
                    let matches = if let Some(lit) = &literal {
                        s_ref.contains(lit)
                    } else {
                        re.as_ref().expect("regex compiled").is_match(s_ref)
                    };
                    if !matches {
                        continue;
                    }
                    let s_id = batch.s_id.get(i);
                    hit_subjects.push(s_id);
                    let lang = if ot.is_lang_string() {
                        store
                            .resolve_lang_tag(ot_u16)
                            .map(std::string::ToString::to_string)
                    } else {
                        None
                    };
                    hit_labels.push((s_ref.to_string(), lang));
                    if hit_subjects.len() >= MAX_HITS {
                        // Too many matches; fall back to generic pipeline rather than
                        // doing per-subject rdf:type checks.
                        return Ok(None);
                    }
                }
            }

            if hit_subjects.is_empty() {
                return Ok(Some(empty_batch(schema.clone())?));
            }

            // Batched rdf:type membership lookup for all hit subjects.
            let mut uniq_subjects = hit_subjects.clone();
            uniq_subjects.sort_unstable();
            uniq_subjects.dedup();
            let type_map =
                batched_lookup_predicate_refs(store, g_id, rdf_type_p_id, &uniq_subjects, ctx.to_t)
                    .map_err(|e| {
                        QueryError::Internal(format!("batched_lookup_predicate_refs: {e}"))
                    })?;

            let dt_sid = store.as_ref().encode_iri(xsd::STRING);
            let mut col_s: Vec<Binding> = Vec::new();
            let mut col_label: Vec<Binding> = Vec::new();

            for (idx, s_id) in hit_subjects.iter().copied().enumerate() {
                let has = type_map
                    .get(&s_id)
                    .is_some_and(|vals| vals.binary_search(&class_s_id).is_ok());
                if !has {
                    continue;
                }
                col_s.push(Binding::encoded_sid(s_id));
                let (label, lang) = &hit_labels[idx];
                let lit = FlakeValue::String(label.clone());
                col_label.push(match lang {
                    Some(tag) => Binding::lit_lang(lit, Arc::from(tag.as_str())),
                    None => Binding::lit(lit, dt_sid.clone()),
                });
            }

            Ok(Some(Batch::new(schema.clone(), vec![col_s, col_label])?))
        },
        fallback,
        "label-regex-type",
    ))
}

fn build_regex(pattern: &str, flags: &str) -> Result<Regex> {
    let mut builder = RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            c => {
                return Err(QueryError::InvalidFilter(format!(
                    "Unknown regex flag: '{c}'"
                )))
            }
        }
    }
    builder
        .build()
        .map_err(|e| QueryError::InvalidFilter(format!("Invalid regex: {e}")))
}

fn is_plain_literal(pattern: &str) -> bool {
    // Conservative: treat as literal only if it contains no common regex metacharacters.
    !pattern.bytes().any(|b| {
        matches!(
            b,
            b'.' | b'+'
                | b'*'
                | b'?'
                | b'('
                | b')'
                | b'['
                | b']'
                | b'{'
                | b'}'
                | b'|'
                | b'\\'
                | b'^'
                | b'$'
        )
    })
}
