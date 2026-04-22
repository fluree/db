//! JSON and struct output for class stats from SPOT merge results.
//!
//! Resolves class sid64 → (ns_code, suffix) via targeted binary search into
//! flat dict files (avoids loading the full `BinaryIndexStore`).

use std::collections::HashMap;

use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::GraphId;
use rustc_hash::FxHashMap;

/// Sentinel datatype value used in [`SpotClassStats`] for object-reference
/// properties (`ObjKind::REF_ID`). Displayed as `@id` in stats output.
pub const DT_REF_ID: u16 = u16::MAX;

/// Class→property→datatype statistics collected during the SPOT merge.
///
/// Exploits SPOT ordering (subject-grouped) to compute class statistics
/// with O(properties-per-subject) memory per subject group. Only the global
/// accumulators grow with distinct classes/properties.
///
/// # Datatype keys
///
/// The inner `u16` key is a `DatatypeDictId` for literal values, or
/// [`DT_REF_ID`] (`u16::MAX`) for object references (`@id`).
#[derive(Debug, Default)]
pub struct SpotClassStats {
    /// (g_id, class_sid64) → instance count (number of subjects with this rdf:type)
    pub class_counts: FxHashMap<(GraphId, u64), u64>,
    /// (g_id, class_sid64) → p_id → dt → flake count
    pub class_prop_dts: FxHashMap<(GraphId, u64), FxHashMap<u32, FxHashMap<u16, u64>>>,
    /// (g_id, class_sid64) → p_id → lang_id → flake count
    pub class_prop_langs: FxHashMap<(GraphId, u64), FxHashMap<u32, FxHashMap<u16, u64>>>,
    /// (g_id, class_sid64) → p_id → target_class sid64 → count
    pub class_prop_refs: FxHashMap<(GraphId, u64), FxHashMap<u32, FxHashMap<u64, u64>>>,
}

/// Build JSON array for class→property→datatype stats from SPOT merge results.
///
/// Resolves class sid64 → (ns_code, suffix) via targeted binary search into
/// flat dict files (avoids loading the full BinaryIndexStore).
///
/// Shared between the import pipeline and rebuild pipeline.
pub fn build_class_stats_json(
    cs: &SpotClassStats,
    predicate_sids: &[(u16, String)],
    dt_tags: &[ValueTypeTag],
    run_dir: &std::path::Path,
    namespace_codes: &HashMap<u16, String>,
) -> std::io::Result<Vec<serde_json::Value>> {
    use crate::run_index::dict_io;

    use fluree_db_core::subject_id::SubjectId;
    use std::io::{Read as _, Seek as _, SeekFrom};

    if cs.class_counts.is_empty() {
        return Ok(Vec::new());
    }

    let sids_path = run_dir.join("subjects.sids");
    let idx_path = run_dir.join("subjects.idx");
    let fwd_path = run_dir.join("subjects.fwd");

    let sids_vec = dict_io::read_subject_sid_map(&sids_path)?;
    let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path)?;
    let mut fwd_file = std::fs::File::open(&fwd_path)?;

    // Helper: resolve sid64 → (ns_code, suffix_string).
    // subjects.sids is sorted (both vocab_merge and persist_merge_artifacts
    // guarantee monotonic sid64 order), so binary_search is safe.
    let resolve_sid = |sid64: u64, file: &mut std::fs::File| -> Option<(u16, String)> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let prefix = namespace_codes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some((ns_code, suffix.to_string()))
    };

    // Sort class entries by (g_id, class_sid64) for deterministic output.
    let mut class_entries: Vec<(&(GraphId, u64), &u64)> = cs.class_counts.iter().collect();
    class_entries.sort_by_key(|&(key, _)| *key);

    // Per-class ref-target data (if available).
    let class_refs = &cs.class_prop_refs;

    let classes_json: Vec<serde_json::Value> = class_entries
        .iter()
        .filter_map(|&(&(g_id, class_sid64), &count)| {
            let (ns_code, suffix) = resolve_sid(class_sid64, &mut fwd_file)?;

            // Look up this class's ref-target map (if any).
            let ref_map = class_refs.get(&(g_id, class_sid64));

            // Build property entries. Each property gets dt breakdown, and
            // optionally ref-target class info using the DB-R extended object
            // format: {"ref-classes": [[[ns, suffix], count], ...]}.
            let prop_json: Vec<serde_json::Value> = if let Some(prop_map) =
                cs.class_prop_dts.get(&(g_id, class_sid64))
            {
                let mut props: Vec<_> = prop_map.iter().collect();
                props.sort_by_key(|&(pid, _)| *pid);

                props
                    .iter()
                    .filter_map(|&(&p_id, dt_map)| {
                        let psid = predicate_sids.get(p_id as usize)?;

                        // Check if this property has ref-target class data.
                        let prop_refs = ref_map.and_then(|rm| rm.get(&p_id));

                        if let Some(target_map) = prop_refs {
                            // Emit DB-R extended object with ref-classes.
                            let mut targets: Vec<_> = target_map.iter().collect();
                            targets.sort_by_key(|&(sid, _)| *sid);
                            let refs_json: Vec<serde_json::Value> = targets
                                .iter()
                                .filter_map(|&(&target_sid, &tcount)| {
                                    let (tns, tsuffix) = resolve_sid(target_sid, &mut fwd_file)?;
                                    Some(serde_json::json!([[tns, tsuffix], tcount]))
                                })
                                .collect();
                            Some(serde_json::json!(
                                [[psid.0, &psid.1], {"ref-classes": refs_json}]
                            ))
                        } else {
                            // Standard format: property with datatype counts.
                            let mut dts: Vec<_> = dt_map.iter().collect();
                            dts.sort_by_key(|&(dt, _)| *dt);
                            let dt_json: Vec<serde_json::Value> = dts
                                .iter()
                                .map(|&(&dt, &count)| {
                                    if dt == DT_REF_ID {
                                        serde_json::json!(["@id", count])
                                    } else if let Some(tag) = dt_tags.get(dt as usize) {
                                        serde_json::json!([tag.as_u8(), count])
                                    } else {
                                        serde_json::json!([dt, count])
                                    }
                                })
                                .collect();
                            Some(serde_json::json!([[psid.0, &psid.1], dt_json]))
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            Some(serde_json::json!([[ns_code, suffix], [count, prop_json]]))
        })
        .collect();

    tracing::info!(classes = classes_json.len(), "class stats resolved to JSON");

    Ok(classes_json)
}

/// Build `ClassStatEntry` structs from SPOT class stats (struct-based, no JSON).
///
/// Returns a per-graph map: `GraphId → Vec<ClassStatEntry>`. Each graph gets its
/// own class stats reflecting only the subjects and properties within that graph.
///
/// Parallel to `build_class_stats_json` but returns typed structs suitable for
/// binary stats encoding in `IndexRoot`.
pub fn build_class_stat_entries(
    cs: &SpotClassStats,
    predicate_sids: &[(u16, String)],
    dt_tags: &[ValueTypeTag],
    language_tags: &[String],
    run_dir: &std::path::Path,
    namespace_codes: &HashMap<u16, String>,
) -> std::io::Result<HashMap<GraphId, Vec<fluree_db_core::ClassStatEntry>>> {
    use crate::run_index::dict_io;
    use fluree_db_core::sid::Sid;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ClassPropertyUsage, ClassRefCount, ClassStatEntry};
    use std::io::{Read as _, Seek as _, SeekFrom};

    if cs.class_counts.is_empty() {
        return Ok(HashMap::new());
    }

    let sids_path = run_dir.join("subjects.sids");
    let idx_path = run_dir.join("subjects.idx");
    let fwd_path = run_dir.join("subjects.fwd");

    let sids_vec = dict_io::read_subject_sid_map(&sids_path)?;
    let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path)?;
    let mut fwd_file = std::fs::File::open(&fwd_path)?;

    let resolve_sid = |sid64: u64, file: &mut std::fs::File| -> Option<Sid> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let prefix = namespace_codes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some(Sid::new(ns_code, suffix))
    };

    // Sort by (g_id, class_sid64) for deterministic output.
    let mut class_entries: Vec<(&(GraphId, u64), &u64)> = cs.class_counts.iter().collect();
    class_entries.sort_by_key(|&(key, _)| *key);

    let class_refs = &cs.class_prop_refs;

    let mut per_graph: HashMap<GraphId, Vec<ClassStatEntry>> = HashMap::new();

    for &(&(g_id, class_sid64), &count) in &class_entries {
        let class_sid = match resolve_sid(class_sid64, &mut fwd_file) {
            Some(s) => s,
            None => continue,
        };
        let ref_map = class_refs.get(&(g_id, class_sid64));

        let properties: Vec<ClassPropertyUsage> =
            if let Some(prop_map) = cs.class_prop_dts.get(&(g_id, class_sid64)) {
                let mut props: Vec<_> = prop_map.iter().collect();
                props.sort_by_key(|&(pid, _)| *pid);

                props
                    .iter()
                    .filter_map(|&(&p_id, dt_map)| {
                        let psid_pair = predicate_sids.get(p_id as usize)?;
                        let property_sid = Sid::new(psid_pair.0, &psid_pair.1);

                        // Build per-datatype counts.
                        let mut datatypes: Vec<(u8, u64)> = dt_map
                            .iter()
                            .map(|(&dt_dict_id, &count)| {
                                let tag = if dt_dict_id == DT_REF_ID {
                                    fluree_db_core::value_id::ValueTypeTag::JSON_LD_ID.as_u8()
                                } else {
                                    dt_tags
                                        .get(dt_dict_id as usize)
                                        .map(|t| t.as_u8())
                                        .unwrap_or(
                                            fluree_db_core::value_id::ValueTypeTag::UNKNOWN.as_u8(),
                                        )
                                };
                                (tag, count)
                            })
                            .collect();
                        datatypes.sort_by_key(|d| d.0);

                        // Build per-language-tag counts.
                        let lang_map = cs.class_prop_langs.get(&(g_id, class_sid64));
                        let langs: Vec<(String, u64)> =
                            if let Some(prop_langs) = lang_map.and_then(|lm| lm.get(&p_id)) {
                                let mut lv: Vec<(String, u64)> = prop_langs
                                    .iter()
                                    .filter_map(|(&lang_id, &count)| {
                                        // lang_id is 1-indexed; 0 = no language tag.
                                        let lang_str = language_tags
                                            .get((lang_id as usize).wrapping_sub(1))?;
                                        Some((lang_str.clone(), count))
                                    })
                                    .collect();
                                lv.sort_by(|a, b| a.0.cmp(&b.0));
                                lv
                            } else {
                                Vec::new()
                            };

                        // Ref-class targets for this property (if any).
                        let ref_classes: Vec<ClassRefCount> =
                            if let Some(target_map) = ref_map.and_then(|rm| rm.get(&p_id)) {
                                let mut targets: Vec<_> = target_map.iter().collect();
                                targets.sort_by_key(|&(sid, _)| *sid);
                                targets
                                    .iter()
                                    .filter_map(|&(&target_sid, &tcount)| {
                                        let tsid = resolve_sid(target_sid, &mut fwd_file)?;
                                        Some(ClassRefCount {
                                            class_sid: tsid,
                                            count: tcount,
                                        })
                                    })
                                    .collect()
                            } else {
                                Vec::new()
                            };

                        Some(ClassPropertyUsage {
                            property_sid,
                            datatypes,
                            langs,
                            ref_classes,
                        })
                    })
                    .collect()
            } else {
                Vec::new()
            };

        per_graph.entry(g_id).or_default().push(ClassStatEntry {
            class_sid,
            count,
            properties,
        });
    }

    let total_classes: usize = per_graph.values().map(std::vec::Vec::len).sum();
    tracing::info!(
        classes = total_classes,
        graphs = per_graph.len(),
        "class stats resolved to per-graph entries"
    );
    Ok(per_graph)
}
