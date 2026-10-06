//! Binary wire format for index stats and schema sections.
//!
//! These are embedded in the `IndexRoot` binary root (not separate blobs).
//! The encodings are fully structured binary — no JSON anywhere.
//!
//! ## Stats wire format
//!
//! See `encode_stats()` / `decode_stats()`. Determinism invariants:
//! - Graphs sorted by `g_id`, properties by `p_id`
//! - Aggregate properties sorted by `(ns_code, suffix)`
//! - Classes sorted by `(ns_code, suffix)`, properties within classes likewise
//! - The ledger-wide class list is written only when no graph carries classes;
//!   otherwise decoders derive it as the union of the graphs' lists
//! - Historical tail entries sorted by sid / `(g_id, p_id)`, tags sorted
//!
//! An optional historical-datatypes tail follows the classes section — see
//! `encode_historical_tail` for the layout and why appending is safe for
//! readers on both sides of the change.
//!
//! ## Schema wire format
//!
//! See `encode_schema()` / `decode_schema()`. Determinism invariants:
//! - Entries sorted by `(ns_code, suffix)` (Sid ordering)

use fluree_db_core::index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
use fluree_db_core::index_stats::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry,
    IndexStats, PropertyStatEntry,
};
use fluree_db_core::sid::Sid;
use std::io;

// ============================================================================
// Shared helpers: Sid wire encoding
// ============================================================================

/// Encode a `Sid` as `(ns_code: u16 LE, suffix_len: u16 LE, suffix_bytes)`.
fn write_sid(buf: &mut Vec<u8>, sid: &Sid) {
    buf.extend_from_slice(&sid.namespace_code.to_le_bytes());
    let name_bytes = sid.name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(name_bytes);
}

/// Encode a `(ns_code, suffix)` tuple the same way.
fn write_sid_tuple(buf: &mut Vec<u8>, ns_code: u16, suffix: &str) {
    buf.extend_from_slice(&ns_code.to_le_bytes());
    let suffix_bytes = suffix.as_bytes();
    buf.extend_from_slice(&(suffix_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(suffix_bytes);
}

/// Decode a Sid from wire format. Returns `(sid, bytes_consumed)`.
fn read_sid(data: &[u8], pos: usize) -> io::Result<(Sid, usize)> {
    let mut p = pos;
    ensure_len(data, p, 4, "sid header")?;
    let ns_code = u16::from_le_bytes(data[p..p + 2].try_into().unwrap());
    p += 2;
    let suffix_len = u16::from_le_bytes(data[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    ensure_len(data, p, suffix_len, "sid suffix")?;
    let suffix = std::str::from_utf8(&data[p..p + suffix_len]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in sid: {e}"),
        )
    })?;
    p += suffix_len;
    Ok((Sid::new(ns_code, suffix), p))
}

/// Check that `data[pos..pos+need]` is within bounds.
#[inline]
fn ensure_len(data: &[u8], pos: usize, need: usize, ctx: &str) -> io::Result<()> {
    if pos + need > data.len() {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stats/schema: truncated at {ctx} (need {need} bytes at offset {pos}, have {})",
                data.len()
            ),
        ))
    } else {
        Ok(())
    }
}

/// Read a u16 LE at `pos`, advancing.
#[inline]
fn read_u16(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    ensure_len(data, *pos, 2, "u16")?;
    let v = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

/// Read a u32 LE at `pos`, advancing.
#[inline]
fn read_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    ensure_len(data, *pos, 4, "u32")?;
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

/// Read an i64 LE at `pos`, advancing.
#[inline]
fn read_i64(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    ensure_len(data, *pos, 8, "i64")?;
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

// ============================================================================
// Stats encode
// ============================================================================

/// Encode `IndexStats` to the binary stats section wire format.
///
/// Determinism: graphs sorted by g_id, properties by p_id, aggregate
/// properties by (ns_code, suffix), classes by (ns_code, suffix).
pub fn encode_stats(stats: &IndexStats) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    // Top-level aggregates
    buf.extend_from_slice(&stats.flakes.to_le_bytes());
    buf.extend_from_slice(&stats.size.to_le_bytes());

    // Per-graph stats
    let graphs = stats.graphs.as_deref().unwrap_or(&[]);
    let mut sorted_graphs: Vec<&GraphStatsEntry> = graphs.iter().collect();
    sorted_graphs.sort_by_key(|g| g.g_id);

    buf.extend_from_slice(&(sorted_graphs.len() as u16).to_le_bytes());
    for g in &sorted_graphs {
        buf.extend_from_slice(&g.g_id.to_le_bytes());
        buf.extend_from_slice(&g.flakes.to_le_bytes());
        buf.extend_from_slice(&g.size.to_le_bytes());

        let mut sorted_props: Vec<&GraphPropertyStatEntry> = g.properties.iter().collect();
        sorted_props.sort_by_key(|p| p.p_id);
        buf.extend_from_slice(&(sorted_props.len() as u32).to_le_bytes());
        for p in &sorted_props {
            encode_graph_property(&mut buf, p);
        }

        // Per-graph classes travel in the compact class tail; the legacy slot
        // stays empty so readers that predate the tail see no classes rather
        // than misparse.
        buf.push(0);
    }

    // Aggregate properties (SID-keyed)
    let agg_props = stats.properties.as_deref().unwrap_or(&[]);
    let mut sorted_agg: Vec<&PropertyStatEntry> = agg_props.iter().collect();
    sorted_agg.sort_by(|a, b| a.sid.0.cmp(&b.sid.0).then_with(|| a.sid.1.cmp(&b.sid.1)));

    buf.extend_from_slice(&(sorted_agg.len() as u32).to_le_bytes());
    for p in &sorted_agg {
        write_sid_tuple(&mut buf, p.sid.0, &p.sid.1);
        buf.extend_from_slice(&p.count.to_le_bytes());
        buf.extend_from_slice(&p.ndv_values.to_le_bytes());
        buf.extend_from_slice(&p.ndv_subjects.to_le_bytes());
        buf.extend_from_slice(&p.last_modified_t.to_le_bytes());
        encode_datatypes(&mut buf, &p.datatypes);
    }

    // Classes. The ledger-wide table is the union of the graphs' tables
    // (`union_per_graph_classes`), so once graphs carry classes it is derived
    // on decode rather than stored a second time.
    let graphs_carry_classes = graphs.iter().any(|g| g.classes.is_some());
    let classes = match stats.classes.as_deref() {
        Some(classes) if !graphs_carry_classes => classes,
        _ => &[],
    };
    let mut sorted_classes: Vec<&ClassStatEntry> = classes.iter().collect();
    sorted_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

    buf.extend_from_slice(&(sorted_classes.len() as u32).to_le_bytes());
    for c in &sorted_classes {
        write_sid(&mut buf, &c.class_sid);
        buf.extend_from_slice(&c.count.to_le_bytes());

        let mut sorted_props: Vec<&ClassPropertyUsage> = c.properties.iter().collect();
        sorted_props.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));

        buf.extend_from_slice(&(sorted_props.len() as u16).to_le_bytes());
        for pu in &sorted_props {
            write_sid(&mut buf, &pu.property_sid);
            encode_class_property_payload(&mut buf, pu);
        }
    }

    // Historical tail (see `encode_historical_tail` for the evolution rules).
    encode_historical_tail(&mut buf, stats);
    if graphs_carry_classes {
        fluree_db_core::stats_wire::encode_class_tail(&mut buf, &sorted_graphs);
    }
    if let Some(links) = &stats.links {
        fluree_db_core::stats_wire::encode_link_tail(&mut buf, links);
    }

    buf
}

/// Wire tag identifying the v1 historical tail.
const HISTORICAL_TAIL_TAG: u8 = 1;

/// Append the historical-datatypes tail section.
///
/// ## Why an appended tail is safe in both directions
///
/// The stats section is embedded in the FIR6 root behind a `u32` length
/// prefix, and every root decoder slices exactly that many bytes and advances
/// by the *prefix*, discarding the decoder's own consumed count. So:
///
/// - **New blob, old reader**: the old decoder parses the sections it knows,
///   stops before the tail, and the root decoder skips the tail via the
///   length prefix. Nothing misparses.
/// - **Old blob, new reader**: after the classes section `pos == len`, so the
///   tail reads as absent — no boundary, empty historical sets, and every
///   consumer falls back to today's conservative behavior.
/// - **Future tail versions**: the leading tag byte gates the parse; an
///   unknown tag consumes the remainder (bounded by the same length prefix)
///   and reads as absent.
///
/// ## Layout (only present when `historical_since_t` is `Some`)
///
/// ```text
/// [tag: u8 = 1]
/// [historical_since_t: i64 LE]
/// [agg_count: u32 LE]
///   per entry (sorted by sid; empty sets skipped):
///     [sid tuple][n: u8][n tag bytes]
/// [graph_count: u16 LE]
///   per graph (sorted by g_id; graphs with no sets skipped):
///     [g_id: u16 LE][prop_count: u32 LE]
///     per property (sorted by p_id; empty sets skipped):
///       [p_id: u32 LE][n: u8][n tag bytes]
/// ```
fn encode_historical_tail(buf: &mut Vec<u8>, stats: &IndexStats) {
    let Some(since_t) = stats.historical_since_t else {
        return;
    };
    buf.push(HISTORICAL_TAIL_TAG);
    buf.extend_from_slice(&since_t.to_le_bytes());

    // Aggregate sets, keyed by sid.
    let agg_props = stats.properties.as_deref().unwrap_or(&[]);
    let mut agg: Vec<(&PropertyStatEntry, Vec<u8>)> = agg_props
        .iter()
        .filter_map(|p| encodable_tag_set(&p.historical_datatypes).map(|tags| (p, tags)))
        .collect();
    agg.sort_by(|a, b| {
        a.0.sid
            .0
            .cmp(&b.0.sid.0)
            .then_with(|| a.0.sid.1.cmp(&b.0.sid.1))
    });
    buf.extend_from_slice(&(agg.len() as u32).to_le_bytes());
    for (p, tags) in &agg {
        write_sid_tuple(buf, p.sid.0, &p.sid.1);
        buf.push(tags.len() as u8);
        buf.extend_from_slice(tags);
    }

    // Graph-scoped sets, keyed by (g_id, p_id).
    let graphs = stats.graphs.as_deref().unwrap_or(&[]);
    let mut graph_sets: GraphTagSets = graphs
        .iter()
        .filter_map(|g| {
            let mut props: Vec<(u32, Vec<u8>)> = g
                .properties
                .iter()
                .filter_map(|p| {
                    encodable_tag_set(&p.historical_datatypes).map(|tags| (p.p_id, tags))
                })
                .collect();
            if props.is_empty() {
                return None;
            }
            props.sort_by_key(|&(p_id, _)| p_id);
            Some((g.g_id, props))
        })
        .collect();
    graph_sets.sort_by_key(|&(g_id, _)| g_id);
    buf.extend_from_slice(&(graph_sets.len() as u16).to_le_bytes());
    for (g_id, props) in &graph_sets {
        buf.extend_from_slice(&g_id.to_le_bytes());
        buf.extend_from_slice(&(props.len() as u32).to_le_bytes());
        for (p_id, tags) in props {
            buf.extend_from_slice(&p_id.to_le_bytes());
            buf.push(tags.len() as u8);
            buf.extend_from_slice(tags);
        }
    }
}

/// Sorted, deduplicated tag set ready for the wire — or `None` when there is
/// nothing to write (empty means "unknown" and is represented by absence) or
/// the set cannot be length-prefixed in a `u8` (a full 256-tag set, which
/// cannot be truncated soundly: a dropped tag could *license* an optimization,
/// where absence merely declines one).
fn encodable_tag_set(tags: &[u8]) -> Option<Vec<u8>> {
    if tags.is_empty() {
        return None;
    }
    let mut sorted = tags.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.len() > u8::MAX as usize {
        return None;
    }
    Some(sorted)
}

/// Encode the per-property payload within a class section: datatypes, langs, ref_classes.
fn encode_class_property_payload(buf: &mut Vec<u8>, pu: &ClassPropertyUsage) {
    // Datatypes: sorted by tag.
    let mut sorted_dts: Vec<&(u8, u64)> = pu.datatypes.iter().collect();
    sorted_dts.sort_by_key(|d| d.0);
    buf.extend_from_slice(&(sorted_dts.len() as u16).to_le_bytes());
    for &&(tag, count) in &sorted_dts {
        buf.push(tag);
        buf.extend_from_slice(&count.to_le_bytes());
    }

    // Langs: sorted by lang string.
    let mut sorted_langs: Vec<&(String, u64)> = pu.langs.iter().collect();
    sorted_langs.sort_by(|a, b| a.0.cmp(&b.0));
    buf.extend_from_slice(&(sorted_langs.len() as u16).to_le_bytes());
    for (lang, count) in &sorted_langs {
        let lang_bytes = lang.as_bytes();
        buf.extend_from_slice(&(lang_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(lang_bytes);
        buf.extend_from_slice(&count.to_le_bytes());
    }

    // Ref classes: sorted by class_sid.
    let mut sorted_refs: Vec<&ClassRefCount> = pu.ref_classes.iter().collect();
    sorted_refs.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
    buf.extend_from_slice(&(sorted_refs.len() as u16).to_le_bytes());
    for rc in &sorted_refs {
        write_sid(buf, &rc.class_sid);
        buf.extend_from_slice(&rc.count.to_le_bytes());
    }
}

fn encode_graph_property(buf: &mut Vec<u8>, p: &GraphPropertyStatEntry) {
    buf.extend_from_slice(&p.p_id.to_le_bytes());
    buf.extend_from_slice(&p.count.to_le_bytes());
    buf.extend_from_slice(&p.ndv_values.to_le_bytes());
    buf.extend_from_slice(&p.ndv_subjects.to_le_bytes());
    buf.extend_from_slice(&p.last_modified_t.to_le_bytes());
    encode_datatypes(buf, &p.datatypes);
}

fn encode_datatypes(buf: &mut Vec<u8>, datatypes: &[(u8, u64)]) {
    buf.push(datatypes.len() as u8);
    for &(dt_tag, dt_count) in datatypes {
        buf.push(dt_tag);
        buf.extend_from_slice(&dt_count.to_le_bytes());
    }
}

// ============================================================================
// Stats decode
// ============================================================================

/// Decode `IndexStats` from the binary stats section wire format.
///
/// Thin wrapper over [`decode_stats_with_len`] — one implementation parses
/// the format (including the historical tail), so a field cannot be filled by
/// one entry point and forgotten by the other.
pub fn decode_stats(data: &[u8]) -> io::Result<IndexStats> {
    decode_stats_with_len(data).map(|(stats, _)| stats)
}

/// One graph's rows in the tail: `(g_id, [(p_id, tags)])`.
type GraphTagSets = Vec<(u16, Vec<(u32, Vec<u8>)>)>;

// ============================================================================
// Schema encode
// ============================================================================

/// Encode `IndexSchema` to the binary schema section wire format.
///
/// Sids are encoded as `(ns_code: u16, suffix_len: u16, suffix_bytes)`.
/// Entries are sorted by Sid ordering (ns_code, then suffix).
pub fn encode_schema(schema: &IndexSchema) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    buf.extend_from_slice(&schema.t.to_le_bytes());

    let mut sorted_entries: Vec<&SchemaPredicateInfo> = schema.pred.vals.iter().collect();
    sorted_entries.sort_by(|a, b| a.id.cmp(&b.id));

    buf.extend_from_slice(&(sorted_entries.len() as u32).to_le_bytes());
    for entry in &sorted_entries {
        write_sid(&mut buf, &entry.id);

        // subclass_of
        let mut sorted_sc: Vec<&Sid> = entry.subclass_of.iter().collect();
        sorted_sc.sort();
        buf.extend_from_slice(&(sorted_sc.len() as u16).to_le_bytes());
        for sid in &sorted_sc {
            write_sid(&mut buf, sid);
        }

        // parent_props
        let mut sorted_pp: Vec<&Sid> = entry.parent_props.iter().collect();
        sorted_pp.sort();
        buf.extend_from_slice(&(sorted_pp.len() as u16).to_le_bytes());
        for sid in &sorted_pp {
            write_sid(&mut buf, sid);
        }

        // child_props
        let mut sorted_cp: Vec<&Sid> = entry.child_props.iter().collect();
        sorted_cp.sort();
        buf.extend_from_slice(&(sorted_cp.len() as u16).to_le_bytes());
        for sid in &sorted_cp {
            write_sid(&mut buf, sid);
        }
    }

    buf
}

// ============================================================================
// Schema decode
// ============================================================================

/// Decode `IndexSchema` from the binary schema section wire format.
pub fn decode_schema(data: &[u8]) -> io::Result<IndexSchema> {
    let mut pos = 0usize;

    let t = read_i64(data, &mut pos)?;
    let entry_count = read_u32(data, &mut pos)? as usize;

    let mut vals = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (id, new_pos) = read_sid(data, pos)?;
        pos = new_pos;

        // subclass_of
        let sc_count = read_u16(data, &mut pos)? as usize;
        let mut subclass_of = Vec::with_capacity(sc_count);
        for _ in 0..sc_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            subclass_of.push(sid);
        }

        // parent_props
        let pp_count = read_u16(data, &mut pos)? as usize;
        let mut parent_props = Vec::with_capacity(pp_count);
        for _ in 0..pp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            parent_props.push(sid);
        }

        // child_props
        let cp_count = read_u16(data, &mut pos)? as usize;
        let mut child_props = Vec::with_capacity(cp_count);
        for _ in 0..cp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            child_props.push(sid);
        }

        vals.push(SchemaPredicateInfo {
            id,
            subclass_of,
            parent_props,
            child_props,
        });
    }

    Ok(IndexSchema {
        t,
        pred: SchemaPredicates {
            keys: vec![
                "id".to_string(),
                "subclassOf".to_string(),
                "parentProps".to_string(),
                "childProps".to_string(),
            ],
            vals,
        },
    })
}

// ============================================================================
// Public helpers for root encoder
// ============================================================================

/// Decode stats and the number of bytes consumed. The format has one parser,
/// `fluree_db_core::stats_wire::decode_stats`, shared with the snapshot's
/// metadata-only root decoder.
pub fn decode_stats_with_len(data: &[u8]) -> io::Result<(IndexStats, usize)> {
    fluree_db_core::stats_wire::decode_stats(data)
}

/// Decode schema and return bytes consumed.
pub fn decode_schema_with_len(data: &[u8]) -> io::Result<(IndexSchema, usize)> {
    let mut pos = 0usize;

    let t = read_i64(data, &mut pos)?;
    let entry_count = read_u32(data, &mut pos)? as usize;

    let mut vals = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (id, new_pos) = read_sid(data, pos)?;
        pos = new_pos;

        let sc_count = read_u16(data, &mut pos)? as usize;
        let mut subclass_of = Vec::with_capacity(sc_count);
        for _ in 0..sc_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            subclass_of.push(sid);
        }

        let pp_count = read_u16(data, &mut pos)? as usize;
        let mut parent_props = Vec::with_capacity(pp_count);
        for _ in 0..pp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            parent_props.push(sid);
        }

        let cp_count = read_u16(data, &mut pos)? as usize;
        let mut child_props = Vec::with_capacity(cp_count);
        for _ in 0..cp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            child_props.push(sid);
        }

        vals.push(SchemaPredicateInfo {
            id,
            subclass_of,
            parent_props,
            child_props,
        });
    }

    let schema = IndexSchema {
        t,
        pred: SchemaPredicates {
            keys: vec![
                "id".to_string(),
                "subclassOf".to_string(),
                "parentProps".to_string(),
                "childProps".to_string(),
            ],
            vals,
        },
    };

    Ok((schema, pos))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    // ---- Stats tests ----

    #[test]
    fn test_stats_empty_round_trip() {
        let stats = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        assert_eq!(decoded.flakes, 0);
        assert_eq!(decoded.size, 0);
        assert!(decoded.properties.is_none());
        assert!(decoded.classes.is_none());
        assert!(decoded.graphs.is_none());
    }

    #[test]
    fn test_stats_with_graphs_round_trip() {
        let stats = IndexStats {
            flakes: 50_000,
            size: 1_000_000,
            properties: None,
            classes: None,
            graphs: Some(vec![
                GraphStatsEntry {
                    g_id: 0,
                    flakes: 40_000,
                    size: 800_000,
                    properties: vec![
                        GraphPropertyStatEntry {
                            p_id: 1,
                            count: 10_000,
                            ndv_values: 5_000,
                            ndv_subjects: 2_000,
                            last_modified_t: 42,
                            datatypes: vec![(3, 8_000), (7, 2_000)],
                            observed_datatypes: vec![3, 7],
                            historical_datatypes: vec![],
                        },
                        GraphPropertyStatEntry {
                            p_id: 5,
                            count: 30_000,
                            ndv_values: 15_000,
                            ndv_subjects: 10_000,
                            last_modified_t: 100,
                            datatypes: vec![(1, 30_000)],
                            observed_datatypes: vec![1],
                            historical_datatypes: vec![],
                        },
                    ],
                    classes: None,
                },
                GraphStatsEntry {
                    g_id: 1,
                    flakes: 10_000,
                    size: 200_000,
                    properties: vec![],
                    classes: None,
                },
            ]),
            historical_since_t: None,
            links: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        assert_eq!(decoded.flakes, 50_000);
        assert_eq!(decoded.size, 1_000_000);
        let graphs = decoded.graphs.unwrap();
        assert_eq!(graphs.len(), 2);
        assert_eq!(graphs[0].g_id, 0);
        assert_eq!(graphs[0].properties.len(), 2);
        assert_eq!(graphs[0].properties[0].p_id, 1);
        assert_eq!(graphs[0].properties[0].datatypes.len(), 2);
        assert_eq!(graphs[0].properties[1].p_id, 5);
        assert_eq!(graphs[1].g_id, 1);
        assert_eq!(graphs[1].properties.len(), 0);
    }

    #[test]
    fn test_stats_with_agg_properties_round_trip() {
        let stats = IndexStats {
            flakes: 100,
            size: 500,
            properties: Some(vec![
                PropertyStatEntry {
                    sid: (10, "name".to_string()),
                    count: 50,
                    ndv_values: 45,
                    ndv_subjects: 50,
                    last_modified_t: 3,
                    datatypes: vec![(1, 50)],
                    observed_datatypes: vec![1],
                    historical_datatypes: vec![],
                },
                PropertyStatEntry {
                    sid: (10, "age".to_string()),
                    count: 50,
                    ndv_values: 30,
                    ndv_subjects: 50,
                    last_modified_t: 3,
                    datatypes: vec![(3, 50)],
                    observed_datatypes: vec![3],
                    historical_datatypes: vec![],
                },
            ]),
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        let props = decoded.properties.unwrap();
        assert_eq!(props.len(), 2);
        // Sorted by (ns_code, suffix): "age" < "name"
        assert_eq!(props[0].sid.1, "age");
        assert_eq!(props[1].sid.1, "name");
    }

    #[test]
    fn test_stats_with_classes_round_trip() {
        let stats = IndexStats {
            flakes: 200,
            size: 1000,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: sid(5, "Person"),
                count: 100,
                properties: vec![
                    ClassPropertyUsage {
                        property_sid: sid(5, "name"),
                        datatypes: vec![],
                        langs: vec![],
                        ref_classes: vec![],
                    },
                    ClassPropertyUsage {
                        property_sid: sid(5, "knows"),
                        datatypes: vec![],
                        langs: vec![],
                        ref_classes: vec![
                            ClassRefCount {
                                class_sid: sid(5, "Person"),
                                count: 80,
                            },
                            ClassRefCount {
                                class_sid: sid(5, "Organization"),
                                count: 20,
                            },
                        ],
                    },
                ],
            }]),
            graphs: None,
            historical_since_t: None,
            links: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        let classes = decoded.classes.unwrap();
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].class_sid, sid(5, "Person"));
        assert_eq!(classes[0].count, 100);
        assert_eq!(classes[0].properties.len(), 2);

        // "knows" < "name" in sort order
        assert_eq!(classes[0].properties[0].property_sid, sid(5, "knows"));
        assert_eq!(classes[0].properties[0].ref_classes.len(), 2);
        // "Organization" < "Person"
        assert_eq!(
            classes[0].properties[0].ref_classes[0].class_sid,
            sid(5, "Organization")
        );
        assert_eq!(classes[0].properties[0].ref_classes[0].count, 20);
        assert_eq!(
            classes[0].properties[0].ref_classes[1].class_sid,
            sid(5, "Person")
        );
        assert_eq!(classes[0].properties[0].ref_classes[1].count, 80);

        assert_eq!(classes[0].properties[1].property_sid, sid(5, "name"));
        assert_eq!(classes[0].properties[1].ref_classes.len(), 0);
    }

    /// The ledger-wide class list is the union of the graphs' lists, so a root
    /// does not store it twice: the bytes are those of a root without it, and
    /// both decoders hand back the union, also for an import root that never
    /// set it.
    fn class_tables() -> Vec<GraphStatsEntry> {
        let usage = |property: Sid, refs: &[(Sid, u64)]| ClassPropertyUsage {
            property_sid: property,
            datatypes: vec![(7, 3), (1, 300)],
            langs: vec![("fr".to_string(), 2), ("en".to_string(), 1)],
            ref_classes: refs
                .iter()
                .map(|(class_sid, count)| ClassRefCount {
                    class_sid: class_sid.clone(),
                    count: *count,
                })
                .collect(),
        };
        let class = |class_sid: Sid, count: u64, properties| ClassStatEntry {
            class_sid,
            count,
            properties,
        };
        let graph = |g_id: u16, classes: Option<Vec<ClassStatEntry>>| GraphStatsEntry {
            g_id,
            flakes: 10,
            size: 0,
            properties: vec![],
            classes,
        };
        vec![
            graph(
                0,
                Some(vec![
                    class(
                        sid(5, "PersonB"),
                        1 << 62,
                        vec![
                            usage(sid(5, "name"), &[]),
                            usage(
                                sid(5, "knows"),
                                &[(sid(6, "Person"), 9), (sid(5, "PersonA"), 1 << 40)],
                            ),
                        ],
                    ),
                    class(sid(5, "PersonA"), u64::MAX, vec![]),
                ]),
            ),
            graph(1, None),
            // Its first class is not the table's first Sid, so a class index
            // read as absolute instead of as a delta lands elsewhere.
            graph(
                2,
                Some(vec![
                    class(
                        sid(6, "Person"),
                        1,
                        vec![usage(sid(5, "knows"), &[(sid(5, "PersonB"), 2)])],
                    ),
                    class(sid(5, "PersonB"), 2, vec![]),
                ]),
            ),
        ]
    }

    /// The canonical (sorted) form both encoders have always emitted.
    fn sorted_classes(classes: &[ClassStatEntry]) -> Vec<ClassStatEntry> {
        let mut classes = classes.to_vec();
        classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
        for class in &mut classes {
            class
                .properties
                .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
            for usage in &mut class.properties {
                usage.datatypes.sort_by_key(|d| d.0);
                usage.langs.sort();
                usage
                    .ref_classes
                    .sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
            }
        }
        classes
    }

    #[test]
    fn per_graph_class_tables_round_trip_through_the_class_tail() {
        let graphs = class_tables();
        let stats = IndexStats {
            flakes: 30,
            size: 0,
            properties: None,
            classes: None,
            graphs: Some(graphs.clone()),
            historical_since_t: Some(0),
            links: None,
        };
        let bytes = encode_stats(&stats);
        let (via_core, consumed) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        for decoded in [decode_stats(&bytes).unwrap(), via_core] {
            assert_eq!(
                decoded.historical_since_t,
                Some(0),
                "the historical tail still reads"
            );
            let got = decoded.graphs.unwrap();
            assert_eq!(got.len(), 3);
            for (got, want) in got.iter().zip(&graphs) {
                assert_eq!(got.g_id, want.g_id);
                assert_eq!(
                    format!("{:?}", got.classes),
                    format!("{:?}", want.classes.as_deref().map(sorted_classes)),
                    "graph {}",
                    want.g_id
                );
            }
        }
    }

    /// A reader that predates the class tail parses up to it and stops; what
    /// it sees must be exactly the stats without class tables.
    #[test]
    fn a_reader_without_the_class_tail_sees_no_classes() {
        let with = IndexStats {
            flakes: 30,
            size: 0,
            properties: None,
            classes: None,
            graphs: Some(class_tables()),
            historical_since_t: Some(0),
            links: None,
        };
        let mut without = with.clone();
        for g in without.graphs.iter_mut().flatten() {
            g.classes = None;
        }
        let with = encode_stats(&with);
        let without = encode_stats(&without);
        assert!(with.len() > without.len());
        assert_eq!(&with[..without.len()], &without[..]);
        assert_eq!(with[without.len()], 2, "the class tail follows");
    }

    /// Roots written before the class tail carry each graph's classes in the
    /// legacy per-graph slot, Sids spelled out, fixed-width counts.
    #[test]
    fn legacy_per_graph_class_slots_still_decode() {
        let mut b = Vec::new();
        let put_sid = |b: &mut Vec<u8>, ns: u16, name: &str| {
            b.extend_from_slice(&ns.to_le_bytes());
            b.extend_from_slice(&(name.len() as u16).to_le_bytes());
            b.extend_from_slice(name.as_bytes());
        };
        b.extend_from_slice(&5u64.to_le_bytes()); // flakes
        b.extend_from_slice(&0u64.to_le_bytes()); // size
        b.extend_from_slice(&1u16.to_le_bytes()); // graphs
        b.extend_from_slice(&0u16.to_le_bytes()); // g_id
        b.extend_from_slice(&5u64.to_le_bytes());
        b.extend_from_slice(&0u64.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes()); // properties
        b.push(1); // has_classes
        b.extend_from_slice(&1u32.to_le_bytes());
        put_sid(&mut b, 5, "Person");
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes()); // property usages
        put_sid(&mut b, 5, "knows");
        b.extend_from_slice(&1u16.to_le_bytes()); // datatypes
        b.push(1);
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes()); // langs
        b.extend_from_slice(&1u16.to_le_bytes()); // ref classes
        put_sid(&mut b, 5, "Person");
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes()); // aggregate properties
        b.extend_from_slice(&0u32.to_le_bytes()); // aggregate classes

        let decoded = decode_stats(&b).unwrap();
        let graph = &decoded.graphs.as_ref().unwrap()[0];
        let classes = graph.classes.as_ref().unwrap();
        assert_eq!(classes[0].class_sid, sid(5, "Person"));
        assert_eq!(classes[0].properties[0].ref_classes[0].count, 3);
        assert_eq!(
            format!("{:?}", decoded.classes),
            format!("{:?}", graph.classes),
            "the ledger-wide table is derived"
        );
    }

    #[test]
    fn the_ledger_wide_class_list_is_derived_from_the_graphs() {
        let class = |name: &str, count: u64| ClassStatEntry {
            class_sid: sid(5, name),
            count,
            properties: vec![ClassPropertyUsage {
                property_sid: sid(5, "knows"),
                datatypes: vec![(1, count)],
                langs: vec![],
                ref_classes: vec![ClassRefCount {
                    class_sid: sid(5, "Person"),
                    count,
                }],
            }],
        };
        let graph = |g_id: u16, classes: Vec<ClassStatEntry>| GraphStatsEntry {
            g_id,
            flakes: 10,
            size: 0,
            properties: vec![],
            classes: Some(classes),
        };
        let graphs = vec![
            graph(0, vec![class("Org", 2), class("Person", 3)]),
            graph(1, vec![class("Person", 4)]),
        ];
        let union = fluree_db_core::index_stats::union_per_graph_classes(&graphs);
        let built = IndexStats {
            flakes: 20,
            size: 0,
            properties: None,
            classes: union.clone(),
            graphs: Some(graphs),
            historical_since_t: None,
            links: None,
        };
        let imported = IndexStats {
            classes: None,
            ..built.clone()
        };

        let bytes = encode_stats(&built);
        assert_eq!(bytes, encode_stats(&imported), "the union is not stored");
        let want = format!("{union:?}");
        assert!(
            want.contains("count: 7"),
            "Person summed across graphs: {want}"
        );
        assert_eq!(format!("{:?}", decode_stats(&bytes).unwrap().classes), want);
        let (via_core, consumed) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(format!("{:?}", via_core.classes), want);
    }

    #[test]
    fn test_stats_determinism() {
        let stats = IndexStats {
            flakes: 100,
            size: 500,
            properties: Some(vec![
                PropertyStatEntry {
                    sid: (10, "zzz".to_string()),
                    count: 1,
                    ndv_values: 1,
                    ndv_subjects: 1,
                    last_modified_t: 1,
                    datatypes: vec![],
                    observed_datatypes: vec![],
                    historical_datatypes: vec![],
                },
                PropertyStatEntry {
                    sid: (10, "aaa".to_string()),
                    count: 2,
                    ndv_values: 2,
                    ndv_subjects: 2,
                    last_modified_t: 2,
                    datatypes: vec![],
                    observed_datatypes: vec![],
                    historical_datatypes: vec![],
                },
            ]),
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        };

        let bytes1 = encode_stats(&stats);
        let bytes2 = encode_stats(&stats);
        assert_eq!(bytes1, bytes2, "same inputs must produce identical bytes");
    }

    #[test]
    fn test_stats_with_len() {
        let stats = IndexStats {
            flakes: 42,
            size: 100,
            properties: None,
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 42,
                size: 100,
                properties: vec![],
                classes: None,
            }]),
            historical_since_t: None,
            links: None,
        };

        let bytes = encode_stats(&stats);
        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.flakes, 42);
    }

    /// `PropertyStatEntry::observed_datatypes` is not on the wire — every
    /// decoder re-derives it from the breakdown it just read (only the
    /// *historical* sets travel, in the tail). That makes a decoder that
    /// forgot to fill it invisible to any round-trip assertion about the
    /// *data*: the field would come back empty, which reads as "unknown",
    /// which silently declines the equijoin-filter fold instead of failing
    /// anything. So each decoder gets its own assertion, named after the
    /// entry point, and this is the only thing that would catch a miss.
    ///
    /// The other live decoder is `fluree-db-core`'s reader-only mirror of
    /// this format — the memory backend reaches that one where the binary
    /// path reaches this crate's (whose two entry points now share one
    /// implementation), so both are live and both re-derive.
    #[test]
    fn every_stats_decoder_rederives_the_observed_datatype_tags() {
        let stats = IndexStats {
            flakes: 3,
            size: 30,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "mixed".to_string()),
                count: 3,
                ndv_values: 3,
                ndv_subjects: 3,
                last_modified_t: 4,
                // A ref tag and a literal tag: the set the fold's guard reads.
                datatypes: vec![(1, 2), (7, 1)],
                observed_datatypes: vec![1, 7],
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        };
        let bytes = encode_stats(&stats);
        let expected = vec![1u8, 7];

        let via_decode_stats = decode_stats(&bytes).unwrap();
        assert_eq!(
            via_decode_stats.properties.as_ref().unwrap()[0].observed_datatypes,
            expected,
            "binary-index decode_stats did not re-derive observed_datatypes"
        );

        let (via_with_len, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(
            via_with_len.properties.as_ref().unwrap()[0].observed_datatypes,
            expected,
            "binary-index decode_stats_with_len did not re-derive observed_datatypes"
        );

        let (via_core, _) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(
            via_core.properties.as_ref().unwrap()[0].observed_datatypes,
            expected,
            "fluree-db-core's mirror decoder did not re-derive observed_datatypes"
        );
    }

    // ---- Historical tail tests ----

    /// A stats blob whose historical sets cannot all be re-derived from the
    /// counts: `mixed` remembers a tag (9) no current count mentions, and the
    /// graph-scoped entry likewise. If the tail were not genuinely on the
    /// wire, these would come back empty.
    fn stats_with_historical() -> IndexStats {
        IndexStats {
            flakes: 3,
            size: 30,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "mixed".to_string()),
                count: 3,
                ndv_values: 3,
                ndv_subjects: 3,
                last_modified_t: 4,
                datatypes: vec![(1, 2), (7, 1)],
                observed_datatypes: vec![1, 7],
                historical_datatypes: vec![1, 7, 9],
            }]),
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 3,
                size: 30,
                properties: vec![GraphPropertyStatEntry {
                    p_id: 42,
                    count: 3,
                    ndv_values: 3,
                    ndv_subjects: 3,
                    last_modified_t: 4,
                    datatypes: vec![(1, 2), (7, 1)],
                    observed_datatypes: vec![1, 7],
                    historical_datatypes: vec![1, 7, 9],
                }],
                classes: None,
            }]),
            historical_since_t: Some(2),
            links: None,
        }
    }

    /// The historical sets and their boundary genuinely round-trip through
    /// the wire — in every decoder — while `observed_datatypes` stays
    /// re-derived from the counts (tag 9 must NOT leak into it).
    #[test]
    fn historical_tail_round_trips_through_every_decoder() {
        let bytes = encode_stats(&stats_with_historical());

        let check = |decoded: &IndexStats, who: &str| {
            assert_eq!(decoded.historical_since_t, Some(2), "{who}: boundary lost");
            let agg = &decoded.properties.as_ref().unwrap()[0];
            assert_eq!(
                agg.historical_datatypes,
                vec![1, 7, 9],
                "{who}: aggregate historical set lost"
            );
            assert_eq!(
                agg.observed_datatypes,
                vec![1, 7],
                "{who}: observed set must stay re-derived from the counts"
            );
            let gp = &decoded.graphs.as_ref().unwrap()[0].properties[0];
            assert_eq!(
                gp.historical_datatypes,
                vec![1, 7, 9],
                "{who}: graph-scoped historical set lost"
            );
            assert_eq!(gp.observed_datatypes, vec![1, 7], "{who}: graph observed");
        };

        let (via_with_len, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len(), "tail bytes left unconsumed");
        check(&via_with_len, "decode_stats_with_len");

        let (via_core, core_consumed) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(core_consumed, bytes.len());
        check(&via_core, "fluree-db-core mirror");
    }

    /// Cross-version, old blob → new reader: a blob encoded WITHOUT the tail
    /// (`historical_since_t: None`) decodes with no boundary and empty
    /// historical sets — the conservative fallback every consumer fails
    /// closed on.
    #[test]
    fn a_blob_without_the_tail_decodes_conservatively() {
        let mut stats = stats_with_historical();
        stats.historical_since_t = None; // encoder writes no tail

        let bytes = encode_stats(&stats);
        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.historical_since_t, None);
        assert!(decoded.properties.as_ref().unwrap()[0]
            .historical_datatypes
            .is_empty());
        assert!(decoded.graphs.as_ref().unwrap()[0].properties[0]
            .historical_datatypes
            .is_empty());

        let (via_core, _) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(via_core.historical_since_t, None);
    }

    /// Cross-version, new blob → old reader: the tail is strictly appended,
    /// so a new blob's bytes are the old encoding plus a suffix. An old
    /// reader parses a prefix and its caller advances by the root's length
    /// prefix — meaning its parse of the new blob is byte-for-byte its parse
    /// of the old one. This pins the structural fact that makes that true.
    #[test]
    fn the_tail_is_a_strict_suffix_of_the_old_encoding() {
        let with_tail = encode_stats(&stats_with_historical());

        let mut without = stats_with_historical();
        without.historical_since_t = None;
        let old_encoding = encode_stats(&without);

        assert!(with_tail.len() > old_encoding.len());
        assert_eq!(
            &with_tail[..old_encoding.len()],
            &old_encoding[..],
            "the historical tail changed bytes an old reader parses"
        );
    }

    /// Link counts round-trip in both decoders beside the other tails, and a
    /// blob without them reads as unknown.
    #[test]
    fn link_tail_round_trips_after_the_other_tails() {
        let mut stats = stats_with_historical();
        stats.links = Some(vec![
            fluree_db_core::LinkStatEntry {
                sid: (15, "SEMNET_PART_OF".to_string()),
                count: 231_170,
            },
            fluree_db_core::LinkStatEntry {
                sid: (15, "SEMNET_CAUSES".to_string()),
                count: 0,
            },
        ]);
        let bytes = encode_stats(&stats);
        let expected = vec![
            fluree_db_core::LinkStatEntry {
                sid: (15, "SEMNET_CAUSES".to_string()),
                count: 0,
            },
            fluree_db_core::LinkStatEntry {
                sid: (15, "SEMNET_PART_OF".to_string()),
                count: 231_170,
            },
        ];
        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.links.as_ref(), Some(&expected));
        assert_eq!(decoded.historical_since_t, Some(2), "historical tail kept");
        let (via_core, _) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(via_core.links, Some(expected));

        let without = encode_stats(&stats_with_historical());
        assert_eq!(
            &bytes[..without.len()],
            &without[..],
            "appended, not interleaved"
        );
        let (decoded, _) = decode_stats_with_len(&without).unwrap();
        assert_eq!(decoded.links, None);
    }

    /// Forward evolution: a tail carrying an unknown future tag reads as
    /// absent (conservative), and the remainder is consumed so the section
    /// length still accounts for every byte.
    #[test]
    fn an_unknown_tail_tag_reads_as_absent() {
        let mut stats = stats_with_historical();
        stats.historical_since_t = None;
        let mut bytes = encode_stats(&stats);
        bytes.push(255); // future tail version
        bytes.extend_from_slice(&[1, 2, 3, 4]); // opaque future payload

        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.historical_since_t, None);

        let (via_core, core_consumed) = fluree_db_core::stats_wire::decode_stats(&bytes).unwrap();
        assert_eq!(core_consumed, bytes.len());
        assert_eq!(via_core.historical_since_t, None);
    }

    /// Empty historical sets are represented by absence on the wire, so a
    /// boundary can travel with no per-property entries at all.
    #[test]
    fn a_boundary_with_empty_sets_still_travels() {
        let mut stats = stats_with_historical();
        stats.properties.as_mut().unwrap()[0]
            .historical_datatypes
            .clear();
        stats.graphs.as_mut().unwrap()[0].properties[0]
            .historical_datatypes
            .clear();

        let bytes = encode_stats(&stats);
        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.historical_since_t, Some(2));
        assert!(decoded.properties.as_ref().unwrap()[0]
            .historical_datatypes
            .is_empty());
    }

    // ---- Schema tests ----

    #[test]
    fn test_schema_empty_round_trip() {
        let schema = IndexSchema::default();

        let bytes = encode_schema(&schema);
        let decoded = decode_schema(&bytes).unwrap();

        assert_eq!(decoded.t, 0);
        assert!(decoded.pred.vals.is_empty());
    }

    #[test]
    fn test_schema_with_entries_round_trip() {
        let schema = IndexSchema {
            t: 42,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: vec![
                    SchemaPredicateInfo {
                        id: sid(5, "Person"),
                        subclass_of: vec![sid(5, "Agent")],
                        parent_props: vec![],
                        child_props: vec![sid(5, "Employee")],
                    },
                    SchemaPredicateInfo {
                        id: sid(5, "Agent"),
                        subclass_of: vec![],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                ],
            },
        };

        let bytes = encode_schema(&schema);
        let decoded = decode_schema(&bytes).unwrap();

        assert_eq!(decoded.t, 42);
        assert_eq!(decoded.pred.vals.len(), 2);
        // Sorted by sid: Agent < Person
        assert_eq!(decoded.pred.vals[0].id, sid(5, "Agent"));
        assert_eq!(decoded.pred.vals[1].id, sid(5, "Person"));
        assert_eq!(decoded.pred.vals[1].subclass_of.len(), 1);
        assert_eq!(decoded.pred.vals[1].subclass_of[0], sid(5, "Agent"));
        assert_eq!(decoded.pred.vals[1].child_props.len(), 1);
        assert_eq!(decoded.pred.vals[1].child_props[0], sid(5, "Employee"));
    }

    #[test]
    fn test_schema_determinism() {
        let schema = IndexSchema {
            t: 10,
            pred: SchemaPredicates {
                keys: vec![],
                vals: vec![
                    SchemaPredicateInfo {
                        id: sid(5, "Z"),
                        subclass_of: vec![sid(5, "B"), sid(5, "A")],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                    SchemaPredicateInfo {
                        id: sid(5, "A"),
                        subclass_of: vec![],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                ],
            },
        };

        let bytes1 = encode_schema(&schema);
        let bytes2 = encode_schema(&schema);
        assert_eq!(bytes1, bytes2, "same inputs must produce identical bytes");

        // Also verify sorted order after decode
        let decoded = decode_schema(&bytes1).unwrap();
        assert_eq!(decoded.pred.vals[0].id, sid(5, "A"));
        assert_eq!(decoded.pred.vals[1].id, sid(5, "Z"));
        // subclass_of within Z should be sorted
        assert_eq!(decoded.pred.vals[1].subclass_of[0], sid(5, "A"));
        assert_eq!(decoded.pred.vals[1].subclass_of[1], sid(5, "B"));
    }

    #[test]
    fn test_schema_with_len() {
        let schema = IndexSchema {
            t: 5,
            pred: SchemaPredicates {
                keys: vec![],
                vals: vec![SchemaPredicateInfo {
                    id: sid(1, "test"),
                    subclass_of: vec![],
                    parent_props: vec![],
                    child_props: vec![],
                }],
            },
        };

        let bytes = encode_schema(&schema);
        let (decoded, consumed) = decode_schema_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.t, 5);
    }
}
