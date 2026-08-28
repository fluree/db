//! Binary wire format decoders for index stats and schema sections.
//!
//! These decode the binary stats/schema sections embedded in `IndexRoot`
//! (FIR6). The encode functions live in `fluree-db-indexer`.

use crate::index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
use crate::index_stats::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry,
    IndexStats, PropertyStatEntry,
};
use crate::sid::Sid;
use std::io;

// ---- Binary helpers ----

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

#[inline]
fn read_u8(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    ensure_len(data, *pos, 1, "u8")?;
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

#[inline]
fn read_u16(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    ensure_len(data, *pos, 2, "u16")?;
    let v = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

#[inline]
fn read_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    ensure_len(data, *pos, 4, "u32")?;
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

#[inline]
fn read_u64(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    ensure_len(data, *pos, 8, "u64")?;
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

#[inline]
fn read_i64(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    ensure_len(data, *pos, 8, "i64")?;
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

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

fn read_sid_tuple(data: &[u8], pos: usize) -> io::Result<((u16, String), usize)> {
    let mut p = pos;
    ensure_len(data, p, 4, "sid tuple header")?;
    let ns_code = u16::from_le_bytes(data[p..p + 2].try_into().unwrap());
    p += 2;
    let suffix_len = u16::from_le_bytes(data[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    ensure_len(data, p, suffix_len, "sid tuple suffix")?;
    let suffix = std::str::from_utf8(&data[p..p + suffix_len]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in sid tuple: {e}"),
        )
    })?;
    p += suffix_len;
    Ok(((ns_code, suffix.to_string()), p))
}

fn decode_datatypes(data: &[u8], pos: &mut usize) -> io::Result<Vec<(u8, u64)>> {
    let count = read_u8(data, pos)? as usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let dt_tag = read_u8(data, pos)?;
        let dt_count = read_u64(data, pos)?;
        result.push((dt_tag, dt_count));
    }
    Ok(result)
}

fn decode_graph_property(data: &[u8], pos: &mut usize) -> io::Result<GraphPropertyStatEntry> {
    let p_id = read_u32(data, pos)?;
    let count = read_u64(data, pos)?;
    let ndv_values = read_u64(data, pos)?;
    let ndv_subjects = read_u64(data, pos)?;
    let last_modified_t = read_i64(data, pos)?;
    let datatypes = decode_datatypes(data, pos)?;
    let observed_datatypes = PropertyStatEntry::tags_of(&datatypes);

    Ok(GraphPropertyStatEntry {
        p_id,
        count,
        ndv_values,
        ndv_subjects,
        last_modified_t,
        datatypes,
        observed_datatypes,
        historical_datatypes: Vec::new(),
    })
}

/// One graph's rows in the tail: `(g_id, [(p_id, tags)])`.
type GraphTagSets = Vec<(u16, Vec<(u32, Vec<u8>)>)>;

/// The historical tail section appended after the classes section.
///
/// This is the reader-only mirror of the format owned by
/// `fluree-db-binary-index/src/format/stats_wire.rs` — see the encoder there
/// for the wire layout and the evolution rules that make the tail safe for
/// readers on both sides of the change.
struct HistoricalTail {
    since_t: i64,
    agg: Vec<((u16, String), Vec<u8>)>,
    graphs: GraphTagSets,
}

/// Wire tag identifying the v1 historical tail.
const HISTORICAL_TAIL_TAG: u8 = 1;

fn read_tag_set(data: &[u8], pos: &mut usize) -> io::Result<Vec<u8>> {
    let n = read_u8(data, pos)? as usize;
    ensure_len(data, *pos, n, "historical tag set")?;
    let tags = data[*pos..*pos + n].to_vec();
    *pos += n;
    Ok(tags)
}

/// Decode the optional historical tail. `None` when the section is absent
/// (an old blob, exactly `pos == data.len()`) or carries an unknown future
/// tag — in which case the remainder is consumed, which is safe because the
/// root length-prefixes the whole stats section.
fn decode_historical_tail(data: &[u8], pos: &mut usize) -> io::Result<Option<HistoricalTail>> {
    if *pos >= data.len() {
        return Ok(None);
    }
    let tag = read_u8(data, pos)?;
    if tag != HISTORICAL_TAIL_TAG {
        *pos = data.len();
        return Ok(None);
    }
    let since_t = read_i64(data, pos)?;
    let agg_count = read_u32(data, pos)? as usize;
    let mut agg = Vec::with_capacity(agg_count);
    for _ in 0..agg_count {
        let (sid, new_pos) = read_sid_tuple(data, *pos)?;
        *pos = new_pos;
        let tags = read_tag_set(data, pos)?;
        agg.push((sid, tags));
    }
    let graph_count = read_u16(data, pos)? as usize;
    let mut graphs = Vec::with_capacity(graph_count);
    for _ in 0..graph_count {
        let g_id = read_u16(data, pos)?;
        let prop_count = read_u32(data, pos)? as usize;
        let mut props = Vec::with_capacity(prop_count);
        for _ in 0..prop_count {
            let p_id = read_u32(data, pos)?;
            let tags = read_tag_set(data, pos)?;
            props.push((p_id, tags));
        }
        graphs.push((g_id, props));
    }
    Ok(Some(HistoricalTail {
        since_t,
        agg,
        graphs,
    }))
}

/// Attach a decoded historical tail to the stats: set the boundary and fill
/// the per-entry `historical_datatypes` sets. Entries the tail does not name
/// keep their empty (unknown) set, which fails closed.
fn apply_historical_tail(stats: &mut IndexStats, tail: Option<HistoricalTail>) {
    let Some(tail) = tail else { return };
    stats.historical_since_t = Some(tail.since_t);
    if let Some(props) = stats.properties.as_mut() {
        let mut by_sid: std::collections::HashMap<(u16, String), Vec<u8>> =
            tail.agg.into_iter().collect();
        for entry in &mut *props {
            if let Some(tags) = by_sid.remove(&entry.sid) {
                entry.historical_datatypes = tags;
            }
        }
    }
    if let Some(graphs) = stats.graphs.as_mut() {
        let mut by_key: std::collections::HashMap<(u16, u32), Vec<u8>> = tail
            .graphs
            .into_iter()
            .flat_map(|(g_id, props)| {
                props
                    .into_iter()
                    .map(move |(p_id, tags)| ((g_id, p_id), tags))
            })
            .collect();
        for graph in &mut *graphs {
            for prop in &mut graph.properties {
                if let Some(tags) = by_key.remove(&(graph.g_id, prop.p_id)) {
                    prop.historical_datatypes = tags;
                }
            }
        }
    }
}

/// Decode the per-property payload within a class section: datatypes, langs, ref_classes.
fn decode_class_property_payload(
    data: &[u8],
    pos: &mut usize,
    property_sid: Sid,
) -> io::Result<ClassPropertyUsage> {
    // Datatypes
    let dt_count = read_u16(data, pos)? as usize;
    let mut datatypes = Vec::with_capacity(dt_count);
    for _ in 0..dt_count {
        let tag = read_u8(data, pos)?;
        let count = read_u64(data, pos)?;
        datatypes.push((tag, count));
    }

    // Langs
    let lang_count = read_u16(data, pos)? as usize;
    let mut langs = Vec::with_capacity(lang_count);
    for _ in 0..lang_count {
        let lang_len = read_u16(data, pos)? as usize;
        ensure_len(data, *pos, lang_len, "lang string")?;
        let lang = std::str::from_utf8(&data[*pos..*pos + lang_len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid UTF-8 in lang tag: {e}"),
            )
        })?;
        *pos += lang_len;
        let count = read_u64(data, pos)?;
        langs.push((lang.to_string(), count));
    }

    // Ref classes
    let rc_count = read_u16(data, pos)? as usize;
    let mut ref_classes = Vec::with_capacity(rc_count);
    for _ in 0..rc_count {
        let (ref_sid, new_pos) = read_sid(data, *pos)?;
        *pos = new_pos;
        let ref_count = read_u64(data, pos)?;
        ref_classes.push(ClassRefCount {
            class_sid: ref_sid,
            count: ref_count,
        });
    }

    Ok(ClassPropertyUsage {
        property_sid,
        datatypes,
        langs,
        ref_classes,
    })
}

// ---- Public decode functions ----

/// Decode `IndexStats` from the binary wire format.
///
/// Returns `(stats, bytes_consumed)`.
pub fn decode_stats(data: &[u8]) -> io::Result<(IndexStats, usize)> {
    let mut pos = 0usize;

    let flakes = read_u64(data, &mut pos)?;
    let size = read_u64(data, &mut pos)?;

    let graph_count = read_u16(data, &mut pos)? as usize;
    let mut graphs = Vec::with_capacity(graph_count);
    for _ in 0..graph_count {
        let g_id = read_u16(data, &mut pos)?;
        let g_flakes = read_u64(data, &mut pos)?;
        let g_size = read_u64(data, &mut pos)?;
        let prop_count = read_u32(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(prop_count);
        for _ in 0..prop_count {
            properties.push(decode_graph_property(data, &mut pos)?);
        }
        // Per-graph classes (optional section after properties).
        // Backward compat: if there are remaining bytes in the graph section,
        // read the has_classes flag. Otherwise default to None.
        let graph_classes = if pos < data.len() {
            let has_classes = read_u8(data, &mut pos)?;
            if has_classes != 0 {
                let gc_count = read_u32(data, &mut pos)? as usize;
                let mut gc = Vec::with_capacity(gc_count);
                for _ in 0..gc_count {
                    let (class_sid, new_pos) = read_sid(data, pos)?;
                    pos = new_pos;
                    let instance_count = read_u64(data, &mut pos)?;
                    let pu_count = read_u16(data, &mut pos)? as usize;
                    let mut properties = Vec::with_capacity(pu_count);
                    for _ in 0..pu_count {
                        let (property_sid, new_pos2) = read_sid(data, pos)?;
                        pos = new_pos2;
                        properties.push(decode_class_property_payload(
                            data,
                            &mut pos,
                            property_sid,
                        )?);
                    }
                    gc.push(ClassStatEntry {
                        class_sid,
                        count: instance_count,
                        properties,
                    });
                }
                if gc.is_empty() {
                    None
                } else {
                    Some(gc)
                }
            } else {
                None
            }
        } else {
            None
        };

        graphs.push(GraphStatsEntry {
            g_id,
            flakes: g_flakes,
            size: g_size,
            properties,
            classes: graph_classes,
        });
    }

    let agg_count = read_u32(data, &mut pos)? as usize;
    let mut agg_props = Vec::with_capacity(agg_count);
    for _ in 0..agg_count {
        let (sid, new_pos) = read_sid_tuple(data, pos)?;
        pos = new_pos;
        let count = read_u64(data, &mut pos)?;
        let ndv_values = read_u64(data, &mut pos)?;
        let ndv_subjects = read_u64(data, &mut pos)?;
        let last_modified_t = read_i64(data, &mut pos)?;
        let datatypes = decode_datatypes(data, &mut pos)?;
        let observed_datatypes = PropertyStatEntry::tags_of(&datatypes);
        agg_props.push(PropertyStatEntry {
            sid,
            count,
            ndv_values,
            ndv_subjects,
            last_modified_t,
            datatypes,
            observed_datatypes,
            historical_datatypes: Vec::new(),
        });
    }

    let class_count = read_u32(data, &mut pos)? as usize;
    let mut classes = Vec::with_capacity(class_count);
    for _ in 0..class_count {
        let (class_sid, new_pos) = read_sid(data, pos)?;
        pos = new_pos;
        let instance_count = read_u64(data, &mut pos)?;
        let pu_count = read_u16(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(pu_count);
        for _ in 0..pu_count {
            let (property_sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            properties.push(decode_class_property_payload(data, &mut pos, property_sid)?);
        }
        classes.push(ClassStatEntry {
            class_sid,
            count: instance_count,
            properties,
        });
    }

    let tail = decode_historical_tail(data, &mut pos)?;

    let mut stats = IndexStats {
        flakes,
        size,
        properties: if agg_props.is_empty() {
            None
        } else {
            Some(agg_props)
        },
        classes: if classes.is_empty() {
            None
        } else {
            Some(classes)
        },
        graphs: if graphs.is_empty() {
            None
        } else {
            Some(graphs)
        },
        historical_since_t: None,
    };
    apply_historical_tail(&mut stats, tail);

    Ok((stats, pos))
}

/// Decode `IndexSchema` from the binary wire format.
///
/// Returns `(schema, bytes_consumed)`.
pub fn decode_schema(data: &[u8]) -> io::Result<(IndexSchema, usize)> {
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
