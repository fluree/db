//! Binary wire format decoders for index stats and schema sections.
//!
//! These decode the binary stats/schema sections embedded in `IndexRoot`
//! (FIR6). The encode functions live in `fluree-db-binary-index`, except the
//! compact class tail ([`encode_class_tail`]), which lives here beside its
//! decoder.

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

/// Wire tag identifying the compact per-graph class tail.
const CLASS_TAIL_TAG: u8 = 2;

fn read_varint(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    crate::commit::codec::varint::decode_varint(data, pos)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("stats varint: {e}")))
}

/// A count read from the wire, with capacity capped by the bytes left so a
/// corrupt count cannot allocate past the blob.
fn read_count(data: &[u8], pos: &mut usize) -> io::Result<(usize, usize)> {
    let n = read_varint(data, pos)? as usize;
    Ok((n, n.min(data.len().saturating_sub(*pos))))
}

/// Append the per-graph class tables as one compact tail section.
///
/// Every Sid the tables name (classes, their properties, ref targets) is
/// written once, in a sorted table, front-coded against the previous name in
/// the same namespace; entries refer to it by index and counts are varints.
/// Readers that predate the section skip it (see the historical tail for why
/// an appended section is safe), so the encoder leaves the legacy per-graph
/// class slots empty: those readers see no class tables rather than
/// misparsing.
///
/// ```text
/// [tag: u8 = 2]
/// [sid_count: varint]
///   per Sid, sorted: [ns_code: u16 LE][shared: varint][rest_len: varint][rest]
/// [graph_count: varint]
///   per graph with classes, by g_id: [g_id: u16 LE][class_count: varint]
///   per class, by Sid: [index delta from the previous class: varint]
///                      [count: varint][property_count: varint]
///     per property, by Sid: [index: varint]
///       [n: varint] n × [tag: u8][count: varint]
///       [n: varint] n × [len: varint][lang bytes][count: varint]
///       [n: varint] n × [ref class index: varint][count: varint]
/// ```
pub fn encode_class_tail(buf: &mut Vec<u8>, graphs: &[&GraphStatsEntry]) {
    use crate::commit::codec::varint::encode_varint;
    use std::collections::{BTreeSet, HashMap};

    let with_classes: Vec<(u16, &[ClassStatEntry])> = graphs
        .iter()
        .filter_map(|g| g.classes.as_deref().map(|c| (g.g_id, c)))
        .collect();
    let mut sids: BTreeSet<&Sid> = BTreeSet::new();
    for (_, classes) in &with_classes {
        for class in *classes {
            sids.insert(&class.class_sid);
            for usage in &class.properties {
                sids.insert(&usage.property_sid);
                sids.extend(usage.ref_classes.iter().map(|r| &r.class_sid));
            }
        }
    }
    let index: HashMap<&Sid, u64> = sids
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, i as u64))
        .collect();

    buf.push(CLASS_TAIL_TAG);
    encode_varint(sids.len() as u64, buf);
    let mut prev: Option<&Sid> = None;
    for sid in &sids {
        let name = sid.name.as_bytes();
        let shared = match prev {
            Some(p) if p.namespace_code == sid.namespace_code => name
                .iter()
                .zip(p.name.as_bytes())
                .take_while(|(a, b)| a == b)
                .count(),
            _ => 0,
        };
        buf.extend_from_slice(&sid.namespace_code.to_le_bytes());
        encode_varint(shared as u64, buf);
        encode_varint((name.len() - shared) as u64, buf);
        buf.extend_from_slice(&name[shared..]);
        prev = Some(sid);
    }

    let mut with_classes = with_classes;
    with_classes.sort_by_key(|(g_id, _)| *g_id);
    encode_varint(with_classes.len() as u64, buf);
    for (g_id, classes) in with_classes {
        buf.extend_from_slice(&g_id.to_le_bytes());
        let mut classes: Vec<&ClassStatEntry> = classes.iter().collect();
        classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
        encode_varint(classes.len() as u64, buf);
        let mut prev_class = 0u64;
        for class in classes {
            let i = index[&class.class_sid];
            encode_varint(i - prev_class, buf);
            prev_class = i;
            encode_varint(class.count, buf);
            let mut usages: Vec<&ClassPropertyUsage> = class.properties.iter().collect();
            usages.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
            encode_varint(usages.len() as u64, buf);
            for usage in usages {
                encode_varint(index[&usage.property_sid], buf);
                let mut dts: Vec<&(u8, u64)> = usage.datatypes.iter().collect();
                dts.sort_by_key(|d| d.0);
                encode_varint(dts.len() as u64, buf);
                for &&(tag, count) in &dts {
                    buf.push(tag);
                    encode_varint(count, buf);
                }
                let mut langs: Vec<&(String, u64)> = usage.langs.iter().collect();
                langs.sort_by(|a, b| a.0.cmp(&b.0));
                encode_varint(langs.len() as u64, buf);
                for (lang, count) in langs {
                    encode_varint(lang.len() as u64, buf);
                    buf.extend_from_slice(lang.as_bytes());
                    encode_varint(*count, buf);
                }
                let mut refs: Vec<&ClassRefCount> = usage.ref_classes.iter().collect();
                refs.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                encode_varint(refs.len() as u64, buf);
                for r in refs {
                    encode_varint(index[&r.class_sid], buf);
                    encode_varint(r.count, buf);
                }
            }
        }
    }
}

/// Decode the class tail written by [`encode_class_tail`] (tag already
/// consumed): each graph's class table. Decoded Sids share their names.
fn decode_class_tail(data: &[u8], pos: &mut usize) -> io::Result<Vec<(u16, Vec<ClassStatEntry>)>> {
    let invalid =
        |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("class tail: {what}"));

    let (sid_count, cap) = read_count(data, pos)?;
    let mut sids: Vec<Sid> = Vec::with_capacity(cap);
    let mut prev: Vec<u8> = Vec::new();
    let mut prev_ns: Option<u16> = None;
    for _ in 0..sid_count {
        let ns_code = read_u16(data, pos)?;
        let shared = read_varint(data, pos)? as usize;
        let rest = read_varint(data, pos)? as usize;
        if shared > 0 && (prev_ns != Some(ns_code) || shared > prev.len()) {
            return Err(invalid("shared prefix out of range"));
        }
        ensure_len(data, *pos, rest, "class tail sid")?;
        prev.truncate(shared);
        prev.extend_from_slice(&data[*pos..*pos + rest]);
        *pos += rest;
        let name = std::str::from_utf8(&prev).map_err(|_| invalid("sid name is not UTF-8"))?;
        sids.push(Sid::new(ns_code, name));
        prev_ns = Some(ns_code);
    }
    let sid_at = |i: u64| -> io::Result<Sid> {
        sids.get(i as usize)
            .cloned()
            .ok_or_else(|| invalid("sid index out of range"))
    };

    let (graph_count, cap) = read_count(data, pos)?;
    let mut graphs = Vec::with_capacity(cap);
    for _ in 0..graph_count {
        let g_id = read_u16(data, pos)?;
        let (class_count, cap) = read_count(data, pos)?;
        let mut classes = Vec::with_capacity(cap);
        let mut class_index = 0u64;
        for _ in 0..class_count {
            class_index = class_index
                .checked_add(read_varint(data, pos)?)
                .ok_or_else(|| invalid("class index overflow"))?;
            let class_sid = sid_at(class_index)?;
            let count = read_varint(data, pos)?;
            let (usage_count, cap) = read_count(data, pos)?;
            let mut properties = Vec::with_capacity(cap);
            for _ in 0..usage_count {
                let property_sid = sid_at(read_varint(data, pos)?)?;
                let (n, cap) = read_count(data, pos)?;
                let mut datatypes = Vec::with_capacity(cap);
                for _ in 0..n {
                    let tag = read_u8(data, pos)?;
                    datatypes.push((tag, read_varint(data, pos)?));
                }
                let (n, cap) = read_count(data, pos)?;
                let mut langs = Vec::with_capacity(cap);
                for _ in 0..n {
                    let len = read_varint(data, pos)? as usize;
                    ensure_len(data, *pos, len, "class tail lang")?;
                    let lang = std::str::from_utf8(&data[*pos..*pos + len])
                        .map_err(|_| invalid("lang tag is not UTF-8"))?
                        .to_string();
                    *pos += len;
                    langs.push((lang, read_varint(data, pos)?));
                }
                let (n, cap) = read_count(data, pos)?;
                let mut ref_classes = Vec::with_capacity(cap);
                for _ in 0..n {
                    let class_sid = sid_at(read_varint(data, pos)?)?;
                    ref_classes.push(ClassRefCount {
                        class_sid,
                        count: read_varint(data, pos)?,
                    });
                }
                properties.push(ClassPropertyUsage {
                    property_sid,
                    datatypes,
                    langs,
                    ref_classes,
                });
            }
            classes.push(ClassStatEntry {
                class_sid,
                count,
                properties,
            });
        }
        graphs.push((g_id, classes));
    }
    Ok(graphs)
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

    // Appended sections, each led by its tag; an unknown tag ends the parse
    // (the root length-prefixes the stats section, so the rest is skipped).
    let mut tail = None;
    while pos < data.len() {
        match data[pos] {
            HISTORICAL_TAIL_TAG => tail = decode_historical_tail(data, &mut pos)?,
            CLASS_TAIL_TAG => {
                pos += 1;
                for (g_id, classes) in decode_class_tail(data, &mut pos)? {
                    let graph = graphs.iter_mut().find(|g| g.g_id == g_id).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("class tail names graph {g_id}, which the stats do not hold"),
                        )
                    })?;
                    graph.classes = (!classes.is_empty()).then_some(classes);
                }
            }
            _ => pos = data.len(),
        }
    }

    let mut stats = IndexStats {
        flakes,
        size,
        properties: if agg_props.is_empty() {
            None
        } else {
            Some(agg_props)
        },
        // Not stored when graphs carry classes; see the binary-index encoder.
        classes: if classes.is_empty() {
            crate::index_stats::union_per_graph_classes(&graphs)
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
