use crate::Novelty;
use async_trait::async_trait;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::index_stats::union_per_graph_classes;
use fluree_db_core::is_rdf_type;
use fluree_db_core::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphId, GraphPropertyStatEntry,
    GraphStatsEntry, IndexStats, LedgerSnapshot, OverlayProvider, PropertyStatEntry,
    RuntimePredicateId, RuntimeSmallDicts, Sid, ValueTypeTag,
};
use fluree_db_core::{Flake, FlakeValue};
use fluree_vocab::namespaces::FLUREE_COMMIT;
use std::collections::{HashMap, HashSet};

#[derive(Debug, thiserror::Error)]
pub enum StatsAssemblyError {
    #[error("{0}")]
    Message(String),
}

#[async_trait]
pub trait StatsLookup: Send + Sync {
    fn runtime_small_dicts(&self) -> Option<&RuntimeSmallDicts> {
        None
    }

    fn persisted_predicate_id_for_sid(&self, _sid: &Sid) -> Option<RuntimePredicateId> {
        None
    }

    fn runtime_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
        resolve_runtime_predicate_id(sid, self.runtime_small_dicts(), || {
            self.persisted_predicate_id_for_sid(sid)
        })
    }

    async fn lookup_subject_classes(
        &self,
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        g_id: GraphId,
        subjects: &[Sid],
    ) -> Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError>;
}

pub fn resolve_runtime_predicate_id(
    sid: &Sid,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    persisted_lookup: impl FnOnce() -> Option<RuntimePredicateId>,
) -> Option<RuntimePredicateId> {
    runtime_small_dicts
        .and_then(|dicts| dicts.predicate_id(sid))
        .or_else(persisted_lookup)
}

pub fn assemble_fast_stats(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
) -> IndexStats {
    assemble_fast_stats_inner(indexed, snapshot, novelty, to_t, lookup)
}

pub async fn assemble_full_stats(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty: &Novelty,
    to_t: i64,
    lookup: &dyn StatsLookup,
) -> Result<IndexStats, StatsAssemblyError> {
    let mut stats = assemble_fast_stats_inner(indexed, snapshot, novelty, to_t, Some(lookup));
    let mut touched_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut object_refs_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut subject_props: HashMap<(GraphId, Sid), HashMap<Sid, PropertyDelta>> = HashMap::new();

    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);
        if !include_in_runtime_stats(flake, to_t) {
            continue;
        }
        let g_id = graph_id_for_flake(snapshot, flake);
        if is_rdf_type(&flake.p) {
            touched_by_graph
                .entry(g_id)
                .or_default()
                .insert(flake.s.clone());
            if let FlakeValue::Ref(target_class) = &flake.o {
                touched_by_graph
                    .entry(g_id)
                    .or_default()
                    .insert(target_class.clone());
            }
            continue;
        }

        touched_by_graph
            .entry(g_id)
            .or_default()
            .insert(flake.s.clone());

        let entry = subject_props
            .entry((g_id, flake.s.clone()))
            .or_default()
            .entry(flake.p.clone())
            .or_default();
        entry.apply_flake(flake);

        if let FlakeValue::Ref(target) = &flake.o {
            object_refs_by_graph
                .entry(g_id)
                .or_default()
                .insert(target.clone());
        }
    }

    if subject_props.is_empty() {
        return Ok(stats);
    }

    let mut graph_subject_classes: HashMap<(GraphId, Sid), Vec<Sid>> = HashMap::new();
    for (g_id, subjects) in &touched_by_graph {
        let subject_vec: Vec<Sid> = subjects.iter().cloned().collect();
        let resolved = lookup
            .lookup_subject_classes(snapshot, overlay, to_t, *g_id, &subject_vec)
            .await?;
        for (subject, classes) in resolved {
            graph_subject_classes.insert((*g_id, subject), classes);
        }
    }

    for (g_id, objects) in &object_refs_by_graph {
        let object_vec: Vec<Sid> = objects.iter().cloned().collect();
        let resolved = lookup
            .lookup_subject_classes(snapshot, overlay, to_t, *g_id, &object_vec)
            .await?;
        for (subject, classes) in resolved {
            graph_subject_classes.insert((*g_id, subject), classes);
        }
    }

    let graphs = stats.graphs.get_or_insert_with(Vec::new);
    let mut graph_index: HashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();

    for ((g_id, subject), props) in subject_props {
        let Some(class_sids) = graph_subject_classes.get(&(g_id, subject.clone())) else {
            continue;
        };
        let graph_entry = get_or_insert_graph_entry(graphs, &mut graph_index, g_id);
        let classes = graph_entry.classes.get_or_insert_with(Vec::new);

        for class_sid in class_sids {
            let class_entry = get_or_insert_class_entry(classes, class_sid);
            for (property_sid, delta) in &props {
                let prop_usage = get_or_insert_class_property(class_entry, property_sid);
                merge_datatypes(&mut prop_usage.datatypes, &delta.datatypes);
                merge_langs(&mut prop_usage.langs, &delta.langs);

                for target in &delta.ref_targets {
                    if let Some(target_classes) = graph_subject_classes.get(&(g_id, target.clone()))
                    {
                        for target_class in target_classes {
                            increment_ref_class(&mut prop_usage.ref_classes, target_class, 1);
                        }
                    }
                }
            }
        }
    }

    graphs.sort_by_key(|entry| entry.g_id);
    for graph in graphs.iter_mut() {
        if let Some(classes) = &mut graph.classes {
            classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
            for class in classes.iter_mut() {
                class
                    .properties
                    .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
                for prop in &mut class.properties {
                    prop.datatypes.sort_by_key(|entry| entry.0);
                    prop.langs.sort_by(|a, b| a.0.cmp(&b.0));
                    prop.ref_classes
                        .sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                }
            }
        }
    }

    stats.classes = union_per_graph_classes(graphs);
    Ok(stats)
}

fn assemble_fast_stats_inner(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
) -> IndexStats {
    if novelty.is_empty() || to_t <= indexed_t(indexed, snapshot) {
        return indexed.clone();
    }

    let mut property_counts = build_property_counts(indexed);
    let mut class_data = build_class_data(indexed);
    let mut graphs = indexed.graphs.clone().unwrap_or_default();
    let mut graph_index: HashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();
    let mut flakes_delta: i64 = 0;
    let mut graph_subject_classes: HashMap<(GraphId, Sid), HashSet<Sid>> = HashMap::new();

    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);
        if flake.t > to_t {
            continue;
        }

        let delta = if flake.op { 1 } else { -1 };
        let g_id = graph_id_for_flake(snapshot, flake);
        flakes_delta += delta;

        let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
        graph_entry.flakes = ((graph_entry.flakes as i64) + delta).max(0) as u64;

        if !include_in_runtime_stats(flake, to_t) {
            continue;
        }

        if is_rdf_type(&flake.p) {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                let data = class_data.entry(class_sid.clone()).or_default();
                data.count_delta += delta;
                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let classes = graph_entry.classes.get_or_insert_with(Vec::new);
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                class_entry.count = ((class_entry.count as i64) + delta).max(0) as u64;

                let subject_classes = graph_subject_classes
                    .entry((g_id, flake.s.clone()))
                    .or_default();
                if flake.op {
                    subject_classes.insert(class_sid.clone());
                } else {
                    subject_classes.remove(class_sid);
                }
            }
            continue;
        }

        let sid_key = (flake.p.namespace_code, flake.p.name.to_string());
        *property_counts.entry(sid_key).or_insert(0) += delta;

        if let Some(stats_lookup) = lookup {
            if let Some(p_id) = stats_lookup.runtime_predicate_id_for_sid(&flake.p) {
                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let prop_entry = get_or_insert_graph_property(graph_entry, p_id);
                prop_entry.count = ((prop_entry.count as i64) + delta).max(0) as u64;
                prop_entry.last_modified_t = prop_entry.last_modified_t.max(flake.t);
                update_graph_property_datatypes(prop_entry, flake, delta);
            }
        }

        if let Some(class_sids) = graph_subject_classes.get(&(g_id, flake.s.clone())) {
            for class_sid in class_sids {
                let class = class_data.entry(class_sid.clone()).or_default();
                let prop = class.properties.entry(flake.p.clone()).or_default();
                prop.count_delta += delta;

                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let classes = graph_entry.classes.get_or_insert_with(Vec::new);
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                let prop_usage = get_or_insert_class_property(class_entry, &flake.p);
                update_class_property_usage(prop_usage, flake, delta, &graph_subject_classes, g_id);
            }
        }
    }

    let mut stats = finalize_stats(indexed, property_counts, class_data);
    stats.flakes = (indexed.flakes as i64 + flakes_delta).max(0) as u64;
    stats.size = indexed.size + novelty.size as u64;
    if !graphs.is_empty() {
        graphs.sort_by_key(|entry| entry.g_id);
        for graph in &mut graphs {
            graph.properties.sort_by_key(|entry| entry.p_id);
            if let Some(classes) = &mut graph.classes {
                classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                for class in classes.iter_mut() {
                    class
                        .properties
                        .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
                    for prop in &mut class.properties {
                        prop.datatypes.sort_by_key(|entry| entry.0);
                        prop.langs.sort_by(|a, b| a.0.cmp(&b.0));
                        prop.ref_classes
                            .sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                    }
                }
            }
        }
        stats.classes = union_per_graph_classes(&graphs);
        stats.graphs = Some(graphs);
    }
    stats
}

#[derive(Debug, Default)]
struct PropertyDelta {
    datatypes: Vec<(u8, u64)>,
    langs: Vec<(String, u64)>,
    ref_targets: Vec<Sid>,
}

impl PropertyDelta {
    fn apply_flake(&mut self, flake: &Flake) {
        let delta = if flake.op { 1 } else { -1 };
        increment_count(&mut self.datatypes, runtime_datatype_tag(flake), delta);
        if let Some(lang) = flake.m.as_ref().and_then(|meta| meta.lang.as_ref()) {
            increment_string_count(&mut self.langs, lang.clone(), delta);
        }
        if let FlakeValue::Ref(target) = &flake.o {
            if flake.op {
                self.ref_targets.push(target.clone());
            }
        }
    }
}

fn include_in_runtime_stats(flake: &Flake, to_t: i64) -> bool {
    if flake.t > to_t {
        return false;
    }
    if flake.s.namespace_code == FLUREE_COMMIT {
        return false;
    }
    if let Some(g) = &flake.g {
        let name = g.name.as_ref();
        if name.contains("txn-meta") {
            return false;
        }
    }
    true
}

fn graph_id_for_flake(snapshot: &LedgerSnapshot, flake: &Flake) -> GraphId {
    let Some(g_sid) = &flake.g else {
        return 0;
    };
    snapshot
        .decode_sid(g_sid)
        .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
        .unwrap_or(0)
}

fn indexed_t(indexed: &IndexStats, snapshot: &LedgerSnapshot) -> i64 {
    if indexed.graphs.is_some() || indexed.properties.is_some() || indexed.classes.is_some() {
        snapshot.t
    } else {
        0
    }
}

fn get_or_insert_graph_entry<'a>(
    graphs: &'a mut Vec<GraphStatsEntry>,
    graph_index: &mut HashMap<GraphId, usize>,
    g_id: GraphId,
) -> &'a mut GraphStatsEntry {
    if let Some(idx) = graph_index.get(&g_id).copied() {
        return &mut graphs[idx];
    }
    let idx = graphs.len();
    graphs.push(GraphStatsEntry {
        g_id,
        flakes: 0,
        size: 0,
        properties: Vec::new(),
        classes: Some(Vec::new()),
    });
    graph_index.insert(g_id, idx);
    &mut graphs[idx]
}

fn get_or_insert_graph_property(
    graph_entry: &mut GraphStatsEntry,
    p_id: RuntimePredicateId,
) -> &mut GraphPropertyStatEntry {
    if let Some(idx) = graph_entry
        .properties
        .iter()
        .position(|entry| entry.p_id == p_id.as_u32())
    {
        return &mut graph_entry.properties[idx];
    }
    graph_entry.properties.push(GraphPropertyStatEntry {
        p_id: p_id.as_u32(),
        count: 0,
        ndv_values: 0,
        ndv_subjects: 0,
        last_modified_t: 0,
        datatypes: Vec::new(),
    });
    graph_entry.properties.last_mut().expect("just inserted")
}

fn get_or_insert_class_entry<'a>(
    classes: &'a mut Vec<ClassStatEntry>,
    class_sid: &Sid,
) -> &'a mut ClassStatEntry {
    if let Some(idx) = classes
        .iter()
        .position(|entry| entry.class_sid == *class_sid)
    {
        return &mut classes[idx];
    }
    classes.push(ClassStatEntry {
        class_sid: class_sid.clone(),
        count: 0,
        properties: Vec::new(),
    });
    classes.last_mut().expect("just inserted")
}

fn get_or_insert_class_property<'a>(
    class_entry: &'a mut ClassStatEntry,
    property_sid: &Sid,
) -> &'a mut ClassPropertyUsage {
    if let Some(idx) = class_entry
        .properties
        .iter()
        .position(|entry| entry.property_sid == *property_sid)
    {
        return &mut class_entry.properties[idx];
    }
    class_entry.properties.push(ClassPropertyUsage {
        property_sid: property_sid.clone(),
        datatypes: Vec::new(),
        langs: Vec::new(),
        ref_classes: Vec::new(),
    });
    class_entry.properties.last_mut().expect("just inserted")
}

fn update_graph_property_datatypes(
    prop_entry: &mut GraphPropertyStatEntry,
    flake: &Flake,
    delta: i64,
) {
    increment_count(
        &mut prop_entry.datatypes,
        runtime_datatype_tag(flake),
        delta,
    );
}

fn update_class_property_usage(
    prop_usage: &mut ClassPropertyUsage,
    flake: &Flake,
    delta: i64,
    graph_subject_classes: &HashMap<(GraphId, Sid), HashSet<Sid>>,
    g_id: GraphId,
) {
    increment_count(
        &mut prop_usage.datatypes,
        runtime_datatype_tag(flake),
        delta,
    );
    if let Some(lang) = flake.m.as_ref().and_then(|meta| meta.lang.as_ref()) {
        increment_string_count(&mut prop_usage.langs, lang.clone(), delta);
    }
    if let FlakeValue::Ref(target_sid) = &flake.o {
        if let Some(target_classes) = graph_subject_classes.get(&(g_id, target_sid.clone())) {
            for target_class in target_classes {
                increment_ref_class(&mut prop_usage.ref_classes, target_class, delta);
            }
        }
    }
}

fn runtime_datatype_tag(flake: &Flake) -> u8 {
    if matches!(flake.o, FlakeValue::Ref(_)) {
        ValueTypeTag::JSON_LD_ID.as_u8()
    } else {
        ValueTypeTag::from_ns_name(flake.dt.namespace_code, &flake.dt.name).as_u8()
    }
}

fn increment_count(entries: &mut Vec<(u8, u64)>, tag: u8, delta: i64) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.0 == tag) {
        entry.1 = ((entry.1 as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push((tag, delta as u64));
    }
    entries.retain(|entry| entry.1 > 0);
}

fn increment_string_count(entries: &mut Vec<(String, u64)>, key: String, delta: i64) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.0 == key) {
        entry.1 = ((entry.1 as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push((key, delta as u64));
    }
    entries.retain(|entry| entry.1 > 0);
}

fn merge_datatypes(target: &mut Vec<(u8, u64)>, source: &[(u8, u64)]) {
    for (tag, count) in source {
        increment_count(target, *tag, *count as i64);
    }
}

fn merge_langs(target: &mut Vec<(String, u64)>, source: &[(String, u64)]) {
    for (lang, count) in source {
        increment_string_count(target, lang.clone(), *count as i64);
    }
}

fn increment_ref_class(entries: &mut Vec<ClassRefCount>, class_sid: &Sid, delta: i64) {
    if let Some(entry) = entries
        .iter_mut()
        .find(|entry| entry.class_sid == *class_sid)
    {
        entry.count = ((entry.count as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push(ClassRefCount {
            class_sid: class_sid.clone(),
            count: delta as u64,
        });
    }
    entries.retain(|entry| entry.count > 0);
}

type PropertyCountMap = HashMap<(u16, String), i64>;

fn build_property_counts(indexed: &IndexStats) -> PropertyCountMap {
    let mut counts = HashMap::new();
    if let Some(ref props) = indexed.properties {
        for entry in props {
            counts.insert(entry.sid.clone(), entry.count as i64);
        }
    }
    counts
}

#[derive(Debug, Default)]
struct ClassDataMut {
    count_delta: i64,
    properties: HashMap<Sid, PropertyDataMut>,
}

#[derive(Debug, Default)]
struct PropertyDataMut {
    count_delta: i64,
    ref_classes: Vec<ClassRefCount>,
}

fn build_class_data(indexed: &IndexStats) -> HashMap<Sid, ClassDataMut> {
    let mut class_data = HashMap::new();
    if let Some(ref classes) = indexed.classes {
        for entry in classes {
            let mut props = HashMap::new();
            for prop_usage in &entry.properties {
                props.insert(
                    prop_usage.property_sid.clone(),
                    PropertyDataMut {
                        count_delta: 1,
                        ref_classes: prop_usage.ref_classes.clone(),
                    },
                );
            }
            class_data.insert(
                entry.class_sid.clone(),
                ClassDataMut {
                    count_delta: entry.count as i64,
                    properties: props,
                },
            );
        }
    }
    class_data
}

fn finalize_stats(
    indexed: &IndexStats,
    property_counts: PropertyCountMap,
    class_data: HashMap<Sid, ClassDataMut>,
) -> IndexStats {
    let properties = if property_counts.is_empty() {
        indexed.properties.clone()
    } else {
        let indexed_props: HashMap<(u16, String), &PropertyStatEntry> = indexed
            .properties
            .as_ref()
            .map(|props| props.iter().map(|p| (p.sid.clone(), p)).collect())
            .unwrap_or_default();

        let mut entries: Vec<_> = property_counts.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let props: Vec<PropertyStatEntry> = entries
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(sid, count)| {
                let indexed_entry = indexed_props.get(&sid);
                PropertyStatEntry {
                    sid,
                    count: count.max(0) as u64,
                    ndv_values: indexed_entry.map(|e| e.ndv_values).unwrap_or(0),
                    ndv_subjects: indexed_entry.map(|e| e.ndv_subjects).unwrap_or(0),
                    last_modified_t: indexed_entry.map(|e| e.last_modified_t).unwrap_or(0),
                    datatypes: indexed_entry
                        .map(|e| e.datatypes.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();
        if props.is_empty() {
            None
        } else {
            Some(props)
        }
    };

    let classes = if class_data.is_empty() {
        indexed.classes.clone()
    } else {
        let mut entries: Vec<_> = class_data.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let class_entries: Vec<ClassStatEntry> = entries
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0)
            .map(|(class_sid, data)| {
                let mut prop_entries: Vec<_> = data.properties.into_iter().collect();
                prop_entries.sort_by(|a, b| a.0.cmp(&b.0));
                let properties: Vec<ClassPropertyUsage> = prop_entries
                    .into_iter()
                    .filter(|(_, prop)| prop.count_delta > 0)
                    .map(|(property_sid, prop)| ClassPropertyUsage {
                        property_sid,
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: prop.ref_classes,
                    })
                    .collect();
                ClassStatEntry {
                    class_sid,
                    count: data.count_delta.max(0) as u64,
                    properties,
                }
            })
            .collect();
        if class_entries.is_empty() {
            None
        } else {
            Some(class_entries)
        }
    };

    IndexStats {
        flakes: indexed.flakes,
        size: indexed.size,
        properties,
        classes,
        graphs: indexed.graphs.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Novelty;
    use fluree_db_core::{
        Flake, GraphStatsEntry, PropertyStatEntry, RuntimePredicateId, ValueTypeTag,
    };

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn prop_flake(subject: Sid, property: Sid, value: i64, t: i64) -> Flake {
        Flake::new(
            subject,
            property,
            FlakeValue::Long(value),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::INTEGER,
            ),
            t,
            true,
            None,
        )
    }

    fn type_flake(subject: Sid, class_sid: Sid, t: i64) -> Flake {
        Flake::new(
            subject,
            Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::predicates::RDF_TYPE,
            ),
            FlakeValue::Ref(class_sid),
            Sid::new(fluree_vocab::namespaces::JSON_LD, "@id"),
            t,
            true,
            None,
        )
    }

    struct StubLookup {
        p_ids: HashMap<Sid, RuntimePredicateId>,
        classes: HashMap<Sid, Vec<Sid>>,
    }

    #[async_trait]
    impl StatsLookup for StubLookup {
        fn runtime_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
            self.p_ids.get(sid).copied()
        }

        async fn lookup_subject_classes(
            &self,
            _snapshot: &LedgerSnapshot,
            _overlay: &dyn OverlayProvider,
            _to_t: i64,
            _g_id: GraphId,
            subjects: &[Sid],
        ) -> Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError> {
            Ok(subjects
                .iter()
                .filter_map(|subject| {
                    self.classes
                        .get(subject)
                        .cloned()
                        .map(|classes| (subject.clone(), classes))
                })
                .collect())
        }
    }

    #[test]
    fn fast_stats_updates_graph_datatypes() {
        let indexed = IndexStats {
            flakes: 1,
            size: 10,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "name".to_string()),
                count: 1,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 1,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 1,
                size: 10,
                properties: vec![],
                classes: Some(vec![]),
            }]),
        };
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(1);
        let subject = sid(10, "alice");
        let property = sid(10, "name");
        novelty
            .apply_commit(
                vec![
                    type_flake(subject.clone(), sid(10, "Person"), 2),
                    prop_flake(subject, property.clone(), 42, 2),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(property, RuntimePredicateId::from_u32(7))]),
            classes: HashMap::new(),
        };

        let stats = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, Some(&lookup));
        let graph_prop = stats
            .graphs
            .as_ref()
            .and_then(|graphs| graphs[0].properties.iter().find(|entry| entry.p_id == 7))
            .expect("graph property stats");
        assert_eq!(graph_prop.count, 1);
        assert_eq!(
            graph_prop.datatypes,
            vec![(ValueTypeTag::INTEGER.as_u8(), 1)]
        );
    }

    #[tokio::test]
    async fn full_stats_recovers_class_property_from_lookup() {
        let person = sid(10, "Person");
        let property = sid(10, "name");
        let subject = sid(10, "alice");
        let indexed = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: person.clone(),
                count: 1,
                properties: Vec::new(),
            }]),
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 0,
                size: 0,
                properties: vec![],
                classes: Some(vec![ClassStatEntry {
                    class_sid: person.clone(),
                    count: 1,
                    properties: Vec::new(),
                }]),
            }]),
        };
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![prop_flake(subject.clone(), property.clone(), 42, 2)],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(property.clone(), RuntimePredicateId::from_u32(7))]),
            classes: HashMap::from([(subject, vec![person.clone()])]),
        };

        let stats = assemble_full_stats(&indexed, &snapshot, &novelty, &novelty, 2, &lookup)
            .await
            .expect("full stats");
        let class_entry = stats
            .graphs
            .as_ref()
            .and_then(|graphs| graphs[0].classes.as_ref())
            .and_then(|classes| classes.iter().find(|entry| entry.class_sid == person))
            .expect("class entry");
        assert_eq!(class_entry.count, 1);
        assert!(
            class_entry
                .properties
                .iter()
                .any(|usage| usage.property_sid == property),
            "full lookup should recover property attribution from base class membership"
        );
    }
}
