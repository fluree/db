//! Erasing from an index leaf by leaf: the erased events' history entries
//! are dropped from the leaves holding them, and every other artifact kept.

use std::collections::BTreeSet;

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{BinaryIndexStore, IndexRoot};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{
    range, ContentStore as _, Flake, FlakeValue, IndexType, LedgerSnapshot, RangeMatch,
    RangeOptions, RangeTest, Sid, SubjectId,
};
use fluree_db_indexer::{erase_index_events, ErasedEvent, IndexerConfig};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::binary_scan::encode_stored_object;

use super::Captured;
use crate::{ApiError, Fluree, IndexingMode, Result};

/// What erasing leaf by leaf did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Erased {
    /// The index holds none of the events.
    Nothing,
    /// The leaves holding them, and the strings only they used, were
    /// rewritten.
    Leafwise,
    /// A value lives in a store leaf by leaf erasure does not rewrite: a
    /// numeric, vector, geometry or full-text arena, a triple term, or the
    /// subject dictionaries.
    NeedsRebuild,
}

impl Fluree {
    /// Rewrite `record`'s index without the indexed events of `captured`,
    /// rewriting only the leaves that hold them.
    pub(crate) async fn erase_from_index(
        &self,
        record: &NsRecord,
        captured: &[Captured],
    ) -> Result<Erased> {
        // Held against indexing, as a reindex is: a build begun from the
        // root being erased would publish its events again over this one.
        let _maintenance = match &self.indexing_mode {
            IndexingMode::Background(handle) => {
                Some(crate::admin::hold_branch_quiesced(handle, &record.ledger_id).await?)
            }
            IndexingMode::Disabled => None,
        };
        // As the quiesced builds left it.
        let Some(record) = self.nameservice().lookup(&record.ledger_id).await? else {
            return Ok(Erased::Nothing);
        };
        let record = &record;
        let Some(root_id) = record.index_head_id.clone() else {
            return Ok(Erased::Nothing);
        };
        let store = self
            .content_store_for_record_or_id(Some(record), &record.storage_namespace())
            .await?;
        let state = self
            .load_queryable_state_with_store(store.clone(), record.clone())
            .await?;
        let Some(index) = state
            .binary_store
            .as_ref()
            .and_then(|te| te.0.downcast_ref::<BinaryIndexStore>())
        else {
            return Err(ApiError::internal(format!(
                "{} has no binary index to erase from",
                record.ledger_id
            )));
        };
        let mut events = Vec::new();
        for c in captured.iter().filter(|c| c.flake.t <= record.index_t) {
            match indexed_record(&state.snapshot, index, &c.flake)? {
                Indexed::At(indexed) => events.push(ErasedEvent {
                    record: indexed,
                    assert: c.flake.op,
                }),
                Indexed::Absent => {}
                // A value in an arena has no key to find it by.
                Indexed::Unknown => return Ok(Erased::NeedsRebuild),
            }
        }
        if events.is_empty() {
            return Ok(Erased::Nothing);
        }
        if orphans_an_iri(&state.snapshot, captured, record.index_t).await? {
            return Ok(Erased::NeedsRebuild);
        }
        let root_bytes = store.get(&root_id).await?;
        let root = IndexRoot::decode(&root_bytes)
            .map_err(|e| ApiError::internal(format!("index root {root_id}: {e}")))?;
        if events.iter().any(|e| !leafwise(&root, &e.record)) {
            return Ok(Erased::NeedsRebuild);
        }
        let built = erase_index_events(
            store,
            record.ledger_id.as_str(),
            &root_id,
            &events,
            &IndexerConfig::default(),
        )
        .await?;
        self.publisher()?
            .publish_index_allow_equal_fenced(
                record.ledger_id.as_str(),
                record.fence,
                built.index_t,
                &built.root_id,
            )
            .await?;
        if let Some(manager) = &self.ledger_manager {
            manager.disconnect(&record.ledger_id).await;
        }
        Ok(Erased::Leafwise)
    }
}

/// Whether `event` lives only in leaves, their history and the string
/// dictionary, which leaf by leaf erasure rewrites.
fn leafwise(root: &IndexRoot, event: &RunRecordV2) -> bool {
    let o_type = OType::from_u16(event.o_type);
    let full_text = o_type == OType::FULLTEXT
        || root
            .graph_arenas
            .iter()
            .any(|ga| ga.g_id == event.g_id && ga.fulltext.iter().any(|f| f.p_id == event.p_id));
    match o_type.decode_kind() {
        DecodeKind::NumBigArena
        | DecodeKind::VectorArena
        | DecodeKind::SpatialArena
        | DecodeKind::TripleTermDict => false,
        // A triple term may name the string, and a full-text arena indexes it.
        DecodeKind::StringDict | DecodeKind::JsonArena => root.term_dict.is_none() && !full_text,
        _ => true,
    }
}

/// Whether an erased event names an IRI, as its subject or its object, that
/// no other indexed event uses: the subject dictionaries keep it.
async fn orphans_an_iri(
    snapshot: &LedgerSnapshot,
    captured: &[Captured],
    index_t: i64,
) -> Result<bool> {
    let erased: Vec<&Flake> = captured
        .iter()
        .map(|c| &c.flake)
        .filter(|f| f.t <= index_t)
        .collect();
    let mut iris: Vec<&Sid> = Vec::new();
    for flake in &erased {
        iris.push(&flake.s);
        if let FlakeValue::Ref(o) = &flake.o {
            iris.push(o);
        }
    }
    iris.sort();
    iris.dedup();
    let graphs: BTreeSet<u16> = std::iter::once(0)
        .chain(snapshot.graph_registry.iter_entries().map(|(g, _)| g))
        .collect();
    // An event of another graph taken for an erased one only sends the
    // holder to a rebuild.
    let same = |a: &Flake, b: &Flake| {
        (a.t, a.op) == (b.t, b.op) && a.s == b.s && a.p == b.p && a.o == b.o && a.dt == b.dt
    };
    for iri in iris {
        let mut used = false;
        'graphs: for &g_id in &graphs {
            let uses = [
                (IndexType::Spot, RangeMatch::subject(iri.clone())),
                (
                    IndexType::Opst,
                    RangeMatch::new().with_object(FlakeValue::Ref(iri.clone())),
                ),
            ];
            for (index, matching) in uses {
                let found = range(
                    snapshot,
                    g_id,
                    index,
                    RangeTest::Eq,
                    matching,
                    RangeOptions {
                        history_mode: true,
                        ..Default::default()
                    },
                )
                .await?;
                if found.iter().any(|f| !erased.iter().any(|e| same(e, f))) {
                    used = true;
                    break 'graphs;
                }
            }
        }
        if !used {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Where `flake` is in an index.
enum Indexed {
    At(RunRecordV2),
    /// A term of it is not in the index, which then holds no event of it.
    Absent,
    /// Its value has no key outside an arena: the index may hold it.
    Unknown,
}

/// `flake` in `index`'s ids.
fn indexed_record(
    snapshot: &LedgerSnapshot,
    index: &BinaryIndexStore,
    flake: &Flake,
) -> Result<Indexed> {
    let g_id = match &flake.g {
        None => 0,
        Some(g) => match snapshot
            .decode_sid(g)
            .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
        {
            Some(g_id) => g_id,
            None => return Ok(Indexed::Absent),
        },
    };
    let Some(s_id) = index
        .find_subject_id_by_parts(flake.s.namespace_code, &flake.s.name)
        .map_err(|e| ApiError::internal(format!("subject lookup: {e}")))?
    else {
        return Ok(Indexed::Absent);
    };
    let Some(p_id) = index.sid_to_p_id(&flake.p) else {
        return Ok(Indexed::Absent);
    };
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    let Ok((o_type, o_key)) =
        encode_stored_object(&flake.o, &flake.dt, lang, index, Some((g_id, p_id)))
    else {
        // A dictionary value the dictionaries do not hold is in no leaf; an
        // arena value has no key here at all.
        return Ok(
            if matches!(
                flake.o,
                FlakeValue::Vector(_)
                    | FlakeValue::BigInt(_)
                    | FlakeValue::Decimal(_)
                    | FlakeValue::GeoPoint(_)
                    | FlakeValue::TripleTerm(_)
            ) {
                Indexed::Unknown
            } else {
                Indexed::Absent
            },
        );
    };
    let o_i = flake
        .m
        .as_ref()
        .and_then(|m| m.i)
        .map_or(u32::MAX, |i| i as u32);
    Ok(Indexed::At(RunRecordV2::new(
        g_id,
        SubjectId(s_id),
        p_id,
        o_type,
        o_key,
        o_i,
        flake.t as u32,
    )))
}
