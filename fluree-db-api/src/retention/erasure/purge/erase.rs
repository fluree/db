//! Erasing from an index leaf by leaf: the erased events' history entries
//! are dropped from the leaves holding them, and every other artifact kept.

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{BinaryIndexStore, IndexRoot};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{ContentStore as _, Flake, FlakeValue, LedgerSnapshot, SubjectId};
use fluree_db_indexer::{
    erase_index_events, incremental_index_erasing, ErasedEvent, IndexErasure, IndexerConfig,
};
use fluree_db_nameservice::NsRecord;
use fluree_db_query::binary_scan::encode_stored_object;

use super::Captured;
use crate::{ApiError, Fluree, Result};

/// What erasing leaf by leaf did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Erased {
    /// The index holds none of the events.
    Nothing,
    /// The leaves holding them, and the strings only they used, were
    /// rewritten; a current row went as a retraction would take it, by an
    /// incremental build.
    Leafwise,
    /// A value lives in a store leaf by leaf erasure does not rewrite: a
    /// numeric, vector, geometry or full-text arena, a triple term, or the
    /// pooled tree.
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
        let _maintenance = self.hold_indexing(&record.ledger_id).await?;
        let erased = self.erase_from_held_index(record, captured, None).await?;
        if erased == Erased::Leafwise {
            self.announce_refresh(&record.ledger_id).await;
        }
        Ok(erased)
    }

    /// Hold `ledger_id`'s indexing, as a reindex does: a build begun from a
    /// root being erased would publish its events again over it. None
    /// without background indexing.
    pub(crate) async fn hold_indexing(
        &self,
        ledger_id: &fluree_db_core::LedgerId,
    ) -> Result<Vec<fluree_db_indexer::MaintenanceGuard>> {
        self.hold_indexers(ledger_id).await
    }

    /// [`Self::erase_from_index`], its indexing already held and its cached
    /// state left to the caller. `reading` reads its commits, when a store
    /// must substitute erasures not yet published: a build reads its
    /// unindexed commits.
    pub(crate) async fn erase_from_held_index(
        &self,
        record: &NsRecord,
        captured: &[Captured],
        reading: Option<std::sync::Arc<dyn fluree_db_core::ContentStore>>,
    ) -> Result<Erased> {
        // As the quiesced builds left it.
        let Some(record) = self.nameservice().lookup(&record.ledger_id).await? else {
            return Ok(Erased::Nothing);
        };
        let record = &record;
        let Some(root_id) = record.index_head_id.clone() else {
            return Ok(Erased::Nothing);
        };
        let store = match reading {
            Some(store) => store,
            None => {
                self.content_store_for_record_or_id(Some(record), &record.storage_namespace())
                    .await?
            }
        };
        let captured = &self
            .captured_on(store.as_ref(), record.commit_head_id.as_ref(), captured)
            .await?;
        if !captured.iter().any(|c| c.flake.t <= record.index_t) {
            return Ok(Erased::Nothing);
        }
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
        let mut erasure = IndexErasure::default();
        for c in captured.iter().filter(|c| c.flake.t <= record.index_t) {
            match indexed_record(&state.snapshot, index, &c.flake)? {
                Indexed::At(indexed)
                    if c.current_until.is_some_and(|until| record.index_t < until) =>
                {
                    erasure.current.push(indexed);
                }
                Indexed::At(indexed) => erasure.events.push(ErasedEvent {
                    record: indexed,
                    assert: c.flake.op,
                }),
                Indexed::Absent => {}
                // A value in an arena has no key to find it by.
                Indexed::Unknown => return Ok(Erased::NeedsRebuild),
            }
        }
        if erasure.events.is_empty() && erasure.current.is_empty() {
            return Ok(Erased::Nothing);
        }
        let root_bytes = store.get(&root_id).await?;
        let root = IndexRoot::decode(&root_bytes)
            .map_err(|e| ApiError::internal(format!("index root {root_id}: {e}")))?;
        let records = erasure
            .events
            .iter()
            .map(|e| &e.record)
            .chain(&erasure.current);
        if records.into_iter().any(|r| !leafwise(&root, r)) {
            return Ok(Erased::NeedsRebuild);
        }
        let built = match erasure.current.is_empty() {
            true => {
                erase_index_events(
                    store,
                    record.ledger_id.as_str(),
                    &root_id,
                    &erasure.events,
                    &IndexerConfig::default(),
                )
                .await?
            }
            // Its rows, and the statistics counting them, change as a
            // retraction would change them.
            false => {
                let config = self
                    .rebuild_config(&record.ledger_id, Some(&state), Default::default())
                    .await?;
                // As the indexer does when an incremental build fails, a
                // full one follows.
                match incremental_index_erasing(
                    store,
                    fluree_db_core::tracking::Tracker::disabled(),
                    record.ledger_id.as_str(),
                    record,
                    config,
                    &erasure,
                )
                .await
                {
                    Ok(built) => built,
                    Err(e) => {
                        tracing::warn!(ledger = %record.ledger_id, error = %e, "erasure: incremental erase failed, rebuilding");
                        return Ok(Erased::NeedsRebuild);
                    }
                }
            }
        };
        self.publisher()?
            .publish_index_allow_equal_fenced(
                record.ledger_id.as_str(),
                record.write_basis(),
                built.index_t,
                &built.root_id,
            )
            .await?;
        Ok(Erased::Leafwise)
    }
}

impl Fluree {
    /// Rebuild `record`'s index from its commits as `store` reads them, and
    /// publish it.
    pub(crate) async fn rebuild_index_from(
        &self,
        store: std::sync::Arc<dyn fluree_db_core::ContentStore>,
        record: &NsRecord,
    ) -> Result<()> {
        let state = self
            .load_unlisted_state(store.clone(), record.clone())
            .await
            .ok();
        let config = self
            .rebuild_config(&record.ledger_id, state.as_ref(), Default::default())
            .await?;
        let built = fluree_db_indexer::rebuild_index_from_commits_with_tracker(
            store,
            fluree_db_core::tracking::Tracker::disabled(),
            record.ledger_id.as_str(),
            record,
            config,
        )
        .await?;
        self.publisher()?
            .publish_index_allow_equal_fenced(
                record.ledger_id.as_str(),
                record.write_basis(),
                built.index_t,
                &built.root_id,
            )
            .await?;
        Ok(())
    }
}

/// Whether `event` lives only in leaves, their history and the string
/// dictionary, which leaf by leaf erasure rewrites.
fn leafwise(root: &IndexRoot, event: &RunRecordV2) -> bool {
    // The pooled tree holds many graphs, and an erasure's history drop is
    // per graph.
    if root.pool.as_ref().is_some_and(|p| p.contains(event.g_id)) {
        return false;
    }
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
        _ if o_type.is_string_keyed() => root.term_dict.is_none() && !full_text,
        _ => true,
    }
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
            .and_then(|iri| snapshot.graph_registry.assigned_graph_id(&iri))
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
    let o_i = flake.m.as_ref().map_or(
        fluree_db_core::list_index::NONE,
        fluree_db_core::FlakeMeta::packed_place,
    );
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
