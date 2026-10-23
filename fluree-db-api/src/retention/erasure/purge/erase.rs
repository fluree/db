//! Erasing from an index leaf by leaf: the erased events' history entries
//! are dropped from the leaves holding them, and every other artifact kept.

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{BinaryIndexStore, IndexRoot};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{ContentStore as _, Flake, LedgerSnapshot, SubjectId};
use fluree_db_indexer::{erase_index_events, ErasedEvent, IndexerConfig};
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
    /// rewritten.
    Leafwise,
    /// A value lives in a store leaf by leaf erasure does not rewrite: a
    /// numeric, vector, geometry or full-text arena, or a triple term.
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
            if let Some(indexed) = indexed_record(&state.snapshot, index, &c.flake)? {
                events.push(ErasedEvent {
                    record: indexed,
                    assert: c.flake.op,
                });
            }
        }
        if events.is_empty() {
            return Ok(Erased::Nothing);
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

/// `flake` in `index`'s ids; `None` when a term of it is not in the index,
/// which then holds no event of it.
fn indexed_record(
    snapshot: &LedgerSnapshot,
    index: &BinaryIndexStore,
    flake: &Flake,
) -> Result<Option<RunRecordV2>> {
    let g_id = match &flake.g {
        None => 0,
        Some(g) => match snapshot
            .decode_sid(g)
            .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
        {
            Some(g_id) => g_id,
            None => return Ok(None),
        },
    };
    let Some(s_id) = index
        .find_subject_id_by_parts(flake.s.namespace_code, &flake.s.name)
        .map_err(|e| ApiError::internal(format!("subject lookup: {e}")))?
    else {
        return Ok(None);
    };
    let Some(p_id) = index.sid_to_p_id(&flake.p) else {
        return Ok(None);
    };
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    // A value the dictionaries do not hold is in no leaf.
    let Ok((o_type, o_key)) =
        encode_stored_object(&flake.o, &flake.dt, lang, index, Some((g_id, p_id)))
    else {
        return Ok(None);
    };
    let o_i = flake
        .m
        .as_ref()
        .and_then(|m| m.i)
        .map_or(u32::MAX, |i| i as u32);
    Ok(Some(RunRecordV2::new(
        g_id,
        SubjectId(s_id),
        p_id,
        o_type,
        o_key,
        o_i,
        flake.t as u32,
    )))
}
