//! Where `fluree export` gets "which reifiers point at this edge".
//!
//! RDF 1.2 annotation syntax names the reifier at the base edge
//! (`s p o ~ <r>`), so serializing it needs the **forward** direction of the
//! edge-annotation index: `EdgeKey -> ann_sid`. That direction exists on disk
//! as the annotation arena's forward branch, and in memory as the attachment
//! overlay for anything committed since the last index build. This module
//! picks between them once per export and answers a whole `ColumnBatch` at a
//! time.
//!
//! ## Why not read the `f:reifies*` flakes the scan is already passing
//!
//! They are the durable encoding, and export sees every one of them. But they
//! are keyed by *reifier* subject, and the scan is in SPOT order: a reifier
//! whose IRI sorts after its base edge's subject arrives too late to influence
//! the line already written. Reconstructing the mapping from the scan alone
//! therefore means either buffering the whole graph or a second full pass.
//! The arena probe is `O(edges·log edges + covered leaves)` and needs neither.

use fluree_db_binary_index::annotation_arena::AnnotationArenaReader;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, EdgeKey, Sid};
use fluree_db_novelty::AttachmentNovelty;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex};

use crate::{ApiError, LedgerState, Result};

/// Forward annotation lookup for the duration of one export.
///
/// Constructed once by [`Self::for_ledger`]; borrowed by every per-graph
/// writer. Holds no state of its own beyond the two sources — the arena
/// reader, which caches branches and hot leaves, is built per probe call
/// because the writers each own their own scan.
/// Only `ExportBuilder` constructs one; the type is public because it appears
/// on the public `ExportConfig`. An external caller building an `ExportConfig`
/// by hand passes `annotations: None` and gets the pre-RDF-1.2 behaviour.
pub struct AnnotationProbe<'a> {
    /// Sealed on-disk arena: authoritative for `t <= its max_t`.
    arena: Option<(&'a AnnotationIndexRoot, &'a Arc<dyn ContentStore>)>,
    /// Attachment events committed since the last index build.
    novelty: Option<&'a AttachmentNovelty>,
    /// Bundles recovered by scanning the base index, for a ledger whose arena
    /// was never sealed. Resolved once at export start; empty otherwise.
    scanned: HashMap<EdgeKey, Vec<Sid>>,
    as_of_t: i64,
    /// Reifiers named by a `~ <r>` marker somewhere in this export.
    named: Mutex<HashSet<Sid>>,
    /// Reifiers whose `f:reifies*` bundle the scan passed — i.e. whose own
    /// subject is inside the exported selection, so their properties are in
    /// the file. Populated from the rows the writers suppress, which costs
    /// nothing extra: they are already being visited and discarded.
    in_scope: Mutex<HashSet<Sid>>,
}

impl<'a> AnnotationProbe<'a> {
    fn new(
        arena: Option<(&'a AnnotationIndexRoot, &'a Arc<dyn ContentStore>)>,
        novelty: Option<&'a AttachmentNovelty>,
        as_of_t: i64,
    ) -> Self {
        Self {
            arena,
            novelty,
            scanned: HashMap::new(),
            as_of_t,
            named: Mutex::new(HashSet::new()),
            in_scope: Mutex::new(HashSet::new()),
        }
    }

    /// Record that a `~ <r>` marker (or its per-format equivalent) was
    /// emitted for `reifier`.
    pub(crate) fn note_reifier_named(&self, reifier: &Sid) {
        if let Ok(mut named) = self.named.lock() {
            if !named.contains(reifier) {
                named.insert(reifier.clone());
            }
        }
    }

    /// Record that the scan passed `s_id`'s `f:reifies*` bundle, so that
    /// reifier's own triples are inside this export.
    ///
    /// Takes the raw subject id and resolves it only on first sight of each
    /// reifier — a bundle is up to seven rows, and resolving all of them
    /// would allocate seven times per annotation for one set entry.
    pub(crate) fn note_bundle_in_scope(&self, resolver: &dyn ReifierSubject, s_id: u64) {
        let Ok(sid) = resolver.reifier_sid(s_id) else {
            return;
        };
        self.note_bundle_sid(sid);
    }

    /// As [`Self::note_bundle_in_scope`], for callers that already hold the
    /// reifier's `Sid`.
    ///
    /// Untranslated overlay rows carry a fully-decoded subject, so they need
    /// no resolver round-trip. They still have to be *counted*: suppressing a
    /// bundle without noting it turns a visible leak into an annotation that
    /// vanishes with no marker, no bundle and no number.
    pub(crate) fn note_bundle_sid(&self, sid: Sid) {
        if let Ok(mut in_scope) = self.in_scope.lock() {
            in_scope.insert(sid);
        }
    }

    /// Reifiers named in the output whose own description is not in it.
    ///
    /// Read once, after every graph of an export has been written: a bundle
    /// can legitimately be emitted in a later graph than the marker, so the
    /// answer is only meaningful for the file as a whole.
    pub(crate) fn out_of_scope_count(&self) -> u64 {
        let (Ok(named), Ok(in_scope)) = (self.named.lock(), self.in_scope.lock()) else {
            return 0;
        };
        named.difference(&in_scope).count() as u64
    }

    /// Bundles the export suppressed whose reifier it never named.
    ///
    /// The complement of [`Self::out_of_scope_count`], and the one that means
    /// data loss: the `f:reifies*` rows were dropped from the output because
    /// annotation syntax was going to replace them, and then no marker was
    /// emitted.
    ///
    /// **This counter is a corruption guard, and is not expected to fire in
    /// normal operation.** That is a deliberate end state, not an oversight,
    /// so it is worth saying why rather than leaving the next reader to
    /// wonder whether it is dead code.
    ///
    /// It previously fired for annotations inside a named graph, because the
    /// base-index scan could not key them. That is fixed. Its remaining
    /// triggers are bundles [`EdgeKey::from_reifies_facts`] refuses to
    /// decode, and every one of those variants means a tampered or partial
    /// bundle. The most obvious, `GraphMismatch`, is **unconstructible from
    /// any legitimate write**: `EdgeKey::to_reifies_facts` emits the
    /// flake-level graph and the `f:reifiesGraph` object from the same
    /// `self.g`, so the two views this check compares are written from one
    /// value and cannot disagree. Every write surface also rejects
    /// hand-written `f:reifies*`.
    ///
    /// So there is deliberately no regression test driving this above zero:
    /// reaching it means writing a corrupt store, and a guard against
    /// corruption being unreachable through supported APIs is the guard
    /// working. What *is* tested is that it stays silent on every path that
    /// should resolve — which is the assertion that would break if the
    /// accounting were removed along with the defect.
    pub(crate) fn unresolved_count(&self) -> u64 {
        let (Ok(named), Ok(in_scope)) = (self.named.lock(), self.in_scope.lock()) else {
            return 0;
        };
        in_scope.difference(&named).count() as u64
    }

    /// Choose an annotation source for `ledger`, or establish that it has none.
    ///
    /// `Ok(None)` is the hard-guarantee case from the truth table in
    /// `fluree_db_core::annotation_index`: no `f:reifies*` flake has ever been
    /// observed, on either the indexed side or the overlay. The caller keeps
    /// its existing scan untouched, so a ledger without annotations pays one
    /// boolean read for all of this.
    ///
    /// `has_annotations = true` with no arena sealed is the state every
    /// `fluree index` pass leaves an annotated ledger in, and a bulk import
    /// before its auto-seal pass. There is no forward index to probe, so the
    /// bundles are recovered from the base index instead: a PSOT scan of the
    /// seven `f:reifies*` predicates, `O(annotations)` rather than
    /// `O(dataset)`, reusing the routine and the canonical
    /// `EdgeKey::from_reifies_facts` decode the arena's own seal pass uses.
    ///
    /// Emitting nothing there was never an option — it would turn #1859's
    /// loud defect into a quiet one, on the ledgers most likely to carry
    /// annotations. Refusing was, until it turned out that `fluree reindex`
    /// cannot clear the state (the sticky `had_annotation_arena` bit blocks
    /// the re-bootstrap), which would have made the refusal a dead end on
    /// ledgers every other reader serves through its own scan fallback.
    pub(crate) async fn for_ledger(ledger: &'a LedgerState, as_of_t: i64) -> Result<Option<Self>> {
        let snapshot = &ledger.snapshot;
        let attachments = &ledger.novelty.attachments;
        let novelty = attachments.has_annotations().then_some(attachments);

        if !snapshot.has_annotations {
            // Indexed side guarantees zero attachments. The overlay may still
            // hold some: a ledger that has never been indexed keeps its entire
            // history there, and so does one indexed before its first
            // annotation was written.
            return Ok(novelty.map(|novelty| Self::new(None, Some(novelty), as_of_t)));
        }

        // Kill switch: take the base-index scan even when an arena is
        // sealed. The two sources should agree; this is how you find out
        // when they do not, without rebuilding an index.
        if std::env::var("FLUREE_EXPORT_ANNOTATION_SCAN").is_ok() {
            let mut probe = Self::new(None, novelty, as_of_t);
            probe.scanned = scan_bundles(ledger, as_of_t).await;
            return Ok(Some(probe));
        }
        match (&snapshot.annotation_index, &snapshot.content_store) {
            (Some(root), Some(store)) => Ok(Some(Self::new(Some((root, store)), novelty, as_of_t))),
            (Some(_), None) => Err(ApiError::internal(
                "ledger has a sealed annotation arena but no content store to read it from",
            )),
            (None, _) => {
                let mut probe = Self::new(None, novelty, as_of_t);
                probe.scanned = scan_bundles(ledger, as_of_t).await;
                Ok(Some(probe))
            }
        }
    }

    /// Bundles the base-index scan recovered for `edge`.
    #[inline]
    fn scanned_for(&self, edge: &EdgeKey) -> Vec<Sid> {
        self.scanned.get(edge).cloned().unwrap_or_default()
    }

    /// Live reifiers for each edge, index-aligned with `edges`.
    ///
    /// Entry `i` is empty when `edges[i]` carries no annotation whose latest
    /// event at or before `as_of_t` is an assert.
    pub(crate) async fn live_reifiers(&self, edges: &[EdgeKey]) -> io::Result<Vec<Vec<Sid>>> {
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        match (self.arena, self.novelty) {
            (None, None) => Ok(edges.iter().map(|e| self.scanned_for(e)).collect()),

            // Sealed arena, nothing pending: one sorted merge-scan for the
            // whole batch. `current_annotations_batch` is arena-only by
            // contract, which is exactly what an empty overlay makes correct.
            (Some((root, store)), None) => {
                let reader = AnnotationArenaReader::new(root, store.as_ref());
                reader
                    .current_annotations_batch(edges, self.as_of_t)
                    .await
                    .map_err(|e| io::Error::other(format!("annotation arena probe: {e}")))
            }

            // Attachments committed since the arena was sealed. The batched
            // read cannot see a novelty *retract* of an indexed attachment, so
            // each edge merges its own event stream instead. Slower, and
            // confined to ledgers with pending annotation novelty.
            (Some((root, store)), Some(novelty)) => {
                let reader = AnnotationArenaReader::new(root, store.as_ref());
                let mut out = Vec::with_capacity(edges.len());
                for edge in edges {
                    let events = novelty.collect_forward_events(edge);
                    out.push(
                        reader
                            .current_annotations_merged(edge, &events, self.as_of_t)
                            .await
                            .map_err(|e| {
                                io::Error::other(format!("annotation arena merge: {e}"))
                            })?,
                    );
                }
                Ok(out)
            }

            // No arena: the overlay is the whole history. An in-memory
            // `BTreeMap` lookup per edge, no I/O.
            (None, Some(novelty)) => Ok(edges
                .iter()
                .map(|edge| {
                    let mut out = self.scanned_for(edge);
                    for ann in novelty.current_annotations_for_at(edge, self.as_of_t) {
                        if !out.contains(&ann) {
                            out.push(ann);
                        }
                    }
                    out
                })
                .collect()),
        }
    }
}

/// Recover `EdgeKey -> reifiers` from the base index for an unsealed ledger.
///
/// Best effort by design: the underlying scan reports its own failures as "no
/// coverage", and an export that emits no annotation marker is a better
/// outcome than one that fails outright on a ledger every other reader can
/// serve.
#[cfg(not(target_arch = "wasm32"))]
async fn scan_bundles(ledger: &LedgerState, as_of_t: i64) -> HashMap<EdgeKey, Vec<Sid>> {
    let events = crate::indexer_attachment_provider::scan_base_index_for_attachment_events_in(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        as_of_t,
        &ledger.snapshot.ledger_id,
    )
    .await;
    let mut out: HashMap<EdgeKey, Vec<Sid>> = HashMap::new();
    for (edge, ann, t, op) in events.unwrap_or_default() {
        // The scan already drops retracted rows; `t` still has to be honoured
        // so a time-travel export does not show an annotation written later.
        if !op || t > as_of_t {
            continue;
        }
        let slot = out.entry(edge).or_default();
        if !slot.contains(&ann) {
            slot.push(ann);
        }
    }
    out
}

#[cfg(target_arch = "wasm32")]
async fn scan_bundles(_ledger: &LedgerState, _as_of_t: i64) -> HashMap<EdgeKey, Vec<Sid>> {
    HashMap::new()
}

/// Resolves a subject id to the `Sid` a reifier was stored under.
///
/// A trait so this module does not have to know about the export writers'
/// resolver, which owns the store and dictionary-novelty handles.
pub(crate) trait ReifierSubject {
    fn reifier_sid(&self, s_id: u64) -> io::Result<Sid>;
}
