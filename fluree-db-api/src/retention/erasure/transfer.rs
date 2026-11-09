//! Erased history in transfers (`DATA_ERASURE.md`, "Transferring erased
//! history"). An erased commit never travels as bytes under its original
//! CID, which its replacement does not match: it travels as its erasure
//! chain, with its latest replacement, every step's receipt, and what
//! authorized each step. A receiver verifies the chain, checks each step
//! against its authorization, and publishes the chain in its own storage,
//! where the commit then reads as its replacement.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use fluree_db_core::commit::codec::{commit_blob_matches, read_commit_envelope};
use fluree_db_core::pack::{
    encode_authorization_frame, encode_erasure_artifact_frame, encode_erasure_frame,
    ErasureArtifactKind, PackFrame,
};
use fluree_db_core::{ContentId, ContentStore, StorageNamespace, TxnMetaValue};
use fluree_vocab::{db, namespaces::FLUREE_DB};
use serde::{Deserialize, Serialize};

use super::index::{Artifact, ErasedCommit, ErasureIndex, ErasureStep};
use super::purge::erase::Erased;
use super::purge::Captured;
use super::request::manifest_hash;
use crate::retention::registry;
use crate::retention::removal::OccurrenceRef;
use crate::{ApiError, Fluree, Result};

/// An erased commit as a transfer carries it: its chain, without the
/// addresses naming the sender's folders, and the chain's digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Chain {
    pub(crate) commit: String,
    pub(crate) ledger_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) txn: Option<String>,
    pub(crate) steps: Vec<ErasureStep>,
    /// [`ErasedCommit::digest`].
    pub(crate) digest: String,
}

impl Chain {
    fn of(erased: &ErasedCommit) -> Self {
        Chain {
            commit: erased.commit.clone(),
            ledger_id: erased.ledger_id.clone(),
            txn: erased.txn.clone(),
            steps: erased.steps.clone(),
            digest: erased.digest(),
        }
    }

    fn erased(&self, replaced_at: Vec<String>) -> ErasedCommit {
        ErasedCommit {
            commit: self.commit.clone(),
            ledger_id: self.ledger_id.clone(),
            txn: self.txn.clone(),
            steps: self.steps.clone(),
            replaced_at,
        }
    }
}

/// What authorized an erasure request: its control commit and the
/// occurrences it named, which a receiver checks each of its steps against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Authorization {
    pub(crate) request_id: String,
    /// The ledger the request was made on, whose history holds `control`.
    pub(crate) ledger_id: String,
    /// The control commit's id.
    pub(crate) control: String,
    /// The SHA-256 (hex) of the control commit's bytes, stored as an
    /// artifact.
    pub(crate) control_bytes: String,
    /// The occurrences the request named, in order: what the control
    /// commit's manifest hash covers.
    pub(crate) manifest: Vec<OccurrenceRef>,
    /// Retractions joined at publication, each ending the span of one of
    /// `manifest`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) added: Vec<OccurrenceRef>,
    /// How the request's proofs handle the original's signatures, as its
    /// control commit records it; absent, from an earlier release, is
    /// `verify`.
    #[serde(default)]
    pub(crate) signatures: super::SignatureMode,
}

impl Authorization {
    /// Why `bytes` are not this request's control commit, if they are not;
    /// its id and `t` if they are.
    fn check_control(&self, bytes: &[u8]) -> std::result::Result<(ContentId, i64), String> {
        let control: ContentId = self
            .control
            .parse()
            .map_err(|_| format!("its control commit id {} does not parse", self.control))?;
        if !commit_blob_matches(&control, bytes) {
            return Err(format!("its control commit does not match {control}"));
        }
        let envelope =
            read_commit_envelope(bytes).map_err(|e| format!("its control commit: {e}"))?;
        let recorded = |name: &str| {
            envelope.txn_meta.iter().find_map(|e| {
                match (
                    &e.value,
                    e.predicate_ns == FLUREE_DB && e.predicate_name == name,
                ) {
                    (TxnMetaValue::String(value), true) => Some(value.as_str()),
                    _ => None,
                }
            })
        };
        if recorded(db::ERASURE_REQUEST) != Some(self.request_id.as_str()) {
            return Err(format!("{control} does not record this request"));
        }
        if recorded(db::ERASURE_MANIFEST) != Some(manifest_hash(&self.manifest).as_str()) {
            return Err(format!("{control} records another manifest"));
        }
        let signatures =
            recorded(db::ERASURE_SIGNATURES).unwrap_or(super::SignatureMode::Verify.as_str());
        if signatures != self.signatures.as_str() {
            return Err(format!("{control} records another signature mode"));
        }
        Ok((control, envelope.t))
    }

    /// Whether it authorizes removing `slots` of `commit`.
    fn covers(&self, commit: &str, slots: &[u32]) -> bool {
        slots.iter().all(|slot| {
            self.manifest
                .iter()
                .chain(&self.added)
                .any(|o| o.commit == commit && o.slot == *slot)
        })
    }
}

/// A request id as erasure requests make them, safe to name a file by.
fn valid_request_id(id: &str) -> bool {
    id.strip_prefix("erasure-")
        .is_some_and(|hex| !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn bad(message: String) -> ApiError {
    ApiError::http(400, message)
}

fn frame(encode: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut buf = Vec::new();
    encode(&mut buf);
    buf
}

/// The erasures of the history a stream sends, and what it has sent.
pub(crate) struct Sender {
    index: Option<Arc<ErasureIndex>>,
    /// Erased commits among those sent, each sent at its place.
    erased: HashMap<ContentId, ErasedCommit>,
    /// Erased commits the receiver has already, whose chains it lacks: sent
    /// before the commits, so it erases them too.
    held: Vec<ErasedCommit>,
    /// The history holds an erased commit, sent or not.
    history_erased: bool,
    authorizations: HashSet<String>,
    artifacts: HashSet<String>,
}

impl Sender {
    /// The erasures of the history `want` names in `store`, for a receiver
    /// lacking `missing` of it (the history a pack sends: all of it when
    /// `have` is empty) and holding the chains `held` (digests).
    pub(crate) async fn of(
        fluree: &Fluree,
        ledger_id: &str,
        store: &dyn ContentStore,
        want: &[ContentId],
        missing: &[ContentId],
        held: &[String],
    ) -> Result<Self> {
        let index = fluree.retention.erasure.get().map(|r| r.index.clone());
        let mut sender = Sender::new(index.clone());
        let Some(index) = index else {
            return Ok(sender);
        };
        if want.is_empty() || !index.any_erased().await? {
            return Ok(sender);
        }
        let missing: HashSet<&ContentId> = missing.iter().collect();
        let mut seen = HashSet::new();
        for head in want {
            for (cid, erased) in fluree.erased_reached(ledger_id, store, head).await? {
                if !seen.insert(cid.clone()) {
                    continue;
                }
                sender.history_erased = true;
                if missing.contains(&cid) {
                    sender.erased.insert(cid, erased);
                } else if !held.contains(&erased.digest()) {
                    sender.held.push(erased);
                }
            }
        }
        Ok(sender)
    }

    fn new(index: Option<Arc<ErasureIndex>>) -> Self {
        Sender {
            index,
            erased: HashMap::new(),
            held: Vec::new(),
            history_erased: false,
            authorizations: HashSet::new(),
            artifacts: HashSet::new(),
        }
    }

    /// Whether the history holds an erased commit, sent or not.
    pub(crate) fn history_erased(&self) -> bool {
        self.history_erased
    }

    /// Erased commits among those sent, at their places.
    pub(crate) fn positions(&self) -> usize {
        self.erased.len()
    }

    /// Erasure chains sent.
    pub(crate) fn count(&self) -> usize {
        self.erased.len() + self.held.len()
    }

    /// Artifacts sent so far.
    pub(crate) fn artifacts_sent(&self) -> usize {
        self.artifacts.len()
    }

    /// Refuse a stream whose receiver does not take erased history: its
    /// commits, or those it may hold already, which it would go on serving.
    pub(crate) fn refuse(&self) -> Result<()> {
        if !self.history_erased {
            return Ok(());
        }
        let first = self
            .erased
            .keys()
            .map(ToString::to_string)
            .chain(self.held.iter().map(|e| e.commit.clone()))
            .min()
            .unwrap_or_default();
        Err(ApiError::http(
            501,
            format!(
                "commit {first} is permanently erased; this transfer does not carry erased \
                 commits, which a client must ask for"
            ),
        ))
    }

    /// The frames of the chains the receiver lacks for commits it already
    /// has, to send before the commits.
    pub(crate) async fn held_frames(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        for erased in self.held.clone() {
            frames.extend(self.chain_frames(&erased).await?);
        }
        Ok(frames)
    }

    /// The frames carrying `commit` when it is erased: the authorizations
    /// not sent yet, its latest replacement and receipts, then its chain.
    pub(crate) async fn frames(&mut self, commit: &ContentId) -> Result<Option<Vec<Vec<u8>>>> {
        match self.erased.get(commit).cloned() {
            Some(erased) => self.chain_frames(&erased).await.map(Some),
            None => Ok(None),
        }
    }

    /// The frames carrying `erased`: the authorizations not sent yet, its
    /// latest replacement and receipts, then its chain.
    async fn chain_frames(&mut self, erased: &ErasedCommit) -> Result<Vec<Vec<u8>>> {
        let index = self
            .index
            .clone()
            .ok_or_else(|| ApiError::internal("no erasure index"))?;
        let commit = &erased.commit;
        let mut frames = Vec::new();
        for step in &erased.steps {
            if self.authorizations.contains(&step.request_id) {
                continue;
            }
            let authorization = index
                .authorization(&step.request_id)
                .await?
                .ok_or_else(|| {
                    ApiError::internal(format!(
                        "erasure request {} has no stored authorization to send",
                        step.request_id
                    ))
                })?;
            let control = index
                .artifact(Artifact::Control, &authorization.control_bytes)
                .await?;
            if self.artifacts.insert(authorization.control_bytes.clone()) {
                frames.push(frame(|b| {
                    encode_erasure_artifact_frame(ErasureArtifactKind::Control, &control, b);
                }));
            }
            let json = serde_json::to_value(&authorization)
                .map_err(|e| ApiError::internal(e.to_string()))?;
            frames.push(frame(|b| encode_authorization_frame(&json, b)));
            self.authorizations.insert(step.request_id.clone());
        }
        let latest = erased
            .steps
            .last()
            .ok_or_else(|| ApiError::internal(format!("erasure of {commit} has no steps")))?;
        let replacement: ContentId = latest
            .replacement
            .parse()
            .map_err(|_| ApiError::internal("erasure names an unparsable replacement"))?;
        let bytes = index.read_replacement(&replacement).await?;
        if self.artifacts.insert(replacement.address_digest()) {
            frames.push(frame(|b| {
                encode_erasure_artifact_frame(ErasureArtifactKind::Replacement, &bytes, b);
            }));
        }
        for step in &erased.steps {
            let receipt = index.artifact(Artifact::Proof, &step.proof).await?;
            if self.artifacts.insert(step.proof.clone()) {
                frames.push(frame(|b| {
                    encode_erasure_artifact_frame(ErasureArtifactKind::Receipt, &receipt, b);
                }));
            }
        }
        let json = serde_json::to_value(Chain::of(erased))
            .map_err(|e| ApiError::internal(e.to_string()))?;
        frames.push(frame(|b| encode_erasure_frame(&json, b)));
        Ok(frames)
    }
}

/// What a pack brings besides the objects it stores: the erasures of its
/// history, staged as their frames arrive (see [`Fluree::receive_pack_frame`]),
/// and its source's removal journal.
#[derive(Default)]
pub struct PackReception {
    received: Received,
    journal: Option<crate::retention::registry::ArchivedRetention>,
}

impl PackReception {
    /// Erased commits received.
    pub fn erased_commits(&self) -> usize {
        self.received.count()
    }
}

/// What [`Fluree::accept_pack`] did.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcceptedPack {
    /// Erased commits received.
    pub erased_commits: usize,
    /// Branches here, the receiving ledger's among them, made to read as the
    /// erasures do.
    pub reconciled: Vec<String>,
}

impl Fluree {
    /// Take a pack frame that is not a stored object: an erasure's, staged,
    /// or a `phase: "retention"` manifest. Other frames are ignored.
    pub async fn receive_pack_frame(
        &self,
        reception: &mut PackReception,
        frame: PackFrame,
    ) -> Result<()> {
        match frame {
            PackFrame::Manifest(json) if json["phase"] == "retention" => {
                reception.journal =
                    Some(crate::retention::registry::ArchivedRetention::parse(&json)?);
                Ok(())
            }
            frame @ (PackFrame::Erasure(_)
            | PackFrame::Authorization(_)
            | PackFrame::ErasureArtifact { .. }) => {
                self.take_erasure_frame(frame, &mut reception.received)
                    .await
            }
            _ => Ok(()),
        }
    }

    /// The digests of the erasure chains this storage holds, for a pack
    /// request: the source sends the erased commits of a history whose
    /// chains are not among them.
    pub async fn held_erasure_digests(&self) -> Result<Vec<String>> {
        match self.retention.erasure.get() {
            Some(runtime) => runtime.index.digests().await,
            None => Ok(Vec::new()),
        }
    }

    /// Ready `ledger_id` for `head`, a head a pack brought it, before it
    /// moves there: take the pack's erasures as a restore does, every ledger
    /// here holding an original they erase (`ledger_id` among them, for a
    /// pull) made to read as they do, and merge the source's journal into
    /// `ledger_id`'s. `erasure_authority`: the source is designated one;
    /// erasures from any other are refused.
    pub async fn accept_pack(
        &self,
        ledger_id: &str,
        reception: PackReception,
        head: &ContentId,
        erasure_authority: bool,
    ) -> Result<AcceptedPack> {
        let ledger_id = fluree_db_core::LedgerId::parse(ledger_id)?;
        let record = self
            .nameservice()
            .lookup(&ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        let PackReception { received, journal } = reception;
        let mut accepted = AcceptedPack {
            erased_commits: received.count(),
            ..Default::default()
        };
        if accepted.erased_commits > 0 {
            if !erasure_authority {
                return Err(ApiError::http(
                    403,
                    "the pack holds permanently erased history, and its source is not \
                     designated an erasure authority here",
                ));
            }
            let runtime = match self.retention.erasure.get() {
                Some(runtime) if self.retention.replicas.get().is_none() => runtime,
                _ => return Err(crate::commit_transfer::cannot_hold_erasures()),
            };
            // An erasure accepted or published meanwhile would miss what
            // this one changes.
            let _admission = runtime.admission.write().await;
            let history = self
                .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                .await?;
            let received = self
                .receive_erasures(
                    received,
                    Receiver::Live(ledger_id.as_str()),
                    &record.storage_namespace(),
                    history.as_ref(),
                    head,
                    journal.as_ref().map(|j| &j.journal),
                )
                .await?;
            accepted.reconciled = received.reconciled;
        }
        if let Some(journal) = journal {
            registry::restore(
                self.publisher()?,
                self.registry_objects(),
                &record,
                Some(journal),
                None,
                None,
            )
            .await?;
            self.checkpoint_journal(ledger_id.as_str()).await?;
            self.retention.mark_active(ledger_id.as_str());
        }
        Ok(accepted)
    }
}

impl Fluree {
    /// Take what a push to `ledger_id` carries of erased history and
    /// retention, before its commits are validated and before the ledger's
    /// write lock, which reconciling the ledger takes. The erasures go
    /// through a pull's receive path, with the ledger a holder of the
    /// originals it has; erasures need `authority` to erase. The journal's
    /// removals are merged into the ledger's; one it lacks needs that
    /// authority too. The chains of erased commits the ledger has that the
    /// push lists but neither carries nor this storage holds are asked for,
    /// with a `409` (see [`crate::ERASURES_NEEDED`]); one held that differs
    /// from the sender's is refused. A push of no commits brings these
    /// alone, for the history the ledger has.
    ///
    /// Returns the erased commits the push brings, whose empty bytes in
    /// `request` are now their replacements.
    pub(crate) async fn receive_pushed_erasures(
        &self,
        ledger_id: &fluree_db_core::LedgerId,
        request: &mut crate::PushCommitsRequest,
        authority: crate::PushAuthority,
    ) -> Result<HashSet<ContentId>> {
        let positions = erased_positions(request)?;
        let resent = self.originals_erased_here(request).await?;
        // History rules come from config commits, which the receiver
        // derives itself; only a removal request makes a removal.
        let journal = request
            .retention
            .take()
            .map(|section| crate::retention::registry::ArchivedRetention::parse(&section))
            .transpose()?
            .map(|mut section| {
                section.journal.history_rules.clear();
                section
            })
            .filter(|section| !section.journal.is_empty());
        let erasing = !request.erasure_frames.is_empty() || !positions.is_empty();
        if !erasing && request.base_erasures.is_empty() && journal.is_none() && resent.is_empty() {
            return Ok(HashSet::new());
        }
        let runtime = match self.retention.erasure.get() {
            Some(runtime) if self.retention.replicas.get().is_none() => Some(runtime),
            _ if erasing => return Err(crate::commit_transfer::cannot_hold_erasures()),
            _ => None,
        };
        if erasing && !authority.erasures {
            return Err(ApiError::http(
                403,
                "this push carries permanently erased commits; pushing erasures needs the \
                 authority erasing needs",
            ));
        }
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;

        let mut received = Received::default();
        for bytes in std::mem::take(&mut request.erasure_frames) {
            let (frame, _) = fluree_db_core::pack::decode_frame(
                &bytes.0,
                fluree_db_core::pack::DEFAULT_MAX_PAYLOAD,
            )
            .map_err(|e| bad(format!("erasure frame: {e}")))?;
            if !matches!(
                frame,
                PackFrame::Erasure(_)
                    | PackFrame::Authorization(_)
                    | PackFrame::ErasureArtifact { .. }
            ) {
                return Err(bad("a push's erasure frames carry only erasures".into()));
            }
            self.take_erasure_frame(frame, &mut received).await?;
        }
        if let Some(runtime) = runtime {
            let mut needed = Vec::new();
            for base in &request.base_erasures {
                let held = runtime.index.erased(&base.commit).await?;
                // Held this far, it must be the chain the sender holds.
                if held.as_ref().is_some_and(|held| {
                    held.steps.len() >= base.steps && held.digest_of(base.steps) != base.digest
                }) {
                    return Err(ApiError::http(
                        409,
                        format!(
                            "the erasure of commit {} differs from the one this storage holds",
                            base.commit
                        ),
                    ));
                }
                let steps = held
                    .map(|e| e.steps.len())
                    .max(
                        received
                            .chains
                            .get(&base.commit.to_string())
                            .map(|c| c.steps.len()),
                    )
                    .unwrap_or(0);
                if steps < base.steps {
                    needed.push(base.commit.to_string());
                }
            }
            if !needed.is_empty() {
                let listed = serde_json::to_string(&needed)
                    .map_err(|e| ApiError::internal(e.to_string()))?;
                return Err(ApiError::http(
                    409,
                    format!("{}{listed}", crate::ERASURES_NEEDED),
                ));
            }
        }
        if let Some(journal) = &journal {
            let current = registry::read(
                self.publisher()?,
                self.registry_objects(),
                ledger_id.as_str(),
            )
            .await?
            .1
            .map(|r| r.journal)
            .unwrap_or_default();
            let adds = journal.journal.removals.iter().any(|r| {
                !current
                    .removals
                    .iter()
                    .any(|held| held.commit == r.commit && held.slot == r.slot)
            });
            if adds && !authority.erasures {
                return Err(ApiError::http(
                    403,
                    "this push carries removals this ledger's journal lacks; pushing them \
                     needs the authority removing needs",
                ));
            }
        }

        if received.count() > 0 {
            let runtime = runtime.expect("erasing needs the runtime");
            // An erasure accepted or published meanwhile would miss what
            // this one changes.
            let _admission = runtime.admission.write().await;
            let head = match request.commits.is_empty() {
                // A push of no commits: what the receiver has.
                true => record
                    .commit_head_id
                    .clone()
                    .ok_or_else(|| bad("erasures pushed to a ledger without commits".into()))?,
                false => pushed_head(request)?,
            };
            let history = Substituted {
                inner: self.branched_content_store(ledger_id.as_str()).await?,
                replacements: Arc::new(pushed_bytes(request)),
            };
            self.receive_erasures(
                received,
                Receiver::Live(ledger_id.as_str()),
                &record.storage_namespace(),
                &history,
                &head,
                journal.as_ref().map(|j| &j.journal),
            )
            .await?;
        }
        if let Some(journal) = journal {
            registry::restore(
                self.publisher()?,
                self.registry_objects(),
                &record,
                Some(journal),
                None,
                None,
            )
            .await?;
            self.checkpoint_journal(ledger_id.as_str()).await?;
            self.retention.mark_active(ledger_id.as_str());
        }

        let mut erased = HashSet::new();
        for (at, cid) in positions.into_iter().chain(resent) {
            let chain = match runtime {
                Some(runtime) => runtime.index.erased(&cid).await?,
                None => None,
            }
            .ok_or_else(|| bad(format!("commit {cid} is pushed erased, with no erasure")))?;
            let latest = chain
                .steps
                .last()
                .ok_or_else(|| ApiError::internal(format!("erasure of {cid} has no steps")))?;
            let replacement: ContentId = latest
                .replacement
                .parse()
                .map_err(|_| ApiError::internal("erasure names an unparsable replacement"))?;
            let index = &runtime.expect("held above").index;
            *position_bytes(request, at) = index.read_replacement(&replacement).await?;
            erased.insert(cid);
        }
        Ok(erased)
    }

    /// The commits `request` carries as originals that this storage holds
    /// erased: the sender has not received their erasure. They are taken as
    /// their replacements, as commits pushed erased are, so their originals
    /// reach neither validation nor the ledger's state. Names every commit of
    /// a list holding one, when the sender named none. A cluster, which
    /// takes no erased history, refuses the push.
    async fn originals_erased_here(
        &self,
        request: &mut crate::PushCommitsRequest,
    ) -> Result<Vec<(Position, ContentId)>> {
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(Vec::new());
        };
        let clustered = self.retention.replicas.get().is_some();
        let mut found = Vec::new();
        for (commits, ids, at) in [
            (
                &mut request.commits,
                &mut request.commit_ids,
                Position::Line as fn(usize) -> Position,
            ),
            (
                &mut request.merged_commits,
                &mut request.merged_commit_ids,
                Position::Merged,
            ),
        ] {
            let named: Vec<ContentId> = match ids.len() == commits.len() {
                true => ids.clone(),
                false => commits
                    .iter()
                    .map(|c| ContentId::new(fluree_db_core::ContentKind::Commit, &c.0))
                    .collect(),
            };
            let mut any = false;
            for (i, cid) in named.iter().enumerate() {
                if !commits[i].0.is_empty() && runtime.index.erased(cid).await?.is_some() {
                    if clustered {
                        return Err(crate::commit_transfer::cannot_hold_erasures());
                    }
                    found.push((at(i), cid.clone()));
                    any = true;
                }
            }
            if any && ids.is_empty() {
                *ids = named;
            }
        }
        Ok(found)
    }

    /// Ready `request`, a push of `ledger_id`'s history to a receiver whose
    /// head is `base`, for erased history and retention. Each erased commit
    /// among its commits (named in `commit_ids` / `merged_commit_ids`)
    /// travels as empty bytes and its chain, without its transaction
    /// document; each erased commit of `base`'s history is listed with its
    /// chain's digest (the receiver asks for those it lacks: see
    /// [`Self::erasure_pushes`]). The ledger's retention section rides
    /// along. Returns whether the push carries erased history, which only a
    /// receiver advertising `push.erasures` takes.
    pub async fn ready_push(
        &self,
        ledger_id: &str,
        request: &mut crate::PushCommitsRequest,
        base: Option<&ContentId>,
    ) -> Result<bool> {
        request.retention = self.retention_archive_section(ledger_id).await?;
        request.erasure_frames.clear();
        request.base_erasures.clear();
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(false);
        };
        if !runtime.index.any_erased().await? {
            return Ok(false);
        }
        let mut sender = Sender::new(Some(runtime.index.clone()));
        let mut frames = Vec::new();
        let mut erased_txns = HashSet::new();
        let mut carried = false;
        for (commits, ids, what) in [
            (&mut request.commits, &request.commit_ids, "commit_ids"),
            (
                &mut request.merged_commits,
                &request.merged_commit_ids,
                "merged_commit_ids",
            ),
        ] {
            if ids.len() != commits.len() {
                return Err(ApiError::internal(format!(
                    "a push of erased history names its commits: {what} has {} for {}",
                    ids.len(),
                    commits.len()
                )));
            }
            for (bytes, cid) in commits.iter_mut().zip(ids) {
                let Some(erased) = runtime.index.erased(cid).await? else {
                    continue;
                };
                bytes.0.clear();
                erased_txns.extend(erased.txn.clone());
                frames.extend(sender.chain_frames(&erased).await?);
                carried = true;
            }
        }
        // What a transaction document of an erased commit holds stays here.
        request.blobs.retain(|key, _| !held_by(key, &erased_txns));
        request
            .missing_blobs
            .retain(|key| !held_by(key, &erased_txns));
        if let Some(base) = base {
            let store = self.branched_content_store(ledger_id).await?;
            let history = crate::pack::compute_missing_commits(
                store.as_ref(),
                std::slice::from_ref(base),
                &HashSet::new(),
            )
            .await?;
            for cid in history {
                let Some(erased) = runtime.index.erased(&cid).await? else {
                    continue;
                };
                request.base_erasures.push(crate::BaseErasure {
                    commit: cid,
                    digest: erased.digest(),
                    steps: erased.steps.len(),
                });
            }
        }
        request.erasure_frames = frames.into_iter().map(crate::Base64Bytes).collect();
        Ok(carried || !request.base_erasures.is_empty())
    }

    /// The chains a push's receiver asked for (see
    /// [`crate::ERASURES_NEEDED`]): those of `needed` that are erased
    /// commits of `ledger_id`'s history up to `base`, the receiver's head.
    /// They go ahead of the push, in pushes of no commits a body can hold.
    pub async fn erasure_pushes(
        &self,
        ledger_id: &str,
        base: &ContentId,
        needed: &[ContentId],
        limit: usize,
    ) -> Result<ErasurePushes> {
        let runtime = self.erasure_runtime()?;
        let store = self.branched_content_store(ledger_id).await?;
        let history = crate::pack::compute_missing_commits(
            store.as_ref(),
            std::slice::from_ref(base),
            &HashSet::new(),
        )
        .await?;
        let mut chains = Vec::new();
        for cid in history.iter().filter(|cid| needed.contains(cid)) {
            chains.extend(runtime.index.erased(cid).await?);
        }
        Ok(ErasurePushes {
            index: runtime.index.clone(),
            retention: self.retention_archive_section(ledger_id).await?,
            groups: by_request(chains).into(),
            limit,
        })
    }
}

/// `chains` in groups that share no request: a receiver refuses part of a
/// request, which leaves a commit holding what it erased.
fn by_request(chains: Vec<ErasedCommit>) -> Vec<Vec<ErasedCommit>> {
    let mut groups: Vec<Vec<ErasedCommit>> = Vec::new();
    let mut group_of: HashMap<String, usize> = HashMap::new();
    for erased in chains {
        let mut joined: Vec<usize> = erased
            .steps
            .iter()
            .filter_map(|step| group_of.get(&step.request_id).copied())
            .collect();
        joined.sort_unstable();
        joined.dedup();
        let into = joined.first().copied().unwrap_or(groups.len());
        if into == groups.len() {
            groups.push(Vec::new());
        }
        for other in joined.into_iter().skip(1) {
            let moved = std::mem::take(&mut groups[other]);
            groups[into].extend(moved);
        }
        groups[into].push(erased);
        for member in &groups[into] {
            for step in &member.steps {
                group_of.insert(step.request_id.clone(), into);
            }
        }
    }
    groups.retain(|group| !group.is_empty());
    groups
}

/// Erasure chains a push's receiver asked for, as pushes of no commits:
/// each carries what `limit` bytes of frames hold, or one request's chains,
/// with the journal that records them. The receiver takes each as it
/// comes, so an interrupted run leaves it asking only for the rest.
pub struct ErasurePushes {
    index: Arc<ErasureIndex>,
    retention: Option<serde_json::Value>,
    groups: VecDeque<Vec<ErasedCommit>>,
    limit: usize,
}

impl ErasurePushes {
    /// The next push, or `None` once every chain went.
    pub async fn next(&mut self) -> Result<Option<crate::PushCommitsRequest>> {
        // Each push carries the authorizations and artifacts its chains need.
        let mut sender = Sender::new(Some(self.index.clone()));
        let (mut frames, mut size) = (Vec::new(), 0);
        while let Some(group) = self.groups.front() {
            let mut grouped = Vec::new();
            for erased in group {
                grouped.extend(sender.chain_frames(erased).await?);
            }
            let bytes: usize = grouped.iter().map(Vec::len).sum();
            if !frames.is_empty() && size + bytes > self.limit {
                break;
            }
            frames.extend(grouped);
            size += bytes;
            self.groups.pop_front();
        }
        Ok((!frames.is_empty()).then(|| crate::PushCommitsRequest {
            erasure_frames: frames.into_iter().map(crate::Base64Bytes).collect(),
            retention: self.retention.clone(),
            ..Default::default()
        }))
    }
}

/// What an object request answers for a permanently erased commit, or for
/// its transaction document.
pub enum ErasedObject {
    /// Pack frames: the ledger's retention section as a `phase: "retention"`
    /// manifest, then the commit's chain with its replacement, receipts and
    /// authorizations. A receiver takes them as a pack's.
    Chain(Vec<u8>),
    /// The transaction document of an erased commit, which is not served;
    /// or an erased commit `ledger_id`'s folder does not hold, whose
    /// erasure carries another ledger's data.
    Withheld,
}

impl Fluree {
    /// What a request for object `id` of `ledger_id` answers instead of its
    /// bytes, when the object is an erased commit or its transaction
    /// document: an erased commit's bytes are its replacement, which its CID
    /// does not match.
    pub async fn erased_object(
        &self,
        ledger_id: &str,
        id: &ContentId,
    ) -> Result<Option<ErasedObject>> {
        if matches!(
            id.content_kind(),
            Some(fluree_db_core::ContentKind::Txn | fluree_db_core::ContentKind::Commit)
        ) {
            self.confirm_history_current(ledger_id).await?;
        }
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(None);
        };
        match id.content_kind() {
            Some(fluree_db_core::ContentKind::Txn) => Ok(runtime
                .index
                .is_erased_txn(&id.address_digest())
                .await?
                .then_some(ErasedObject::Withheld)),
            Some(fluree_db_core::ContentKind::Commit) => {
                let Some(erased) = runtime.index.erased(id).await? else {
                    return Ok(None);
                };
                let record = self
                    .nameservice()
                    .lookup(ledger_id)
                    .await?
                    .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
                if !runtime
                    .index
                    .holds_original(&record.storage_namespace(), id)
                    .await?
                {
                    return Ok(Some(ErasedObject::Withheld));
                }
                let mut bytes = Vec::new();
                if let Some(mut section) = self.retention_archive_section(ledger_id).await? {
                    section["phase"] = serde_json::Value::from("retention");
                    fluree_db_core::pack::encode_manifest_frame(&section, &mut bytes);
                }
                let mut sender = Sender::new(Some(runtime.index.clone()));
                for frame in sender.chain_frames(&erased).await? {
                    bytes.extend(frame);
                }
                Ok(Some(ErasedObject::Chain(bytes)))
            }
            _ => Ok(None),
        }
    }
}

/// Whether `key`, a pushed blob's CID or address, names one of `digests`.
fn held_by(key: &str, digests: &HashSet<String>) -> bool {
    key.parse::<ContentId>()
        .map(|id| id.address_digest())
        .is_ok_and(|digest| digests.contains(&digest))
}

/// Where a pushed commit's bytes are: in `commits`, or `merged_commits`.
#[derive(Clone, Copy)]
enum Position {
    Line(usize),
    Merged(usize),
}

fn position_bytes(request: &mut crate::PushCommitsRequest, at: Position) -> &mut Vec<u8> {
    match at {
        Position::Line(i) => &mut request.commits[i].0,
        Position::Merged(i) => &mut request.merged_commits[i].0,
    }
}

/// The pushed commits sent erased, as empty bytes under a named CID.
fn erased_positions(request: &crate::PushCommitsRequest) -> Result<Vec<(Position, ContentId)>> {
    let mut positions = Vec::new();
    for (commits, ids, at) in [
        (
            &request.commits,
            &request.commit_ids,
            Position::Line as fn(usize) -> Position,
        ),
        (
            &request.merged_commits,
            &request.merged_commit_ids,
            Position::Merged,
        ),
    ] {
        for (i, bytes) in commits.iter().enumerate() {
            if !bytes.0.is_empty() {
                continue;
            }
            let cid = ids
                .get(i)
                .filter(|_| ids.len() == commits.len())
                .ok_or_else(|| bad("a commit pushed erased needs its CID named".into()))?;
            positions.push((at(i), cid.clone()));
        }
    }
    Ok(positions)
}

/// The bytes of the pushed commits not sent erased, by CID.
fn pushed_bytes(request: &crate::PushCommitsRequest) -> HashMap<ContentId, Arc<[u8]>> {
    let mut bytes = HashMap::new();
    for (commits, ids) in [
        (&request.commits, &request.commit_ids),
        (&request.merged_commits, &request.merged_commit_ids),
    ] {
        for (i, commit) in commits.iter().enumerate() {
            if commit.0.is_empty() {
                continue;
            }
            let cid = match ids.get(i) {
                Some(cid) => cid.clone(),
                None => ContentId::new(fluree_db_core::ContentKind::Commit, &commit.0),
            };
            bytes.insert(cid, Arc::from(commit.0.as_slice()));
        }
    }
    bytes
}

/// The CID of a push's last commit.
fn pushed_head(request: &crate::PushCommitsRequest) -> Result<ContentId> {
    let last = request
        .commits
        .len()
        .checked_sub(1)
        .ok_or_else(|| bad("an empty push".into()))?;
    Ok(match request.commit_ids.get(last) {
        Some(cid) => cid.clone(),
        None => ContentId::new(
            fluree_db_core::ContentKind::Commit,
            &request.commits[last].0,
        ),
    })
}

impl Fluree {
    /// Hold `view`'s branch against reconciliation with received erasures
    /// until its commit publishes. Refused: a transaction staged before one,
    /// at the head it left, that retracts a value it took out of the current
    /// state; its commit would hold the erased value again.
    pub(crate) async fn hold_against_reconciliation(
        &self,
        view: &fluree_db_ledger::StagedLedger,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<super::Reconciled>>> {
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(None);
        };
        let base = view.base();
        let held = runtime.fence(base.ledger_id().as_str()).read_owned().await;
        if held.head.is_some()
            && held.head == base.head_commit_id
            && view
                .staged_flakes()
                .iter()
                .any(|f| !f.op && held.facts.contains(&runtime.fact_hash(f)))
        {
            return Err(ApiError::http(
                409,
                format!(
                    "this transaction was staged before {} received an erasure of a value \
                     it retracts; stage it again",
                    base.ledger_id()
                ),
            ));
        }
        Ok(Some(held))
    }

    /// Refuse to export, page by page, a history holding an erased commit:
    /// only a pack carries erasures, and a receiver already holding one of
    /// those commits would go on serving what it erased.
    pub(crate) async fn refuse_erased_history(
        &self,
        ledger_id: &str,
        store: &dyn ContentStore,
        head: &ContentId,
    ) -> Result<()> {
        match self.erased_in_history(ledger_id, store, head).await? {
            Some(cid) => Err(ApiError::http(
                501,
                format!(
                    "commit {cid} of this history is permanently erased; only a pack \
                     carries erased history"
                ),
            )),
            None => Ok(()),
        }
    }

    /// A commit of `head`'s history, `ledger_id`'s, this storage holds
    /// erased, if any.
    pub(crate) async fn erased_in_history(
        &self,
        ledger_id: &str,
        store: &dyn ContentStore,
        head: &ContentId,
    ) -> Result<Option<ContentId>> {
        Ok(self
            .erased_reached(ledger_id, store, head)
            .await?
            .into_iter()
            .next()
            .map(|(cid, _)| cid))
    }

    /// The commits of `head`'s history, `ledger_id`'s, read through `store`,
    /// that this storage holds erased, oldest first.
    ///
    /// Only an erased commit whose file is in one of the ledger's folders
    /// (or which one of them reads as its replacement) can be in its
    /// history: when none is, nothing is walked, whatever other ledgers
    /// erased. Otherwise the walk stops at the head last walked, as long as
    /// the erased commits the folders hold are the same.
    pub(crate) async fn erased_reached(
        &self,
        ledger_id: &str,
        store: &dyn ContentStore,
        head: &ContentId,
    ) -> Result<Vec<(ContentId, ErasedCommit)>> {
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(Vec::new());
        };
        let index = &runtime.index;
        if !index.any_erased().await? {
            return Ok(Vec::new());
        }
        // Read before what it versions.
        let version = index.version();
        let folders = self.ledger_folders(ledger_id).await?;
        let prior = runtime.reached.lock().get(ledger_id).cloned();
        // Only the erasures changed since: another ledger's erasure costs a
        // probe of its own commits, once.
        let held = match &prior {
            Some(prior) if prior.folders == folders && prior.version == version => {
                prior.held.clone()
            }
            Some(prior) if prior.folders == folders => {
                let mut held = prior.held.clone();
                for commit in index.held_in(&folders, prior.version).await? {
                    if !held.contains(&commit) {
                        held.push(commit);
                    }
                }
                held.sort_by_key(ContentId::address_digest);
                held
            }
            _ => index.held_in(&folders, 0).await?,
        };
        let reached = if held.is_empty() {
            Vec::new()
        } else {
            let prior = prior.filter(|prior| prior.held == held);
            let stop = prior.as_ref().map(|prior| &prior.head);
            let (walked, met) = walk_down_to(store, head, stop).await?;
            #[cfg(test)]
            runtime
                .reach_reads
                .fetch_add(walked.len(), std::sync::atomic::Ordering::Relaxed);
            let mut reached: Vec<ContentId> = match (met, prior) {
                (true, Some(prior)) => prior.reached,
                _ => Vec::new(),
            };
            for cid in walked.into_iter().rev() {
                if held.contains(&cid) && !reached.contains(&cid) {
                    reached.push(cid);
                }
            }
            reached
        };
        runtime.reached.lock().insert(
            ledger_id.to_string(),
            Reached {
                version,
                folders,
                held,
                head: head.clone(),
                reached: reached.clone(),
            },
        );
        let mut erased = Vec::with_capacity(reached.len());
        for cid in reached {
            if let Some(chain) = index.erased(&cid).await? {
                erased.push((cid, chain));
            }
        }
        Ok(erased)
    }

    /// The folders `ledger_id`'s store reads commits from: its own, then
    /// each source branch's.
    async fn ledger_folders(&self, ledger_id: &str) -> Result<Vec<StorageNamespace>> {
        let mut folders = Vec::new();
        let mut next = self.nameservice().lookup(ledger_id).await?;
        while let Some(record) = next {
            let namespace = record.storage_namespace();
            if folders.contains(&namespace) {
                break;
            }
            folders.push(namespace);
            next = match &record.source_branch {
                Some(source) => {
                    let source = fluree_db_core::LedgerId::from_parts(&record.name, source)?;
                    self.nameservice().lookup(source.as_str()).await?
                }
                None => None,
            };
        }
        Ok(folders)
    }
}

/// Which erased commits a ledger's history holds, as last found.
#[derive(Clone, Debug)]
pub(crate) struct Reached {
    /// The erasure index's version it was found at.
    version: u64,
    /// The ledger's folders: another ledger under the name has its own.
    folders: Vec<StorageNamespace>,
    /// The erased commits the ledger's folders hold, the only ones its
    /// history can.
    held: Vec<ContentId>,
    /// The head whose history was walked.
    head: ContentId,
    /// Those of `held` its history holds, oldest first.
    reached: Vec<ContentId>,
}

/// The commits of `head`'s history, newest first, down to `stop` (not
/// included, nor what is under it), and whether the walk met `stop`.
async fn walk_down_to(
    store: &dyn ContentStore,
    head: &ContentId,
    stop: Option<&ContentId>,
) -> Result<(Vec<ContentId>, bool)> {
    let mut walked = Vec::new();
    let mut seen = HashSet::new();
    let mut met = false;
    let mut frontier = vec![head.clone()];
    while let Some(cid) = frontier.pop() {
        if Some(&cid) == stop {
            met = true;
            continue;
        }
        if !seen.insert(cid.clone()) {
            continue;
        }
        let envelope = fluree_db_core::load_commit_envelope_by_id(store, &cid).await?;
        walked.push(cid);
        frontier.extend(envelope.parents);
    }
    Ok((walked, met))
}

/// What a stream brought of erasures. Its artifacts are stored as they
/// arrive, inert until a published chain names them.
#[derive(Default)]
pub(crate) struct Received {
    chains: BTreeMap<String, Chain>,
    authorizations: HashMap<String, Authorization>,
}

impl Received {
    /// Erased commits received.
    pub(crate) fn count(&self) -> usize {
        self.chains.len()
    }

    /// Whether an erasure of `commit` was received.
    pub(crate) fn holds(&self, commit: &ContentId) -> bool {
        self.chains.contains_key(&commit.to_string())
    }

    pub(crate) fn chain(&mut self, json: serde_json::Value) -> Result<()> {
        let chain: Chain =
            serde_json::from_value(json).map_err(|e| bad(format!("erasure frame: {e}")))?;
        if chain.erased(Vec::new()).digest() != chain.digest {
            return Err(bad(format!(
                "the erasure of {} does not match its digest",
                chain.commit
            )));
        }
        if chain.steps.is_empty() {
            return Err(bad(format!("the erasure of {} has no steps", chain.commit)));
        }
        match self.chains.get(&chain.commit) {
            Some(held) if *held != chain => {
                Err(bad(format!("two erasures of commit {}", chain.commit)))
            }
            _ => {
                self.chains.insert(chain.commit.clone(), chain);
                Ok(())
            }
        }
    }

    pub(crate) fn authorization(&mut self, json: serde_json::Value) -> Result<()> {
        let authorization: Authorization =
            serde_json::from_value(json).map_err(|e| bad(format!("authorization frame: {e}")))?;
        if !valid_request_id(&authorization.request_id) {
            return Err(bad(format!(
                "\"{}\" is not an erasure request id",
                authorization.request_id
            )));
        }
        match self.authorizations.get(&authorization.request_id) {
            Some(held) if *held != authorization => Err(bad(format!(
                "two authorizations of erasure request {}",
                authorization.request_id
            ))),
            _ => {
                self.authorizations
                    .insert(authorization.request_id.clone(), authorization);
                Ok(())
            }
        }
    }
}

/// Store an artifact a stream brought, under the hash of its bytes.
pub(crate) async fn stage_artifact(
    index: &ErasureIndex,
    kind: ErasureArtifactKind,
    bytes: &[u8],
) -> Result<()> {
    let kind = match kind {
        ErasureArtifactKind::Replacement => Artifact::Replacement,
        ErasureArtifactKind::Receipt => Artifact::Proof,
        ErasureArtifactKind::Control => Artifact::Control,
    };
    index.put_artifact(kind, bytes).await.map(|_| ())
}

/// How a received chain stands to the one this storage holds.
#[derive(Debug, PartialEq, Eq)]
enum Standing {
    /// None is held.
    New,
    /// The same, or a prefix of it: the receiver is ahead.
    Held,
    /// It extends the held chain, from step `from` on.
    Extends { from: usize },
}

fn standing(held: Option<&ErasedCommit>, chain: &Chain) -> Result<Standing> {
    let Some(held) = held else {
        return Ok(Standing::New);
    };
    let shared = held.steps.len().min(chain.steps.len());
    if held.steps[..shared] != chain.steps[..shared] || held.ledger_id != chain.ledger_id {
        return Err(ApiError::http(
            409,
            format!(
                "the erasure of commit {} differs from the one this storage holds",
                chain.commit
            ),
        ));
    }
    Ok(if chain.steps.len() > held.steps.len() {
        Standing::Extends {
            from: held.steps.len(),
        }
    } else {
        Standing::Held
    })
}

impl Fluree {
    /// Take one erasure frame of a stream into `received`, storing an
    /// artifact.
    pub(crate) async fn take_erasure_frame(
        &self,
        frame: PackFrame,
        received: &mut Received,
    ) -> Result<()> {
        let runtime = match self.retention.erasure.get() {
            Some(runtime) if self.retention.replicas.get().is_none() => runtime,
            _ => return Err(crate::commit_transfer::cannot_hold_erasures()),
        };
        match frame {
            PackFrame::Erasure(json) => received.chain(json),
            PackFrame::Authorization(json) => received.authorization(json),
            PackFrame::ErasureArtifact { kind, payload } => {
                stage_artifact(&runtime.index, kind, &payload).await
            }
            _ => Ok(()),
        }
    }

    /// Take the erasures a restore brought into the ledger at `namespace`,
    /// whose history `head` names, before anything reads that history:
    /// verify each chain and check each step against its authorization,
    /// refusing a chain that differs from one this storage holds; reconcile
    /// every ledger here holding a newly erased original (see
    /// [`Self::activate`]); then publish the chains, `namespace` reading
    /// each commit as its replacement. `receiver`, the ledger the history
    /// goes to, takes their purge when only soft-dropped ledgers here hold
    /// the originals.
    pub(crate) async fn receive_erasures(
        &self,
        received: Received,
        receiver: Receiver<'_>,
        namespace: &StorageNamespace,
        history: &dyn ContentStore,
        head: &ContentId,
        journal: Option<&crate::retention::journal::Journal>,
    ) -> Result<ReceivedErasures> {
        let runtime = self.erasure_runtime()?;
        let index = &runtime.index;
        let mut controls: HashMap<&str, (ContentId, i64)> = HashMap::new();
        let mut parents: HashMap<ContentId, Vec<ContentId>> = HashMap::new();
        let mut genesis = None;
        let mut newly = Vec::new();
        for chain in received.chains.values() {
            let cid: ContentId = chain.commit.parse().map_err(|_| {
                bad(format!(
                    "erasure of an unparsable commit id {}",
                    chain.commit
                ))
            })?;
            let mut signatures = Vec::with_capacity(chain.steps.len());
            for step in &chain.steps {
                let authorization =
                    received
                        .authorizations
                        .get(&step.request_id)
                        .ok_or_else(|| {
                            bad(format!(
                                "the erasure of {} names request {}, whose authorization the \
                                 transfer does not carry",
                                chain.commit, step.request_id
                            ))
                        })?;
                if !controls.contains_key(step.request_id.as_str()) {
                    let control = self.check_authorization(index, authorization).await?;
                    controls.insert(&step.request_id, control);
                }
                if !authorization.covers(&chain.commit, &step.slots) {
                    return Err(bad(format!(
                        "the erasure of {} removes slots {:?} that request {} did not name",
                        chain.commit, step.slots, step.request_id
                    )));
                }
                signatures.push(authorization.signatures.into());
            }
            let erased = chain.erased(Vec::new());
            let replacement = index
                .verify_with_signatures(&erased, signatures)
                .await
                .map_err(|e| bad(e.to_string()))?;
            let envelope = read_commit_envelope(&replacement)
                .map_err(|e| bad(format!("the replacement of {}: {e}", chain.commit)))?;
            if envelope.txn.as_ref().map(ContentId::address_digest) != chain.txn {
                return Err(bad(format!(
                    "the erasure of {} names another transaction document than its commit",
                    chain.commit
                )));
            }
            if envelope.t == 1 {
                genesis = Some(cid.clone());
            }
            parents.insert(cid.clone(), envelope.parents);
            let from = match standing(index.erased(&cid).await?.as_ref(), chain)? {
                Standing::New => 0,
                Standing::Extends { from } => from,
                Standing::Held => continue,
            };
            newly.push(Newly {
                cid,
                t: envelope.t,
                chain,
                from,
                replacement,
            });
        }
        self.check_erasures_in_history(history, head, &parents)
            .await?;
        check_journal_covers(&received, journal)?;

        for request in controls.keys() {
            index
                .put_authorization(&received.authorizations[*request])
                .await?;
        }
        let prepared = self.prepare_holders(&newly).await?;
        let jobs = received_jobs(&newly, &controls, &prepared);
        // Each holding ledger's purge, run from its root branch when that
        // holds one; with only soft-dropped ledgers holding the originals,
        // the receiver's.
        let mut hosts: BTreeMap<&str, &str> = BTreeMap::new();
        for holder in &prepared.holders {
            let id = &holder.record.ledger_id;
            let chosen = hosts.entry(id.name()).or_insert(id.as_str());
            if holder.record.source_branch.is_none() {
                *chosen = id.as_str();
            }
        }
        if hosts.is_empty() {
            hosts.insert("", receiver.ledger_id());
        }
        let placed = hosts
            .values()
            .flat_map(|host| jobs.iter().map(|j| (host.to_string(), j.clone())))
            .collect();
        let activation = Activation::new(&received, placed);
        // Ledgers here change, and the erasures publish, only once this is
        // recorded: an activation that stops completes from it.
        let pending = match prepared.holders.is_empty() && activation.jobs.is_empty() {
            true => None,
            false => {
                let id = activation.id();
                let bytes = serde_json::to_vec(&activation)
                    .map_err(|e| ApiError::internal(e.to_string()))?;
                index.put_pending(&id, &bytes).await?;
                Some(id)
            }
        };
        let deferred = match receiver {
            Receiver::Restored(id) => Some(id),
            Receiver::Live(_) => None,
        };
        let reconciled = self
            .activate(
                prepared,
                &newly,
                &activation,
                Some(namespace),
                pending.as_deref(),
                deferred,
            )
            .await?;
        let jobs: Vec<_> = activation
            .jobs
            .into_iter()
            .filter(|(host, _)| Some(host.as_str()) == deferred)
            .map(|(_, job)| job)
            .collect();
        Ok(ReceivedErasures {
            genesis,
            reconciled,
            pending: pending.filter(|_| !jobs.is_empty()),
            jobs,
        })
    }

    /// Check that `authorization`'s control commit, stored as an artifact,
    /// records its request and manifest, and that this storage holds no
    /// other authorization of the request. Returns the control commit's id
    /// and `t`.
    async fn check_authorization(
        &self,
        index: &ErasureIndex,
        authorization: &Authorization,
    ) -> Result<(ContentId, i64)> {
        let request = &authorization.request_id;
        let bytes = index
            .artifact(Artifact::Control, &authorization.control_bytes)
            .await
            .map_err(|e| {
                bad(format!(
                    "the control commit of erasure request {request}: {e}"
                ))
            })?;
        let control = authorization.check_control(&bytes).map_err(|why| {
            bad(format!(
                "erasure request {request} is not authorized: {why}"
            ))
        })?;
        if index
            .authorization(request)
            .await?
            .is_some_and(|held| held != *authorization)
        {
            return Err(ApiError::http(
                409,
                format!(
                    "the authorization of erasure request {request} differs from the one this \
                     storage holds"
                ),
            ));
        }
        Ok(control)
    }

    /// Lock and check every ledger here holding an original `newly` erases,
    /// writing nothing: see [`Self::activate`].
    ///
    /// Each live branch whose history holds one is locked against commits
    /// from here until it no longer holds an erased value as current: a
    /// transaction meanwhile could retract one, in a commit holding the
    /// value that no prover here could erase. Refused: a branch whose own
    /// commit bounds an erased span, and one with a full-text or vector
    /// index reading an erased property that cannot be rebuilt.
    async fn prepare_holders(&self, newly: &[Newly<'_>]) -> Result<Prepared> {
        let runtime = self.erasure_runtime()?;
        let cids: Vec<ContentId> = newly.iter().map(|n| n.cid.clone()).collect();
        let commits: Vec<String> = cids.iter().map(ToString::to_string).collect();
        let commits: Vec<&str> = commits.iter().map(String::as_str).collect();
        let mut prepared = Prepared {
            dropped: self
                .dropped_holders(&commits)
                .await?
                .into_iter()
                .map(|(r, _)| r.ledger_id.to_string())
                .collect(),
            ..Default::default()
        };
        if newly.is_empty() {
            return Ok(prepared);
        }
        let occurrences: Vec<OccurrenceRef> = newly
            .iter()
            .flat_map(|n| {
                n.chain.steps[n.from..].iter().flat_map(|step| {
                    step.slots.iter().map(|slot| OccurrenceRef {
                        commit: n.chain.commit.clone(),
                        slot: *slot,
                    })
                })
            })
            .collect();
        let listed = self.ledgers_holding(&cids, None).await?;
        // Before any holder is locked or changed: a triple term spelling an
        // erased value would keep it readable there.
        let mut holders = Vec::with_capacity(listed.len());
        for record in &listed {
            let store = self
                .content_store_for_record_or_id(Some(record), &record.storage_namespace())
                .await?;
            holders.push((record.clone(), store));
        }
        holders.extend(self.dropped_holders(&commits).await?);
        if let Some(why) = self.spelled_in(&holders, &occurrences).await? {
            return Err(ApiError::http(409, why));
        }
        for listed in listed {
            // The cache's lock first, as a commit through the cache takes
            // them.
            let guard = self.lock_ledger(&listed.ledger_id).await?;
            let fence = runtime.fence(listed.ledger_id.as_str()).write_owned().await;
            let Some(record) = self.nameservice().lookup(&listed.ledger_id).await? else {
                continue;
            };
            let held = guard
                .as_ref()
                .map(crate::ledger_manager::LedgerWriteGuard::clone_state);
            let Some(removal) = self
                .received_removal(record.ledger_id.as_str(), occurrences.clone(), held)
                .await?
            else {
                continue;
            };
            prepared.holders.push(Holder {
                record,
                removal,
                guard,
                fence,
            });
        }
        let branches: BTreeSet<String> = prepared
            .holders
            .iter()
            .map(|h| h.record.ledger_id.to_string())
            .collect();
        let properties: BTreeSet<&str> = prepared
            .holders
            .iter()
            .flat_map(|h| h.removal.removals.iter().map(|r| r.predicate.as_str()))
            .collect();
        let known = (!properties.contains("")).then_some(&properties);
        prepared.sources = self
            .sources_over(&branches.iter().collect(), known)
            .await?
            .into_iter()
            .filter(|s| !s.dropped && s.reads_erased)
            .map(|s| s.id)
            .collect();
        prepared.properties = properties.iter().map(ToString::to_string).collect();
        if let Some(stuck) = self.unrebuildable_sources(&prepared.sources).await?.first() {
            return Err(ApiError::http(
                409,
                format!(
                    "{stuck} indexes a ledger here that holds an erased value and cannot be \
                     rebuilt from it; drop it first"
                ),
            ));
        }
        for holder in &prepared.holders {
            prepared
                .indexing
                .extend(self.hold_indexing(&holder.record.ledger_id).await?);
        }
        #[cfg(test)]
        hooks::checked().await;
        Ok(prepared)
    }

    /// Make every ledger `prepared` holds read as the erasures `newly` will,
    /// then publish `activation`'s chains, `restoring` (a restored ledger's
    /// folder) reading them as replacements too, and record its jobs.
    ///
    /// Each holder has the occurrences recorded in its removal journal,
    /// against its own history, and their events erased from its index:
    /// what is history there leaf by leaf, what is still current as a
    /// retraction would remove it, by an incremental build that leaves no
    /// history of it; a value outside the leaves by a rebuild reading the
    /// received replacements. Its full-text and vector indexes that may read
    /// an erased property are rebuilt. Nothing is committed to it: a
    /// retraction would hold the value. A transaction staged there before
    /// is refused if it retracts a value it no longer holds as current (see
    /// [`Self::hold_against_reconciliation`]).
    ///
    /// `pending`, the activation's record, is deleted once it is complete:
    /// once every job is recorded, but those of `deferred`, a restored
    /// ledger not yet visible, whose restore records them and then deletes
    /// it.
    async fn activate(
        &self,
        prepared: Prepared,
        newly: &[Newly<'_>],
        activation: &Activation,
        restoring: Option<&StorageNamespace>,
        pending: Option<&str>,
        deferred: Option<&str>,
    ) -> Result<Vec<String>> {
        let runtime = self.erasure_runtime()?;
        let index = &runtime.index;
        #[cfg(test)]
        hooks::stop(hooks::Stop::BeforeHolders)?;
        let replacements: Arc<HashMap<ContentId, Arc<[u8]>>> = Arc::new(
            newly
                .iter()
                .map(|n| (n.cid.clone(), n.replacement.clone()))
                .collect(),
        );
        let mut reconciled = Vec::new();
        for holder in prepared.holders {
            let Holder {
                record,
                removal,
                mut guard,
                mut fence,
            } = holder;
            if fence.head != record.commit_head_id {
                fence.head = record.commit_head_id.clone();
                fence.facts.clear();
            }
            fence.facts.extend(
                removal
                    .flakes
                    .iter()
                    .zip(&removal.removals)
                    .filter(|(flake, entry)| flake.op && entry.until_t.is_none())
                    .map(|(flake, _)| runtime.fact_hash(flake)),
            );
            let captured: Vec<Captured> = removal
                .flakes
                .iter()
                .zip(&removal.removals)
                .map(|(flake, entry)| {
                    Ok(Captured {
                        flake: flake.clone(),
                        commit: entry.commit.parse().map_err(|_| {
                            ApiError::internal(format!("not a commit id: {}", entry.commit))
                        })?,
                        // Retracted since its index, an assertion is current
                        // there: the erased retraction no longer hides it.
                        current_until: flake.op.then(|| entry.until_t.unwrap_or(i64::MAX)),
                    })
                })
                .collect::<Result<_>>()?;
            let scope_t = removal.scope_t;
            self.record_received_removal(removal).await?;
            // Its unindexed commits read as the erasures will have them.
            let reading: Arc<dyn ContentStore> = Arc::new(Substituted {
                inner: self
                    .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                    .await?,
                replacements: replacements.clone(),
            });
            if self
                .erase_from_held_index(&record, &captured, Some(reading.clone()))
                .await?
                == Erased::NeedsRebuild
            {
                self.rebuild_index_from(reading.clone(), &record).await?;
            }
            #[cfg(test)]
            hooks::stop(hooks::Stop::AfterHolder)?;
            let current = self
                .nameservice()
                .lookup(&record.ledger_id)
                .await?
                .ok_or_else(|| ApiError::NotFound(record.ledger_id.to_string()))?;
            match guard.as_mut() {
                // From here on nothing current holds an erased value, and
                // no transaction can retract one.
                Some(guard) => {
                    let state = self
                        .load_queryable_state_with_store(reading, current)
                        .await?;
                    guard.ledger().sync_binary_store_from_state(&state).await;
                    guard.replace(state);
                }
                // Without a cache, nothing locks out a transaction: one
                // since the checks fails the activation, which resumes.
                None if current.commit_t != scope_t => {
                    return Err(ApiError::http(
                        409,
                        format!(
                            "{} changed while it was made to read as the erasures; they are \
                             activated once it is checked again",
                            record.ledger_id
                        ),
                    ));
                }
                None => {}
            }
            drop(guard);
            drop(fence);
            reconciled.push(record.ledger_id.to_string());
        }
        self.rebuild_sources(&prepared.sources).await?;
        let published = activation
            .chains
            .iter()
            .map(|chain| {
                let cid: ContentId = chain.commit.parse().expect("parsed when received");
                let at = restoring
                    .map(|namespace| index.commit_address(namespace, &cid))
                    .into_iter()
                    .collect();
                chain.erased(at)
            })
            .collect();
        index.publish(published).await?;
        // Their cached states read through the substituting store until
        // now; they read the published erasures from here on.
        for ledger_id in &reconciled {
            if let Ok(id) = fluree_db_core::LedgerId::parse(ledger_id) {
                self.announce_refresh(&id).await;
            }
        }
        // A purge, which recording a job may wait for, holds indexing.
        drop(prepared.indexing);
        #[cfg(test)]
        hooks::stop(hooks::Stop::AfterPublish)?;
        let mut by_branch: BTreeMap<&str, Vec<super::ErasureJob>> = BTreeMap::new();
        for (branch, job) in &activation.jobs {
            by_branch.entry(branch).or_default().push(job.clone());
        }
        let deferring = deferred.is_some_and(|d| by_branch.remove(d).is_some());
        for (branch, jobs) in by_branch {
            self.record_jobs(branch, &jobs).await?;
        }
        if let Some(id) = pending.filter(|_| !deferring) {
            index.complete_pending(id).await?;
        }
        Ok(reconciled)
    }

    /// Record `jobs` in `branch`'s registration (see [`add_jobs`]). A branch
    /// gone since, a restored ledger whose restore failed after publishing,
    /// takes none, once no soft-dropped ledger holds an original they purge.
    async fn record_jobs(&self, branch: &str, jobs: &[super::ErasureJob]) -> Result<()> {
        // A pass under way would mark a job that grew purged.
        let _purging = self.erasure_runtime()?.purging.lock().await;
        let recorded = registry::update(
            self.publisher()?,
            self.registry_objects(),
            branch,
            |current| {
                let mut next = current.cloned().unwrap_or_default();
                add_jobs(&mut next, jobs).then_some(next)
            },
        )
        .await;
        match recorded {
            Ok(_) => {
                self.retention.mark_active(branch);
                Ok(())
            }
            Err(ApiError::NotFound(_)) => {
                let commits: Vec<&str> = jobs
                    .iter()
                    .flat_map(|j| j.commits.iter().map(|c| c.commit.as_str()))
                    .collect();
                match self.dropped_holders(&commits).await?.is_empty() {
                    true => Ok(()),
                    false => Err(ApiError::http(
                        409,
                        format!(
                            "{branch}, which was to purge received erasures, is gone, and \
                             soft-dropped ledgers still hold their originals"
                        ),
                    )),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Complete the activations of received erasures that a restore
    /// recorded and did not finish. Returns how many completed.
    pub async fn resume_received_erasures(&self) -> Result<usize> {
        let Some(runtime) = self.retention.erasure.get() else {
            return Ok(0);
        };
        let _admission = runtime.admission.write().await;
        let mut completed = 0;
        for (id, bytes) in runtime.index.pending().await? {
            let resumed = async {
                let activation: Activation = serde_json::from_slice(&bytes)
                    .map_err(|e| ApiError::internal(format!("erasure activation {id}: {e}")))?;
                let newly = self.newly_of(&activation.chains).await?;
                let prepared = self.prepare_holders(&newly).await?;
                self.activate(prepared, &newly, &activation, None, Some(&id), None)
                    .await
            };
            match resumed.await {
                Ok(_) => completed += 1,
                Err(e) => {
                    tracing::warn!(activation = %id, error = %e, "erasure: received erasures not activated");
                }
            }
        }
        Ok(completed)
    }

    /// Of `chains`, verified again, those that erase more of their commit
    /// than this storage has.
    async fn newly_of<'a>(&self, chains: &'a [Chain]) -> Result<Vec<Newly<'a>>> {
        let index = &self.erasure_runtime()?.index;
        let mut newly = Vec::new();
        for chain in chains {
            let cid: ContentId = chain
                .commit
                .parse()
                .map_err(|_| ApiError::internal(format!("not a commit id: {}", chain.commit)))?;
            let from = match standing(index.erased(&cid).await?.as_ref(), chain)? {
                Standing::New => 0,
                Standing::Extends { from } => from,
                Standing::Held => continue,
            };
            let replacement = index.verify(&chain.erased(Vec::new())).await?;
            let t = read_commit_envelope(&replacement)
                .map_err(|e| ApiError::internal(format!("replacement of {}: {e}", chain.commit)))?
                .t;
            newly.push(Newly {
                cid,
                t,
                chain,
                from,
                replacement,
            });
        }
        Ok(newly)
    }

    /// Record `jobs` in the registration of `record`, a restored ledger not
    /// yet visible whose head is at `head_t`, for its purge; then complete
    /// `pending`, the activation that deferred them.
    pub(crate) async fn record_received_jobs(
        &self,
        record: &fluree_db_nameservice::NsRecord,
        head_t: i64,
        jobs: Vec<super::ErasureJob>,
        pending: Option<String>,
    ) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        registry::update_fenced(
            self.publisher()?,
            self.registry_objects(),
            record.ledger_id.as_str(),
            record.fence,
            |current| {
                let mut next = current.cloned().unwrap_or_else(|| registry::Registration {
                    active_since_t: head_t + 1,
                    watermark_t: head_t,
                    ..Default::default()
                });
                add_jobs(&mut next, &jobs).then_some(next)
            },
        )
        .await?;
        self.retention.mark_active(record.ledger_id.as_str());
        if let Some(id) = pending {
            self.erasure_runtime()?.index.complete_pending(&id).await?;
        }
        Ok(())
    }

    /// Refuse erasures of commits the history `head` names in `store` does
    /// not hold; `parents` are the erased commits' own.
    async fn check_erasures_in_history(
        &self,
        store: &dyn ContentStore,
        head: &ContentId,
        parents: &HashMap<ContentId, Vec<ContentId>>,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        let mut frontier = vec![head.clone()];
        while let Some(cid) = frontier.pop() {
            if !seen.insert(cid.clone()) {
                continue;
            }
            match parents.get(&cid) {
                Some(of) => frontier.extend(of.iter().cloned()),
                None => {
                    let bytes = store.get(&cid).await?;
                    let envelope = read_commit_envelope(&bytes)
                        .map_err(|e| bad(format!("commit {cid}: {e}")))?;
                    frontier.extend(envelope.parents);
                }
            }
        }
        match parents.keys().find(|cid| !seen.contains(*cid)) {
            Some(cid) => Err(bad(format!(
                "the transfer carries an erasure of commit {cid}, which its history does not \
                 hold"
            ))),
            None => Ok(()),
        }
    }
}

/// Refuse erasures whose occurrences the transfer's removal journal does not
/// record: the receiver's history would serve, as complete, a history
/// missing them.
fn check_journal_covers(
    received: &Received,
    journal: Option<&crate::retention::journal::Journal>,
) -> Result<()> {
    let recorded: HashSet<(&str, u32)> = journal
        .map(|j| {
            j.removals
                .iter()
                .map(|r| (r.commit.as_str(), r.slot))
                .collect()
        })
        .unwrap_or_default();
    for chain in received.chains.values() {
        for step in &chain.steps {
            if let Some(slot) = step
                .slots
                .iter()
                .find(|s| !recorded.contains(&(chain.commit.as_str(), **s)))
            {
                return Err(bad(format!(
                    "the transfer's removal journal does not record erased operation {slot} \
                     of commit {}",
                    chain.commit
                )));
            }
        }
    }
    Ok(())
}

/// A received chain that erases more of its commit than this storage has.
struct Newly<'a> {
    cid: ContentId,
    t: i64,
    chain: &'a Chain,
    /// Its first step this storage does not hold.
    from: usize,
    /// Its latest replacement, verified.
    replacement: Arc<[u8]>,
}

/// The ledgers here holding newly erased originals, locked and checked.
#[derive(Default)]
struct Prepared {
    /// Live branches whose history holds one.
    holders: Vec<Holder>,
    /// Branches of soft-dropped ledgers, left to the purge.
    dropped: Vec<String>,
    /// The properties (IRIs) of the occurrences erased there; one empty
    /// where a predicate did not decode.
    properties: Vec<String>,
    /// Their full-text and vector indexes that may read one.
    sources: Vec<String>,
    /// Their indexing, held until the erasures are published.
    indexing: Vec<fluree_db_indexer::MaintenanceGuard>,
}

/// A live branch holding a newly erased original.
struct Holder {
    record: fluree_db_nameservice::NsRecord,
    removal: crate::retention::removal::ReceivedRemoval,
    /// Its write lock, when this instance caches ledgers.
    guard: Option<crate::ledger_manager::LedgerWriteGuard>,
    /// Held against every commit to it.
    fence: tokio::sync::OwnedRwLockWriteGuard<super::Reconciled>,
}

/// The ledger a transfer brings erased history to.
#[derive(Clone, Copy)]
pub(crate) enum Receiver<'a> {
    /// A visible one: a pull's.
    Live(&'a str),
    /// One a restore makes, visible once it completes.
    Restored(&'a str),
}

impl Receiver<'_> {
    fn ledger_id(&self) -> &str {
        match self {
            Receiver::Live(id) | Receiver::Restored(id) => id,
        }
    }
}

/// Received erasures being activated, recorded before the first ledger
/// here changes for them: an activation that stops completes from it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Activation {
    chains: Vec<Chain>,
    /// Published jobs to record, each with the branch whose registration
    /// takes it, for its purge.
    jobs: Vec<(String, super::ErasureJob)>,
}

impl Activation {
    fn new(received: &Received, jobs: Vec<(String, super::ErasureJob)>) -> Self {
        Activation {
            chains: received.chains.values().cloned().collect(),
            jobs,
        }
    }

    /// Its chains' digests, hashed.
    fn id(&self) -> String {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        for chain in &self.chains {
            hasher.update(chain.digest.as_bytes());
        }
        hex::encode(hasher.finalize())
    }
}

/// What a restore's erasures did here.
pub(crate) struct ReceivedErasures {
    /// The history's genesis commit, when it is erased.
    pub(crate) genesis: Option<ContentId>,
    /// Branches of other ledgers here made to read as the erasures do.
    pub(crate) reconciled: Vec<String>,
    /// Published jobs for the restored ledger's registration, when only
    /// soft-dropped ledgers here hold the originals: its purge deletes them.
    pub(crate) jobs: Vec<super::ErasureJob>,
    /// The activation to complete once they are recorded.
    pub(crate) pending: Option<String>,
}

/// Add `jobs` to a registration. A published job of the same request takes
/// the commits, slots, branches and properties it lacks, and is purged
/// again when it grows: a transfer can bring a request's erasures a part at
/// a time, a fork's before its source's. Whether it changed.
fn add_jobs(registration: &mut registry::Registration, jobs: &[super::ErasureJob]) -> bool {
    let mut changed = false;
    for job in jobs {
        let Some(held) = registration
            .erasures
            .iter_mut()
            .find(|j| j.request_id == job.request_id)
        else {
            registration.erasures.push(job.clone());
            changed = true;
            continue;
        };
        // Unless published here: still being proved, refused (its own
        // publication covers it), or in a later release's stage.
        let published = matches!(
            held.stage,
            super::ErasureStage::Published
                | super::ErasureStage::Purging
                | super::ErasureStage::Purged
        );
        if !published || !merge_job(held, job) {
            continue;
        }
        let deleted = held.purge.as_ref().map_or(0, |p| p.deleted);
        held.purge = Some(super::PurgeProgress {
            deleted,
            ..job.purge.clone().unwrap_or_default()
        });
        held.stage = super::ErasureStage::Published;
        changed = true;
    }
    changed
}

/// Merge into `held` what `job`, of the same request, adds. Whether it grew.
fn merge_job(held: &mut super::ErasureJob, job: &super::ErasureJob) -> bool {
    let mut grew = false;
    for commit in &job.commits {
        let Some(at) = held.commits.iter_mut().find(|c| c.commit == commit.commit) else {
            held.commits.push(commit.clone());
            grew = true;
            continue;
        };
        for slot in &commit.slots {
            if !at.slots.contains(slot) {
                at.slots.push(*slot);
                grew = true;
            }
        }
        at.slots.sort_unstable();
        for branch in &commit.branches {
            if !at.branches.contains(branch) {
                at.branches.push(branch.clone());
                grew = true;
            }
        }
    }
    // None listed is any property.
    if !held.properties.is_empty() {
        if job.properties.is_empty() {
            held.properties.clear();
            grew = true;
        }
        for property in &job.properties {
            if !held.properties.contains(property) {
                held.properties.push(property.clone());
                grew = true;
            }
        }
    }
    grew
}

/// A published job per request of `newly`, its holders' indexes already
/// rebuilt, for the purge to delete their originals.
fn received_jobs(
    newly: &[Newly<'_>],
    controls: &HashMap<&str, (ContentId, i64)>,
    prepared: &Prepared,
) -> Vec<super::ErasureJob> {
    if prepared.holders.is_empty() && prepared.dropped.is_empty() {
        return Vec::new();
    }
    let now = crate::retention::worker::now_ms();
    let branches: Vec<String> = prepared
        .holders
        .iter()
        .map(|h| h.record.ledger_id.to_string())
        .chain(prepared.dropped.iter().cloned())
        .collect();
    let mut jobs: BTreeMap<&str, super::ErasureJob> = BTreeMap::new();
    for n in newly {
        for step in &n.chain.steps[n.from..] {
            let (control, authorized_t) = &controls[step.request_id.as_str()];
            let job = jobs
                .entry(step.request_id.as_str())
                .or_insert_with(|| super::ErasureJob {
                    request_id: step.request_id.clone(),
                    requester: None,
                    accepted_ms: now,
                    authorized_t: *authorized_t,
                    authorized_commit: Some(control.to_string()),
                    signatures: Default::default(),
                    stage: super::ErasureStage::Published,
                    commits: Vec::new(),
                    properties: match prepared.properties.iter().any(String::is_empty) {
                        true => Vec::new(),
                        false => prepared.properties.clone(),
                    },
                    retries: 0,
                    retry_at_ms: None,
                    last_error: None,
                    published_ms: Some(now),
                    added: Vec::new(),
                    purge: Some(super::PurgeProgress {
                        rebuilt_ms: Some(now),
                        ..Default::default()
                    }),
                    cluster: None,
                    as_written: None,
                });
            job.commits.push(super::CommitErasure {
                commit: n.chain.commit.clone(),
                t: n.t,
                slots: step.slots.clone(),
                ledger_id: n.chain.ledger_id.clone(),
                branches: branches.clone(),
                proved: None,
            });
        }
    }
    jobs.into_values().collect()
}

/// A store reading received replacements in place of their originals, for
/// a rebuild before the erasures are published.
#[derive(Debug)]
struct Substituted {
    inner: Arc<dyn ContentStore>,
    replacements: Arc<HashMap<ContentId, Arc<[u8]>>>,
}

#[async_trait::async_trait]
impl ContentStore for Substituted {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        match self.replacements.contains_key(id) {
            true => Ok(true),
            false => self.inner.has(id).await,
        }
    }

    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<fluree_db_core::ContentBytes> {
        match self.replacements.get(id) {
            Some(bytes) => Ok(bytes.clone().into()),
            None => self.inner.get(id).await,
        }
    }

    async fn put(
        &self,
        kind: fluree_db_core::ContentKind,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentId> {
        self.inner.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.put_with_id(id, bytes).await
    }

    fn blob_pinner(&self) -> Option<fluree_db_core::storage::pins::BlobPinner> {
        self.inner.blob_pinner()
    }

    fn intents_loaded(&self) -> Option<u64> {
        self.inner.intents_loaded()
    }

    fn get_local(
        &self,
        id: &ContentId,
    ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
        match self.replacements.contains_key(id) {
            true => Ok(None),
            false => self.inner.get_local(id),
        }
    }

    async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
        self.inner.release(id).await
    }
}

/// Where a test stops an activation, as a crash would, or holds it once its
/// holders are locked and checked.
#[cfg(test)]
pub(crate) mod hooks {
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Stop {
        BeforeHolders,
        AfterHolder,
        AfterPublish,
    }

    pub(crate) static STOP: parking_lot::Mutex<Option<Stop>> = parking_lot::Mutex::new(None);

    /// Notified once the holders are checked; the activation then waits
    /// for the second.
    #[allow(clippy::type_complexity)]
    pub(crate) static CHECKED: parking_lot::Mutex<
        Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    > = parking_lot::Mutex::new(None);

    pub(crate) fn stop(at: Stop) -> crate::Result<()> {
        match *STOP.lock() == Some(at) {
            true => Err(crate::ApiError::internal(format!("stopped {at:?}"))),
            false => Ok(()),
        }
    }

    pub(crate) async fn checked() {
        let notifies = CHECKED.lock().clone();
        if let Some((checked, proceed)) = notifies {
            checked.notify_one();
            proceed.notified().await;
        }
    }
}

#[cfg(test)]
mod activation_tests;

#[cfg(test)]
mod reach_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// An authorization's signature mode is the one its control commit
    /// records, and one from before the mode was recorded is `verify`.
    #[test]
    fn an_authorization_takes_its_signature_mode_from_its_control_commit() {
        use super::super::SignatureMode::{Expose, Verify};
        let manifest = vec![OccurrenceRef {
            commit: "c".into(),
            slot: 1,
        }];
        let control = |mode: Option<&str>| {
            let mut entries = vec![
                fluree_db_core::TxnMetaEntry::new(
                    FLUREE_DB,
                    db::ERASURE_REQUEST,
                    TxnMetaValue::String("erasure-1".into()),
                ),
                fluree_db_core::TxnMetaEntry::new(
                    FLUREE_DB,
                    db::ERASURE_MANIFEST,
                    TxnMetaValue::String(manifest_hash(&manifest)),
                ),
            ];
            entries.extend(mode.map(|mode| {
                fluree_db_core::TxnMetaEntry::new(
                    FLUREE_DB,
                    db::ERASURE_SIGNATURES,
                    TxnMetaValue::String(mode.into()),
                )
            }));
            let mut commit = fluree_db_core::Commit::new(2, Vec::new());
            commit.txn_meta = entries;
            fluree_db_core::commit::codec::write_commit(&commit, false, None)
                .unwrap()
                .bytes
        };
        let checked = |bytes: &[u8], signatures| {
            Authorization {
                request_id: "erasure-1".into(),
                ledger_id: "l:main".into(),
                control: ContentId::new(fluree_db_core::ContentKind::Commit, bytes).to_string(),
                control_bytes: String::new(),
                manifest: manifest.clone(),
                added: Vec::new(),
                signatures,
            }
            .check_control(bytes)
            .is_ok()
        };
        let exposed = control(Some("expose"));
        assert!(checked(&exposed, Expose));
        assert!(!checked(&exposed, Verify));
        let verified = control(Some("verify"));
        assert!(checked(&verified, Verify));
        assert!(!checked(&verified, Expose));
        let earlier = control(None);
        assert!(checked(&earlier, Verify));
        assert!(!checked(&earlier, Expose));
    }

    /// A received erasure grows only a job published here: one in a later
    /// release's stage is left as it is.
    #[test]
    fn received_erasures_leave_a_job_in_a_later_release_stage_alone() {
        let job = |stage: &str, commits: &[&str]| -> super::super::ErasureJob {
            let commits: Vec<_> = commits
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "commit": c, "t": 1, "slots": [0],
                        "ledgerId": "l:main", "branches": ["l:main"],
                    })
                })
                .collect();
            serde_json::from_value(serde_json::json!({
                "requestId": "erasure-1", "acceptedMs": 1, "authorizedT": 1,
                "stage": stage, "commits": commits,
            }))
            .unwrap()
        };
        let received = [job("published", &["a", "b"])];
        for (stage, grows) in [("published", true), ("queued", false)] {
            let mut registration = registry::Registration {
                erasures: vec![job(stage, &["a"])],
                ..Default::default()
            };
            assert_eq!(add_jobs(&mut registration, &received), grows, "{stage}");
            let held = &registration.erasures[0];
            assert_eq!(held.commits.len(), if grows { 2 } else { 1 }, "{stage}");
            let after = if grows { "published" } else { stage };
            assert_eq!(serde_json::to_value(&held.stage).unwrap(), after);
        }
    }

    fn step(id: &str) -> ErasureStep {
        ErasureStep {
            request_id: id.into(),
            slots: vec![1],
            replacement: format!("r-{id}"),
            proof: format!("p-{id}"),
        }
    }

    fn chain(steps: &[&str]) -> Chain {
        Chain::of(&ErasedCommit {
            commit: "c".into(),
            ledger_id: "l:main".into(),
            txn: None,
            steps: steps.iter().map(|s| step(s)).collect(),
            replaced_at: vec!["sender's folder".into()],
        })
    }

    #[test]
    fn a_chain_stands_to_the_held_one_by_its_steps() {
        let held = chain(&["a", "b"]).erased(Vec::new());
        assert_eq!(standing(None, &chain(&["a"])).unwrap(), Standing::New);
        assert_eq!(
            standing(Some(&held), &chain(&["a", "b"])).unwrap(),
            Standing::Held
        );
        assert_eq!(
            standing(Some(&held), &chain(&["a"])).unwrap(),
            Standing::Held
        );
        assert_eq!(
            standing(Some(&held), &chain(&["a", "b", "c"])).unwrap(),
            Standing::Extends { from: 2 }
        );
        let divergent = standing(Some(&held), &chain(&["a", "x"])).unwrap_err();
        assert_eq!(divergent.status_code(), 409);
    }

    #[test]
    fn a_chain_travels_without_the_senders_folders() {
        let json = serde_json::to_value(chain(&["a"])).unwrap();
        assert!(json.get("replacedAt").is_none());
        let mut received = Received::default();
        received.chain(json.clone()).unwrap();
        let mut forged = json;
        forged["steps"][0]["slots"] = serde_json::json!([2]);
        assert_eq!(
            Received::default().chain(forged).unwrap_err().status_code(),
            400,
            "a step changed without its digest"
        );
    }

    #[test]
    fn request_ids_name_no_path() {
        assert!(valid_request_id("erasure-0a1b2c"));
        for bad in ["erasure-", "erasure-../x", "removal-0a", "erasure-0a/b"] {
            assert!(!valid_request_id(bad), "{bad}");
        }
    }
}
