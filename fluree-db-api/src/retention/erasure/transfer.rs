//! Erased history in transfers (`DATA_ERASURE.md`, "Transferring erased
//! history"). An erased commit never travels as bytes under its original
//! CID, which its replacement does not match: it travels as its erasure
//! chain, with its latest replacement, every step's receipt, and what
//! authorized each step. A receiver verifies the chain, checks each step
//! against its authorization, and publishes the chain in its own storage,
//! where the commit then reads as its replacement.

use std::collections::{BTreeMap, HashMap, HashSet};
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
use super::request::manifest_hash;
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
}

impl Authorization {
    /// Why `bytes` are not this request's control commit, if they are not.
    fn check_control(&self, bytes: &[u8]) -> std::result::Result<(), String> {
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
        Ok(())
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

/// The erasures of the commits a stream sends, and what it has sent.
pub(crate) struct Sender {
    index: Option<Arc<ErasureIndex>>,
    erased: HashMap<ContentId, ErasedCommit>,
    authorizations: HashSet<String>,
    artifacts: HashSet<String>,
}

impl Sender {
    /// The erasures among `commits`.
    pub(crate) async fn of(fluree: &Fluree, commits: &[ContentId]) -> Result<Self> {
        let index = fluree.retention.erasure.get().map(|r| r.index.clone());
        let mut erased = HashMap::new();
        if let Some(index) = index.as_ref().filter(|_| !commits.is_empty()) {
            if index.any_erased().await? {
                for cid in commits {
                    if let Some(e) = index.erased(cid).await? {
                        erased.insert(cid.clone(), e);
                    }
                }
            }
        }
        Ok(Sender {
            index,
            erased,
            authorizations: HashSet::new(),
            artifacts: HashSet::new(),
        })
    }

    /// Erased commits among those sent.
    pub(crate) fn count(&self) -> usize {
        self.erased.len()
    }

    /// Artifacts sent so far.
    pub(crate) fn artifacts_sent(&self) -> usize {
        self.artifacts.len()
    }

    /// Refuse a stream whose receiver does not take erased history.
    pub(crate) fn refuse(&self) -> Result<()> {
        match self.erased.keys().min() {
            Some(cid) => Err(ApiError::http(
                501,
                format!(
                    "commit {cid} is permanently erased; this transfer does not carry erased \
                     commits yet"
                ),
            )),
            None => Ok(()),
        }
    }

    /// The frames carrying `commit` when it is erased: the authorizations
    /// not sent yet, its latest replacement and receipts, then its chain.
    pub(crate) async fn frames(&mut self, commit: &ContentId) -> Result<Option<Vec<Vec<u8>>>> {
        let (Some(index), Some(erased)) = (self.index.clone(), self.erased.get(commit).cloned())
        else {
            return Ok(None);
        };
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
        let json = serde_json::to_value(Chain::of(&erased))
            .map_err(|e| ApiError::internal(e.to_string()))?;
        frames.push(frame(|b| encode_erasure_frame(&json, b)));
        Ok(Some(frames))
    }
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
    /// It extends the held chain.
    Extends,
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
        Standing::Extends
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
    /// verify each chain and check each step against its authorization;
    /// refuse a chain that differs from one this storage holds, or one
    /// that would newly erase an original some ledger here holds; then
    /// publish them, `namespace` reading each commit as its replacement.
    /// Returns the history's genesis commit when it is erased.
    pub(crate) async fn receive_erasures(
        &self,
        received: Received,
        namespace: &StorageNamespace,
        head: &ContentId,
    ) -> Result<Option<ContentId>> {
        let runtime = self.erasure_runtime()?;
        let index = &runtime.index;
        let mut controls: HashSet<&str> = HashSet::new();
        let mut parents: HashMap<ContentId, Vec<ContentId>> = HashMap::new();
        let mut genesis = None;
        let mut newly_erased = Vec::new();
        for chain in received.chains.values() {
            let cid: ContentId = chain.commit.parse().map_err(|_| {
                bad(format!(
                    "erasure of an unparsable commit id {}",
                    chain.commit
                ))
            })?;
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
                if controls.insert(&step.request_id) {
                    let bytes = index
                        .artifact(Artifact::Control, &authorization.control_bytes)
                        .await
                        .map_err(|e| {
                            bad(format!(
                                "the control commit of erasure request {}: {e}",
                                step.request_id
                            ))
                        })?;
                    authorization.check_control(&bytes).map_err(|why| {
                        bad(format!(
                            "erasure request {} is not authorized: {why}",
                            step.request_id
                        ))
                    })?;
                }
                if !authorization.covers(&chain.commit, &step.slots) {
                    return Err(bad(format!(
                        "the erasure of {} removes slots {:?} that request {} did not name",
                        chain.commit, step.slots, step.request_id
                    )));
                }
            }
            let erased = chain.erased(Vec::new());
            let replacement = index
                .verify(&erased)
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
            if standing(index.erased(&cid).await?.as_ref(), chain)? != Standing::Held {
                newly_erased.push(cid);
            }
        }
        self.check_erasures_in_history(namespace, head, &parents)
            .await?;
        if let Some((cid, holder)) = self.first_holder(&newly_erased).await? {
            return Err(ApiError::http(
                409,
                format!(
                    "the transfer erases commit {cid}, which {holder} holds on this storage; \
                     receiving an erasure of history held here is not supported yet"
                ),
            ));
        }
        for request in controls {
            index
                .put_authorization(&received.authorizations[request])
                .await?;
        }
        let published = received
            .chains
            .values()
            .map(|chain| {
                let cid: ContentId = chain.commit.parse().expect("parsed above");
                chain.erased(vec![index.commit_address(namespace, &cid)])
            })
            .collect();
        index.publish(published).await?;
        Ok(genesis)
    }

    /// Refuse erasures of commits the history `head` names in `namespace`
    /// does not hold; `parents` are the erased commits' own.
    async fn check_erasures_in_history(
        &self,
        namespace: &StorageNamespace,
        head: &ContentId,
        parents: &HashMap<ContentId, Vec<ContentId>>,
    ) -> Result<()> {
        let store = self.content_store(namespace);
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

    /// The first of `originals` a ledger on this storage, live or
    /// soft-dropped, holds in its own folder, with that ledger.
    async fn first_holder(&self, originals: &[ContentId]) -> Result<Option<(ContentId, String)>> {
        if originals.is_empty() {
            return Ok(None);
        }
        let index = &self.erasure_runtime()?.index;
        for record in self.nameservice().all_records().await? {
            if record.retracted {
                continue;
            }
            for cid in originals {
                if index
                    .holds_original(&record.storage_namespace(), cid)
                    .await?
                {
                    return Ok(Some((cid.clone(), record.ledger_id.to_string())));
                }
            }
        }
        let commits: Vec<String> = originals.iter().map(ToString::to_string).collect();
        let commits: Vec<&str> = commits.iter().map(String::as_str).collect();
        for (record, _) in self.dropped_holders(&commits).await? {
            for cid in originals {
                if index
                    .holds_original(&record.storage_namespace(), cid)
                    .await?
                {
                    return Ok(Some((
                        cid.clone(),
                        format!("dropped ledger {}", record.ledger_id),
                    )));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Standing::Extends
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
