//! Serialized acceptance shared by the owned file backend and fault tests.
use super::Checkpoint;
use super::{Error, Journal, JournalIo, Object, Receipt, Record, ReplayTarget, Result, Transition};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Trusted database-layer semantic validation. Implementations must verify the
/// entire supported dependency closure, content identities and head semantics.
/// This is an embedding interface, not a caller-supplied transaction option.
/// The core layer cannot interpret an opaque nameservice head or validate policies.
pub trait AcceptanceValidator {
    fn validate(&self, view: &AcceptanceView<'_>) -> Result<()>;

    /// Explicit opt-in for index-only publication. Validate index CIDs/closure,
    /// built-through ancestry, monotonic index progress and preservation of the
    /// latest commit/configuration fields. Legacy embeddings reject this kind.
    fn validate_index_publication(
        &self,
        _view: &AcceptanceView<'_>,
        _index: &Checkpoint,
    ) -> Result<()> {
        Err(Error::Invalid(
            "validator does not support index publication",
        ))
    }

    /// Explicit opt-in for a checkpoint-aware embedding. Validate its complete
    /// supported baseline semantics before accepting any dependent transition.
    fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<()> {
        Err(Error::Invalid(
            "validator does not support checkpoint baselines",
        ))
    }

    /// Revalidate database semantics during a root's recovery installation hook.
    /// The root has already checked framing, generation and head continuity.
    /// Each record can depend only on its own bytes or preceding records.
    fn validate_recovered(&self, records: &[Record]) -> Result<()> {
        self.validate_recovered_from(records, None)
    }

    /// Recovery with explicit durable baseline prerequisites. Legacy validators
    /// fail closed rather than silently installing an empty baseline.
    fn validate_recovered_from(
        &self,
        records: &[Record],
        checkpoint: Option<&Checkpoint>,
    ) -> Result<()> {
        self.validate_recovered_with_indexes(records, checkpoint, &[])
    }

    /// Validate each publication using only its own verified build and preceding
    /// accepted prerequisites. Index handles are in journal publication order.
    fn validate_recovered_with_indexes(
        &self,
        records: &[Record],
        checkpoint: Option<&Checkpoint>,
        indexes: &[Arc<Checkpoint>],
    ) -> Result<()> {
        if let Some(checkpoint) = checkpoint {
            self.validate_checkpoint(checkpoint)?;
        }
        let mut accepted = BTreeMap::new();
        let mut published = Vec::new();
        let mut candidates = indexes.iter();
        for record in records {
            let index = if record.transition.index_publication.is_some() {
                let c = candidates
                    .next()
                    .ok_or(Error::Invalid("missing recovery index prerequisites"))?;
                if !c.matches_publication(&record.transition) {
                    return Err(Error::Invalid("recovery index binding"));
                }
                Some(c.clone())
            } else {
                None
            };
            let view = AcceptanceView {
                transition: &record.transition,
                accepted: &accepted,
                checkpoint,
                indexes: &published,
                index: index.as_deref(),
                frontier: None,
            };
            if let Some(index) = &index {
                self.validate_index_publication(&view, index)?;
            } else {
                self.validate(&view)?;
            }
            for object in &record.transition.objects {
                accepted.insert(object.key.clone(), object.bytes.clone());
            }
            if let Some(index) = index {
                published.push(index);
            }
        }
        if candidates.next().is_some() {
            return Err(Error::Invalid("unreferenced recovery index"));
        }
        Ok(())
    }
}

/// Opaque identity of an exact journal prefix. This is NOT a semantic proof or
/// an acknowledgment. Trusted embeddings may bind their own validation to it.
/// The receipt digest chains back to the root-specific journal header, including
/// the checkpoint binding. Head and generation are also compared explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptanceFrontier {
    ledger: String,
    generation: String,
    checkpoint: Option<[u8; 32]>,
    head: Option<Object>,
    receipt: Receipt,
}
impl AcceptanceFrontier {
    pub(super) fn prefix_digest(&self) -> [u8; 32] {
        self.receipt.digest
    }

    pub fn head(&self) -> Option<&[u8]> {
        self.head.as_ref().map(|h| h.bytes.as_slice())
    }
}

/// Only candidate, accepted journal, or explicitly verified checkpoint bytes.
/// Arbitrary readable files cannot become durable prerequisites through this view.
pub struct AcceptanceView<'a> {
    pub transition: &'a Transition,
    accepted: &'a BTreeMap<String, Vec<u8>>,
    checkpoint: Option<&'a Checkpoint>,
    indexes: &'a [Arc<Checkpoint>],
    index: Option<&'a Checkpoint>,
    frontier: Option<AcceptanceFrontier>,
}
impl AcceptanceView<'_> {
    /// Present only in live serialized acceptance. Full recovery validation
    /// deliberately supplies no reusable prefix identity.
    pub fn frontier(&self) -> Option<&AcceptanceFrontier> {
        self.frontier.as_ref()
    }

    /// Identity after this candidate and receipt. Use only in the owner's install
    /// hook, after semantic validation and successful flush. A later failure still
    /// leaves the owner unavailable: this value alone never proves acceptance.
    pub fn frontier_after(&self, receipt: &Receipt) -> AcceptanceFrontier {
        AcceptanceFrontier {
            ledger: self.transition.ledger.clone(),
            generation: self.transition.generation.clone(),
            checkpoint: self.checkpoint.map(Checkpoint::digest),
            head: Some(Object {
                key: self.transition.head_key.clone(),
                bytes: self.transition.resulting_head.clone(),
            }),
            receipt: receipt.clone(),
        }
    }

    /// Borrow journal-covered bytes only; use read_content for checkpoint prerequisites.
    pub fn content(&self, key: &str) -> Option<&[u8]> {
        self.transition
            .objects
            .iter()
            .find(|o| o.key == key)
            .map(|o| o.bytes.as_slice())
            .or_else(|| self.accepted.get(key).map(Vec::as_slice))
    }
    pub fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint
    }

    pub fn read_content(&self, key: &str) -> Result<Option<Cow<'_, [u8]>>> {
        if let Some(bytes) = self.content(key) {
            return Ok(Some(Cow::Borrowed(bytes)));
        }
        for index in self
            .index
            .into_iter()
            .chain(self.indexes.iter().rev().map(Arc::as_ref))
            .chain(self.checkpoint)
        {
            if let Some(bytes) = index.read(key)? {
                return Ok(Some(Cow::Owned(bytes)));
            }
        }
        Ok(None)
    }
}

pub(super) trait AcceptanceTarget: ReplayTarget {
    /// Validate paths and existing immutable bytes without making file effects.
    fn preflight(&mut self, transition: &Transition) -> Result<()>;
}

pub(super) struct Coordinator<I> {
    pub journal: Option<Journal<I>>,
    pub poisoned: bool,
    ledger: String,
    generation: String,
    pub(super) head: Option<(String, Vec<u8>)>,
    objects: BTreeMap<String, Vec<u8>>,
    pub last: Option<Receipt>,
    checkpoint: Option<Arc<Checkpoint>>,
    pub(super) indexes: Vec<Arc<Checkpoint>>,
    frontiers: BTreeMap<[u8; 32], Object>,
}

impl<I: JournalIo> Coordinator<I> {
    pub fn restored(
        journal: Journal<I>,
        records: &[Record],
        ledger: &str,
        generation: &str,
    ) -> Result<Self> {
        Self::restored_from(journal, records, ledger, generation, None)
    }

    pub fn restored_from(
        journal: Journal<I>,
        records: &[Record],
        ledger: &str,
        generation: &str,
        checkpoint: Option<Arc<Checkpoint>>,
    ) -> Result<Self> {
        Self::restored_with_indexes(journal, records, ledger, generation, checkpoint, &[])
    }

    pub(super) fn restored_with_indexes(
        journal: Journal<I>,
        records: &[Record],
        ledger: &str,
        generation: &str,
        checkpoint: Option<Arc<Checkpoint>>,
        indexes: &[Arc<Checkpoint>],
    ) -> Result<Self> {
        let mut frontiers = BTreeMap::new();
        if let Some(c) = &checkpoint {
            frontiers.insert(journal.origin, c.head().clone());
        }
        let mut this = Self {
            journal: Some(journal),
            poisoned: false,
            ledger: ledger.into(),
            generation: generation.into(),
            head: checkpoint
                .as_ref()
                .map(|c| (c.head().key.clone(), c.head().bytes.clone())),
            objects: BTreeMap::new(),
            last: None,
            checkpoint,
            indexes: Vec::new(),
            frontiers,
        };
        let mut candidates = indexes.iter();
        for record in records {
            let index = if record.transition.index_publication.is_some() {
                Some(
                    candidates
                        .next()
                        .ok_or(Error::Invalid("missing index prerequisites"))?
                        .clone(),
                )
            } else {
                None
            };
            this.check_transition(&record.transition, index.as_deref())?;
            this.advance(&record.transition, record.receipt.clone(), index);
        }
        if candidates.next().is_some() {
            return Err(Error::Invalid("unreferenced index prerequisites"));
        }
        Ok(this)
    }

    pub(super) fn frontier(&self) -> Result<AcceptanceFrontier> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        Ok(AcceptanceFrontier {
            ledger: self.ledger.clone(),
            generation: self.generation.clone(),
            checkpoint: self.checkpoint.as_deref().map(Checkpoint::digest),
            head: self.head.as_ref().map(|(key, bytes)| Object {
                key: key.clone(),
                bytes: bytes.clone(),
            }),
            receipt: self.journal.as_ref().ok_or(Error::Poisoned)?.receipt(),
        })
    }

    fn check_transition(&self, t: &Transition, index: Option<&Checkpoint>) -> Result<()> {
        t.validate()?;
        match (&t.index_publication, index) {
            (None, None) => {}
            (Some(p), Some(c))
                if c.matches_publication(t)
                    && self.frontiers.get(&p.input_prefix) == Some(&p.input_head)
                    && !self.indexes.iter().any(|old| old.digest() == p.manifest) => {}
            _ => return Err(Error::Invalid("index publication prefix/prerequisites")),
        }
        if t.ledger != self.ledger || t.generation != self.generation {
            return Err(Error::Invalid("acceptance ledger/generation"));
        }
        if self.head.as_ref().map(|(_, bytes)| bytes.as_slice()) != t.expected_head.as_deref()
            || self
                .head
                .as_ref()
                .is_some_and(|(key, _)| key != &t.head_key)
        {
            return Err(Error::Conflict);
        }
        if t.expected_head.as_deref() == Some(t.resulting_head.as_slice()) {
            return Err(Error::Invalid("unchanged acceptance head"));
        }
        // Prevent an object becoming a head, or an old head becoming an object.
        if self.objects.contains_key(&t.head_key)
            || t.objects.iter().any(|o| {
                self.head.as_ref().is_some_and(|(key, _)| &o.key == key)
                    || self
                        .objects
                        .get(&o.key)
                        .is_some_and(|bytes| bytes != &o.bytes)
            })
        {
            return Err(Error::Invalid("immutable key/head conflict"));
        }
        let sources: Vec<_> = self
            .checkpoint
            .iter()
            .map(Arc::as_ref)
            .chain(self.indexes.iter().map(Arc::as_ref))
            .chain(index)
            .collect();
        if let Some(index) = index {
            for e in index.entries() {
                if self
                    .objects
                    .get(&e.key)
                    .is_some_and(|b| b.len() as u64 != e.length || super::digest(b) != e.sha256)
                    || sources.iter().any(|c| {
                        c.entry(&e.key)
                            .is_some_and(|old| old.length != e.length || old.sha256 != e.sha256)
                    })
                {
                    return Err(Error::Invalid("index immutable key conflict"));
                }
            }
        }
        for checkpoint in &sources {
            if checkpoint.entry(&t.head_key).is_some()
                || t.objects.iter().any(|o| {
                    checkpoint.entry(&o.key).is_some_and(|entry| {
                        entry.length != o.bytes.len() as u64
                            || entry.sha256 != super::digest(&o.bytes)
                    })
                })
            {
                return Err(Error::Invalid("checkpoint immutable key conflict"));
            }
        }
        let keys: BTreeSet<_> = std::iter::once(t.head_key.as_str())
            .chain(t.objects.iter().map(|o| o.key.as_str()))
            .chain(
                index
                    .into_iter()
                    .flat_map(|c| c.entries().iter().map(|e| e.key.as_str())),
            )
            .collect();
        for key in &keys {
            if key.split('/').any(|part| part == ".fluree-wal") {
                return Err(Error::Invalid("reserved journal key"));
            }
            // Reject file-vs-directory collisions within and across records.
            for (i, _) in key.match_indices('/') {
                let parent = &key[..i];
                if keys.contains(parent)
                    || self.objects.contains_key(parent)
                    || sources.iter().any(|c| c.entry(parent).is_some())
                    || self.head.as_ref().is_some_and(|(head, _)| head == parent)
                {
                    return Err(Error::Invalid("journal key is another key's parent"));
                }
            }
            let prefix = format!("{key}/");
            if sources.iter().any(|c| c.has_descendant(key))
                || self
                    .objects
                    .range(prefix.clone()..)
                    .next()
                    .is_some_and(|(old, _)| old.starts_with(&prefix))
            {
                return Err(Error::Invalid("journal key replaces an existing directory"));
            }
        }
        Ok(())
    }

    fn advance(&mut self, t: &Transition, receipt: Receipt, index: Option<Arc<Checkpoint>>) {
        for Object { key, bytes } in &t.objects {
            self.objects
                .entry(key.clone())
                .or_insert_with(|| bytes.clone());
        }
        self.head = Some((t.head_key.clone(), t.resulting_head.clone()));
        self.frontiers.insert(
            receipt.digest,
            Object {
                key: t.head_key.clone(),
                bytes: t.resulting_head.clone(),
            },
        );
        self.last = Some(receipt);
        if let Some(index) = index {
            self.indexes.push(index);
        }
    }

    pub fn accept(
        &mut self,
        t: &Transition,
        validator: &impl AcceptanceValidator,
        target: &mut impl AcceptanceTarget,
        install: impl FnOnce(&AcceptanceView<'_>, &Receipt) -> Result<()>,
    ) -> Result<Receipt> {
        self.accept_index(t, validator, target, install, None)
    }

    pub(super) fn accept_index(
        &mut self,
        t: &Transition,
        validator: &impl AcceptanceValidator,
        target: &mut impl AcceptanceTarget,
        install: impl FnOnce(&AcceptanceView<'_>, &Receipt) -> Result<()>,
        index: Option<Arc<Checkpoint>>,
    ) -> Result<Receipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let started = std::time::Instant::now();
        self.check_transition(t, index.as_deref())?;
        let view = AcceptanceView {
            transition: t,
            accepted: &self.objects,
            checkpoint: self.checkpoint.as_deref(),
            indexes: &self.indexes,
            index: index.as_deref(),
            frontier: Some(self.frontier()?),
        };
        if let Some(checkpoint) = &self.checkpoint {
            validator.validate_checkpoint(checkpoint)?;
        }
        if let Some(index) = &index {
            validator.validate_index_publication(&view, index)?;
        } else {
            validator.validate(&view)?;
        }
        target.preflight(t)?;
        if target.read_head(&t.head_key)?.as_deref() != t.expected_head.as_deref() {
            self.poisoned = true;
            return Err(Error::Invalid(
                "materialized head differs from accepted head",
            ));
        }
        let validated = std::time::Instant::now();
        // Any panic/error from here leaves the owner unavailable until recovery.
        self.poisoned = true;
        let journal = self.journal.as_mut().ok_or(Error::Poisoned)?;
        let receipt = match journal.append_and_sync(t) {
            Ok(receipt) => receipt,
            Err(error) => {
                // Capacity/encoding rejection before I/O has a known outcome.
                self.poisoned = journal.poisoned;
                return Err(if self.poisoned {
                    Error::AcceptanceUnresolved {
                        durable: None,
                        cause: Box::new(error),
                    }
                } else {
                    error
                });
            }
        };
        let flushed = std::time::Instant::now();
        let finish = (|| {
            for object in &t.objects {
                target.put_immutable(object)?;
            }
            target.publish_head(&t.head_key, t.expected_head.as_deref(), &t.resulting_head)?;
            let materialized = std::time::Instant::now();
            install(&view, &receipt)?;
            tracing::debug!(target: "fluree::journal_probe", sequence = receipt.sequence, validate_us = validated.duration_since(started).as_micros() as u64, materialize_us = materialized.duration_since(flushed).as_micros() as u64, install_us = materialized.elapsed().as_micros() as u64, "journal acceptance phases");
            Ok(())
        })();
        if let Err(cause) = finish {
            return Err(Error::AcceptanceUnresolved {
                durable: Some(receipt),
                cause: Box::new(cause),
            });
        }
        self.advance(t, receipt.clone(), index);
        self.poisoned = false;
        Ok(receipt)
    }
}
