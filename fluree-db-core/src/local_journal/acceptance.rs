//! Serialized acceptance shared by the owned file backend and fault tests.
use super::{Error, Journal, JournalIo, Object, Receipt, Record, ReplayTarget, Result, Transition};
use std::collections::{BTreeMap, BTreeSet};

/// Trusted database-layer semantic validation. Implementations must verify the
/// entire supported dependency closure, content identities and head semantics.
/// This is an embedding interface, not a caller-supplied transaction option.
/// The core layer cannot interpret an opaque nameservice head or validate policies.
pub trait AcceptanceValidator {
    fn validate(&self, view: &AcceptanceView<'_>) -> Result<()>;

    /// Revalidate database semantics during a root's recovery installation hook.
    /// The root has already checked framing, generation and head continuity.
    /// Each record can depend only on its own bytes or preceding records.
    fn validate_recovered(&self, records: &[Record]) -> Result<()> {
        let mut accepted = BTreeMap::new();
        for record in records {
            self.validate(&AcceptanceView {
                transition: &record.transition,
                accepted: &accepted,
            })?;
            for object in &record.transition.objects {
                accepted.insert(object.key.clone(), object.bytes.clone());
            }
        }
        Ok(())
    }
}

/// Only bytes carried by this candidate or an earlier accepted journal record.
/// Arbitrary readable files cannot become durable prerequisites through this view.
pub struct AcceptanceView<'a> {
    pub transition: &'a Transition,
    accepted: &'a BTreeMap<String, Vec<u8>>,
}
impl AcceptanceView<'_> {
    pub fn content(&self, key: &str) -> Option<&[u8]> {
        self.transition
            .objects
            .iter()
            .find(|o| o.key == key)
            .map(|o| o.bytes.as_slice())
            .or_else(|| self.accepted.get(key).map(Vec::as_slice))
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
}

impl<I: JournalIo> Coordinator<I> {
    pub fn restored(
        journal: Journal<I>,
        records: &[Record],
        ledger: &str,
        generation: &str,
    ) -> Result<Self> {
        let mut this = Self {
            journal: Some(journal),
            poisoned: false,
            ledger: ledger.into(),
            generation: generation.into(),
            head: None,
            objects: BTreeMap::new(),
            last: None,
        };
        for record in records {
            this.check_transition(&record.transition)?;
            this.advance(&record.transition, record.receipt.clone());
        }
        Ok(this)
    }

    fn check_transition(&self, t: &Transition) -> Result<()> {
        t.validate()?;
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
        let keys: BTreeSet<_> = std::iter::once(t.head_key.as_str())
            .chain(t.objects.iter().map(|o| o.key.as_str()))
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
                    || self.head.as_ref().is_some_and(|(head, _)| head == parent)
                {
                    return Err(Error::Invalid("journal key is another key's parent"));
                }
            }
            let prefix = format!("{key}/");
            if self
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

    fn advance(&mut self, t: &Transition, receipt: Receipt) {
        for Object { key, bytes } in &t.objects {
            self.objects
                .entry(key.clone())
                .or_insert_with(|| bytes.clone());
        }
        self.head = Some((t.head_key.clone(), t.resulting_head.clone()));
        self.last = Some(receipt);
    }

    pub fn accept(
        &mut self,
        t: &Transition,
        validator: &impl AcceptanceValidator,
        target: &mut impl AcceptanceTarget,
        install: impl FnOnce(&AcceptanceView<'_>, &Receipt) -> Result<()>,
    ) -> Result<Receipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.check_transition(t)?;
        let view = AcceptanceView {
            transition: t,
            accepted: &self.objects,
        };
        validator.validate(&view)?;
        target.preflight(t)?;
        if target.read_head(&t.head_key)?.as_deref() != t.expected_head.as_deref() {
            self.poisoned = true;
            return Err(Error::Invalid(
                "materialized head differs from accepted head",
            ));
        }
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
        let finish = (|| {
            for object in &t.objects {
                target.put_immutable(object)?;
            }
            target.publish_head(&t.head_key, t.expected_head.as_deref(), &t.resulting_head)?;
            install(&view, &receipt)
        })();
        if let Err(cause) = finish {
            return Err(Error::AcceptanceUnresolved {
                durable: Some(receipt),
                cause: Box::new(cause),
            });
        }
        self.advance(t, receipt.clone());
        self.poisoned = false;
        Ok(receipt)
    }
}
