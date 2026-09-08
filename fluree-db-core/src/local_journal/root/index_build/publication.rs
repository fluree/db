//! Typed publication and recovery authorization for prepared index files.
use super::*;
use crate::local_journal::IndexPublication;

impl LocalRoot {
    /// Serialize physical revalidation, latest-head merge, semantic validation,
    /// journal flush and state installation. Callbacks must not reenter this root.
    /// The embedding must preserve latest commit/config fields and newer novelty;
    /// its explicit index validator must check the built-through root and closure.
    pub fn publish_index(
        self: &Arc<Self>,
        prepared: &PreparedIndex,
        merge: impl FnOnce(&Object) -> Result<Vec<u8>>,
        validator: &impl AcceptanceValidator,
        install: impl FnOnce(&AcceptanceView<'_>, &Receipt) -> Result<()>,
    ) -> Result<Receipt> {
        let mut state = self.coordinator.lock();
        state.frontier()?;
        let index = prepared.verified_checkpoint(self)?;
        let (key, bytes) = state
            .head
            .as_ref()
            .ok_or(Error::Invalid("index publication without head"))?;
        let head = Object {
            key: key.clone(),
            bytes: bytes.clone(),
        };
        let resulting_head = merge(&head)?;
        let transition = Transition {
            ledger: self.ledger().into(),
            generation: self.generation().into(),
            head_key: head.key,
            expected_head: Some(head.bytes),
            resulting_head,
            objects: Vec::new(),
            index_publication: Some(IndexPublication {
                build: prepared
                    .build_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or(Error::Invalid("index build path"))?
                    .into(),
                manifest: prepared.manifest_digest(),
                input_prefix: prepared.frontier().prefix_digest(),
                input_head: prepared.head().clone(),
            }),
        };
        state.accept_index(
            &transition,
            validator,
            &mut FileTarget {
                root: &self.path.join(JOURNAL_DIR).join(DATA),
            },
            install,
            Some(index),
        )
    }
}

/// Only journal records can authorize opening private builds on recovery. No
/// directory scan, unreferenced manifest or loose materialization file is authority.
pub(in crate::local_journal::root) fn open_indexes(
    root: &Path,
    manifest: &Manifest,
    records: &[Record],
    lease: Arc<File>,
) -> Result<Vec<Arc<Checkpoint>>> {
    let mut indexes = Vec::new();
    for record in records {
        if let Some(p) = &record.transition.index_publication {
            p.validate()?;
            let build = checked_path(root, &format!("{JOURNAL_DIR}/{BUILDS}/{}", p.build), false)?;
            let index = Checkpoint::open(
                &build,
                checkpoint::Binding {
                    identity: manifest.identity,
                    ledger: &manifest.ledger,
                    generation: &manifest.generation,
                    index_build: Some(p.input_prefix),
                },
                p.manifest,
                lease.clone(),
            )?;
            if !index.matches_publication(&record.transition) {
                return Err(Error::Invalid("published index manifest binding"));
            }
            indexes.push(index);
        }
    }
    Ok(indexes)
}

#[cfg(test)]
mod tests;
