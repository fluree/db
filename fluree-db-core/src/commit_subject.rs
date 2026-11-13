//! The txn-meta subject that names a commit.
//!
//! A SHA2-256 commit is `fluree:commit:sha256:<hex>` (namespace
//! [`FLUREE_COMMIT`], name the hex digest), as it always has been. A commit
//! minted with another hash is `urn:fluree:commit-<algorithm>-<hex>`
//! (namespace [`FLUREE_URN`], name `commit-<algorithm>-<hex>`): the reserved
//! codes are all taken, and this spelling says which hash it is. The name
//! carries no `/`, `#` or `:`, so the IRI splits back into exactly this
//! namespace and name, as index lookups require. The CID remains the
//! authority; these subjects only name it in queryable metadata.

use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_URN};

use crate::content_kind::CODEC_FLUREE_COMMIT;
use crate::{ContentId, HashAlgorithm, Sid};

/// Name prefix, under [`FLUREE_URN`], of a commit subject for a hash other
/// than SHA2-256.
pub const COMMIT_URN_NAME_PREFIX: &str = "commit-";

/// The subject naming the commit `id`.
pub fn commit_sid(id: &ContentId) -> Sid {
    let digest = hex::encode(id.as_cid().hash().digest());
    match id.hash_algorithm() {
        Some(HashAlgorithm::Sha2_256) | None => Sid::new(FLUREE_COMMIT, &digest),
        Some(alg) => Sid::new(
            FLUREE_URN,
            format!("{COMMIT_URN_NAME_PREFIX}{}-{digest}", alg.name()),
        ),
    }
}

/// The short spelling commit references accept for `id`: its hex digest for
/// SHA2-256, `<algorithm>:<hex>` otherwise.
pub fn commit_ref(id: &ContentId) -> String {
    let digest = hex::encode(id.as_cid().hash().digest());
    match id.hash_algorithm() {
        Some(HashAlgorithm::Sha2_256) | None => digest,
        Some(alg) => format!("{}:{digest}", alg.name()),
    }
}

/// Whether `sid` names a commit.
#[inline]
pub fn is_commit_sid(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_COMMIT
        || (sid.namespace_code == FLUREE_URN && sid.name.starts_with(COMMIT_URN_NAME_PREFIX))
}

/// The CID a commit subject names; `None` for any other subject.
pub fn commit_id_of_sid(sid: &Sid) -> Option<ContentId> {
    match sid.namespace_code {
        FLUREE_COMMIT => ContentId::from_address_digest(CODEC_FLUREE_COMMIT, &sid.name),
        FLUREE_URN => {
            let (alg, hex) = sid
                .name
                .strip_prefix(COMMIT_URN_NAME_PREFIX)?
                .rsplit_once('-')?;
            let alg: HashAlgorithm = alg.parse().ok()?;
            if alg == HashAlgorithm::Sha2_256 {
                return None;
            }
            ContentId::from_address_digest(CODEC_FLUREE_COMMIT, &format!("{}-{hex}", alg.name()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentKind;

    #[test]
    fn sha2_256_commits_keep_their_subject() {
        let id = ContentId::new(ContentKind::Commit, b"c");
        let sid = commit_sid(&id);
        assert_eq!(sid.namespace_code, FLUREE_COMMIT);
        assert_eq!(&*sid.name, id.address_digest());
        assert!(is_commit_sid(&sid));
        assert_eq!(commit_id_of_sid(&sid), Some(id));
    }

    #[test]
    fn other_hashes_name_their_algorithm() {
        let id = ContentId::new_with(HashAlgorithm::Sha2_512, ContentKind::Commit, b"c");
        let sid = commit_sid(&id);
        assert_eq!(sid.namespace_code, FLUREE_URN);
        assert!(sid.name.starts_with("commit-sha2-512-"), "{}", sid.name);
        assert_eq!(sid.name.len(), "commit-sha2-512-".len() + 128);
        assert!(is_commit_sid(&sid));
        assert_eq!(commit_id_of_sid(&sid), Some(id.clone()));
        assert!(commit_ref(&id).starts_with("sha2-512:"));
        // Other URN names are not commits.
        let graph = Sid::new(FLUREE_URN, "mydb:main#txn-meta");
        assert!(!is_commit_sid(&graph));
        assert_eq!(commit_id_of_sid(&graph), None);
        let tagged_256 = Sid::new(FLUREE_URN, format!("commit-sha2-256-{}", "ab".repeat(32)));
        assert_eq!(commit_id_of_sid(&tagged_256), None);
    }

    /// The IRI splits back into the namespace and name the subject was minted
    /// with, under every split mode: otherwise an index decodes it into the
    /// empty namespace and lookups by this Sid miss.
    #[test]
    fn the_subject_iri_splits_canonically() {
        use crate::ns_encoding::{canonical_split, NsSplitMode};
        let id = ContentId::new_with(HashAlgorithm::Sha2_512, ContentKind::Commit, b"c");
        let sid = commit_sid(&id);
        let iri = format!("{}{}", fluree_vocab::fluree::URN, sid.name);
        for mode in [
            NsSplitMode::MostGranular,
            NsSplitMode::HostPlusN(1),
            NsSplitMode::HostPlusN(3),
        ] {
            assert_eq!(
                canonical_split(&iri, mode),
                (fluree_vocab::fluree::URN, &*sid.name),
                "{mode:?}"
            );
        }
    }
}
