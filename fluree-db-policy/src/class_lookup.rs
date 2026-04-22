//! Subject class lookup for policy enforcement
//!
//! This module provides functions to look up the classes (rdf:type) of subjects
//! from the database. This is needed for f:onClass policy enforcement.

use fluree_db_core::{FlakeValue, GraphDbRef, RangeMatch, RangeOptions, RangeTest, Sid};
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::RDF_TYPE;
use std::collections::{HashMap, HashSet};

use crate::error::PolicyError;
use crate::Result;

/// Look up the classes (rdf:type values) for a set of subjects.
///
/// This function queries the database for rdf:type flakes for each subject
/// and returns a map from subject to its class SIDs.
///
/// # Arguments
///
/// * `subjects` - The subject SIDs to look up classes for
/// * `db` - Database reference bundling snapshot, graph id, overlay, and time
///
/// # Returns
///
/// A map from subject SID to a vector of class SIDs. Subjects with no
/// rdf:type assertions will not be present in the map.
pub async fn lookup_subject_classes(
    subjects: &[Sid],
    db: GraphDbRef<'_>,
) -> Result<HashMap<Sid, Vec<Sid>>> {
    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    // Create the rdf:type SID
    let rdf_type = Sid::new(RDF, RDF_TYPE);

    // Prefer an index-native batched lookup when available (binary range provider).
    if let Some(provider) = db.snapshot.range_provider.as_ref() {
        let opts = RangeOptions::new().with_to_t(db.t);
        match provider.lookup_subject_predicate_refs_batched(
            db.g_id,
            fluree_db_core::IndexType::Psot,
            &rdf_type,
            subjects,
            &opts,
            db.overlay,
        ) {
            Ok(map) => return Ok(map),
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                // Fall through to the per-subject SPOT lookup on Unsupported.
            }
            Err(e) => {
                return Err(PolicyError::ClassLookup {
                    message: format!("Batched class lookup failed: {e}"),
                });
            }
        }
    }

    let mut result: HashMap<Sid, Vec<Sid>> = HashMap::new();

    // Fallback: Query rdf:type for each unique subject (correct but can be slow).
    let unique_subjects: HashSet<&Sid> = subjects.iter().collect();
    for subject in unique_subjects {
        let range_match = RangeMatch::subject_predicate(subject.clone(), rdf_type.clone());
        let flakes = db
            .range(fluree_db_core::IndexType::Spot, RangeTest::Eq, range_match)
            .await
            .map_err(|e| PolicyError::ClassLookup {
                message: format!("Failed to look up classes for subject: {e}"),
            })?;

        let mut classes: Vec<Sid> = flakes
            .into_iter()
            .filter_map(|f| match f.o {
                FlakeValue::Ref(class_sid) => Some(class_sid),
                _ => None,
            })
            .collect();
        classes.sort();
        classes.dedup();
        if !classes.is_empty() {
            result.insert(subject.clone(), classes);
        }
    }

    Ok(result)
}

/// Look up classes for subjects and populate the policy context's class cache.
///
/// This is a convenience function that combines lookup with cache population.
///
/// # Arguments
///
/// * `subjects` - The subject SIDs to look up classes for
/// * `db` - Database reference bundling snapshot, graph id, overlay, and time
/// * `policy_ctx` - The policy context whose cache to populate
pub async fn populate_class_cache(
    subjects: &[Sid],
    db: GraphDbRef<'_>,
    policy_ctx: &crate::evaluate::PolicyContext,
) -> Result<()> {
    // Skip if no class policies need checking
    if !policy_ctx.wrapper().has_class_policies() {
        return Ok(());
    }

    let class_map = lookup_subject_classes(subjects, db).await?;

    for (subject, classes) in class_map {
        policy_ctx.cache_subject_classes(subject, classes);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdf_type_sid_creation() {
        let rdf_type = Sid::new(RDF, RDF_TYPE);
        assert_eq!(rdf_type.namespace_code, RDF);
        assert_eq!(rdf_type.name.as_ref(), "type");
    }
}
