//! Schema-bundle overlay that projects whitelisted ontology flakes into `g_id=0`.
//!
//! # Purpose
//!
//! When reasoning is configured with an `f:schemaSource` (plus, optionally, a
//! transitive `owl:imports` closure), RDFS/OWL extraction needs to see
//! schema triples regardless of which graph they physically live in.
//!
//! The existing reasoning extraction code only scans the default graph
//! (`g_id=0`). Rather than teach every reasoner to scan multiple graphs,
//! this module materializes the **relevant** triples from each source graph
//! up-front and exposes them via an `OverlayProvider` that answers queries
//! against `g_id=0`. The reasoner then runs unchanged.
//!
//! # What gets projected
//!
//! Only triples matching a narrow schema/ontology whitelist are projected.
//! Instance data from an imported graph never leaks into the reasoning view:
//!
//! - `rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdfs:domain`, `rdfs:range`
//! - `owl:inverseOf`, `owl:equivalentClass`, `owl:equivalentProperty`,
//!   `owl:sameAs`, `owl:imports`
//! - `rdf:type` **when the object is** one of
//!   `owl:Class`, `owl:ObjectProperty`, `owl:DatatypeProperty`,
//!   `owl:SymmetricProperty`, `owl:TransitiveProperty`,
//!   `owl:FunctionalProperty`, `owl:InverseFunctionalProperty`,
//!   `owl:Ontology`, `rdf:Property`.
//!
//! See `fluree_db_core::{is_schema_predicate, is_schema_class}` for the
//! canonical whitelist used at projection time.
//!
//! # Composition
//!
//! [`SchemaBundleOverlay`] wraps a base overlay (the query's novelty). For
//! `g_id != 0` it delegates straight to the base. For `g_id == 0` it emits
//! the merge of base flakes and the projected schema flakes in index order.
//!
//! The overlay is trivially cheap: the source flakes have already been read
//! and sorted by [`build_schema_bundle_flakes`]; serving them is just a slice
//! walk.

use std::sync::Arc;

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::ids::GraphId;
use fluree_db_core::overlay::OverlayProvider;
use fluree_db_core::range::range_with_overlay;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{
    is_rdf_type, is_schema_class, is_schema_predicate, LedgerSnapshot, RangeMatch, RangeOptions,
    RangeTest,
};

use crate::error::Result;

/// Pre-sorted schema flakes projected from a set of source graphs to `g_id=0`.
///
/// Produced by [`build_schema_bundle_flakes`] once per query (or once per
/// cached bundle) and wrapped by [`SchemaBundleOverlay`] at overlay time.
#[derive(Debug, Clone)]
pub struct SchemaBundleFlakes {
    spot: Arc<[Flake]>,
    psot: Arc<[Flake]>,
    post: Arc<[Flake]>,
    opst: Arc<[Flake]>,
    /// Stable identifier for cache composition with other overlays.
    epoch: u64,
}

impl SchemaBundleFlakes {
    /// Empty bundle — the overlay will pass through to its base.
    pub fn empty() -> Self {
        Self {
            spot: Arc::from([]),
            psot: Arc::from([]),
            post: Arc::from([]),
            opst: Arc::from([]),
            epoch: 0,
        }
    }

    /// Build a bundle from a pre-collected flake list. Shared by
    /// the same-ledger ([`build_schema_bundle_flakes`]) and
    /// cross-ledger (the API crate's `SchemaArtifactWire::translate_to_schema_bundle_flakes`)
    /// paths so both apply the same sort + dedupe discipline and
    /// produce the same four index orderings.
    ///
    /// The caller is responsible for whitelist correctness — both
    /// callers project only whitelist-matching triples before
    /// invoking this helper. The function does not re-validate.
    pub fn from_collected_schema_triples(mut collected: Vec<Flake>) -> Result<Self> {
        if collected.is_empty() {
            return Ok(Self::empty());
        }

        collected.sort_by(|a, b| {
            a.s.cmp(&b.s)
                .then_with(|| a.p.cmp(&b.p))
                .then_with(|| a.o.cmp(&b.o))
                .then_with(|| a.t.cmp(&b.t))
                .then_with(|| a.op.cmp(&b.op))
        });
        collected
            .dedup_by(|a, b| a.s == b.s && a.p == b.p && a.o == b.o && a.t == b.t && a.op == b.op);

        let mut spot = collected.clone();
        let mut psot = collected.clone();
        let mut post = collected.clone();
        let mut opst = collected;

        spot.sort_by(IndexType::Spot.comparator());
        psot.sort_by(IndexType::Psot.comparator());
        post.sort_by(IndexType::Post.comparator());
        opst.sort_by(IndexType::Opst.comparator());

        // Epoch is just the flake count for cross-ledger; same-ledger
        // mixes in the source graph ids via the existing
        // build_schema_bundle_flakes path.
        let epoch: u64 = spot.len() as u64;

        Ok(Self {
            spot: spot.into(),
            psot: psot.into(),
            post: post.into(),
            opst: opst.into(),
            epoch,
        })
    }

    /// Total number of projected flakes (same across all index orderings).
    pub fn len(&self) -> usize {
        self.spot.len()
    }

    /// Whether the bundle projected zero flakes.
    pub fn is_empty(&self) -> bool {
        self.spot.is_empty()
    }

    /// Epoch value used for `OverlayProvider::epoch` composition.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return the projected flakes as a flat `Vec`, suitable for
    /// feeding back into [`Self::from_collected_schema_triples`]
    /// (e.g., to merge with another bundle). Uses the SPOT
    /// ordering as the canonical source — sort + dedupe + index
    /// rebuild happens inside `from_collected_schema_triples`.
    pub fn flakes_for_merge(&self) -> Vec<Flake> {
        self.spot.iter().cloned().collect()
    }

    fn flakes(&self, index: IndexType) -> &[Flake] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    fn upper_bound(&self, index: IndexType, target: &Flake) -> usize {
        let flakes = self.flakes(index);
        let cmp = index.comparator();
        flakes.partition_point(|f| cmp(f, target).is_le())
    }
}

/// Read whitelisted schema flakes from each source graph at `to_t`.
///
/// For each `g_id` in `sources`, runs targeted reads (one per schema
/// predicate, one per schema class) against the base overlay. Flakes from
/// `g_id == 0` are intentionally skipped — they are already visible to the
/// reasoner via the default-graph path and should not be double-counted.
///
/// Results are aggregated, deduplicated by `(s, p, o, t, op)`, and sorted
/// into the four index orderings expected by `OverlayProvider`.
pub async fn build_schema_bundle_flakes<O>(
    snapshot: &LedgerSnapshot,
    base_overlay: &O,
    to_t: i64,
    sources: &[GraphId],
) -> Result<SchemaBundleFlakes>
where
    O: OverlayProvider + ?Sized,
{
    use fluree_vocab::owl;
    use fluree_vocab::rdf;
    use fluree_vocab::rdfs;

    // Predicates whose flakes are projected wholesale.
    let schema_predicate_iris: &[&str] = &[
        rdfs::SUB_CLASS_OF,
        rdfs::SUB_PROPERTY_OF,
        rdfs::DOMAIN,
        rdfs::RANGE,
        owl::INVERSE_OF,
        owl::EQUIVALENT_CLASS,
        owl::EQUIVALENT_PROPERTY,
        owl::SAME_AS,
        owl::IMPORTS,
    ];
    // Classes whose `rdf:type` triples are projected (i.e. subject is
    // declared as an instance of the class).
    let schema_class_iris: &[&str] = &[
        owl::CLASS,
        owl::OBJECT_PROPERTY,
        owl::DATATYPE_PROPERTY,
        owl::SYMMETRIC_PROPERTY,
        owl::TRANSITIVE_PROPERTY,
        owl::FUNCTIONAL_PROPERTY,
        owl::INVERSE_FUNCTIONAL_PROPERTY,
        owl::ONTOLOGY,
        rdf::PROPERTY,
    ];

    // Pre-encode whitelist Sids; missing entries (namespace/name never seen in
    // this ledger) simply contribute nothing.
    let schema_predicates: Vec<_> = schema_predicate_iris
        .iter()
        .filter_map(|iri| snapshot.encode_iri(iri))
        .collect();
    let schema_classes: Vec<_> = schema_class_iris
        .iter()
        .filter_map(|iri| snapshot.encode_iri(iri))
        .collect();
    let rdf_type_sid = snapshot.encode_iri(rdf::TYPE);

    let opts = RangeOptions::default().with_to_t(to_t);

    let mut collected: Vec<Flake> = Vec::new();

    for &g_id in sources {
        if g_id == 0 {
            // The query already sees g_id=0 via its own overlay; skip.
            continue;
        }

        // Per-predicate PSOT scans for hierarchy/OWL axioms.
        for p in &schema_predicates {
            let flakes = range_with_overlay(
                snapshot,
                g_id,
                base_overlay,
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(p.clone()),
                opts.clone(),
            )
            .await?;
            for f in flakes {
                if is_schema_predicate(&f.p) {
                    collected.push(f);
                }
            }
        }

        // Per-class OPST scans for `?s rdf:type <class>` axioms.
        if let Some(rdf_type) = rdf_type_sid.clone() {
            for cls in &schema_classes {
                let flakes = range_with_overlay(
                    snapshot,
                    g_id,
                    base_overlay,
                    IndexType::Opst,
                    RangeTest::Eq,
                    RangeMatch {
                        p: Some(rdf_type.clone()),
                        o: Some(FlakeValue::Ref(cls.clone())),
                        ..Default::default()
                    },
                    opts.clone(),
                )
                .await?;
                for f in flakes {
                    // Defense in depth: confirm the filter matched our
                    // whitelist before projecting.
                    if !is_rdf_type(&f.p) {
                        continue;
                    }
                    let FlakeValue::Ref(obj) = &f.o else { continue };
                    if is_schema_class(obj) {
                        collected.push(f);
                    }
                }
            }
        }
    }

    if collected.is_empty() {
        return Ok(SchemaBundleFlakes::empty());
    }

    // Deduplicate — the same axiom triple can appear across multiple source
    // graphs when an ontology is re-imported transitively. `Flake` isn't
    // hashable, so dedupe via sort+dedup over a normalizing comparator.
    collected.sort_by(|a, b| {
        a.s.cmp(&b.s)
            .then_with(|| a.p.cmp(&b.p))
            .then_with(|| a.o.cmp(&b.o))
            .then_with(|| a.t.cmp(&b.t))
            .then_with(|| a.op.cmp(&b.op))
    });
    collected.dedup_by(|a, b| a.s == b.s && a.p == b.p && a.o == b.o && a.t == b.t && a.op == b.op);

    let mut spot = collected.clone();
    let mut psot = collected.clone();
    let mut post = collected.clone();
    let mut opst = collected;

    let spot_cmp = IndexType::Spot.comparator();
    let psot_cmp = IndexType::Psot.comparator();
    let post_cmp = IndexType::Post.comparator();
    let opst_cmp = IndexType::Opst.comparator();

    spot.sort_by(spot_cmp);
    psot.sort_by(psot_cmp);
    post.sort_by(post_cmp);
    opst.sort_by(opst_cmp);

    // Epoch combines (source graph ids, flake count) so the overlay's
    // composed epoch is stable for caching but changes when the bundle
    // materially differs.
    let mut epoch: u64 = spot.len() as u64;
    for &g in sources {
        epoch = epoch.wrapping_mul(31).wrapping_add(u64::from(g));
    }

    Ok(SchemaBundleFlakes {
        spot: spot.into(),
        psot: psot.into(),
        post: post.into(),
        opst: opst.into(),
        epoch,
    })
}

/// Overlay that exposes [`SchemaBundleFlakes`] at `g_id=0`, composed with a base overlay.
///
/// For `g_id != 0`, delegates to the base overlay unchanged — imports never
/// surface on other graphs' overlay reads.
pub struct SchemaBundleOverlay<'a> {
    base: &'a dyn OverlayProvider,
    bundle: Arc<SchemaBundleFlakes>,
    epoch: u64,
}

impl<'a> SchemaBundleOverlay<'a> {
    /// Compose a base overlay with a projected schema bundle.
    pub fn new(base: &'a dyn OverlayProvider, bundle: Arc<SchemaBundleFlakes>) -> Self {
        let epoch = base
            .epoch()
            .wrapping_mul(1_000_003)
            .wrapping_add(bundle.epoch());
        Self {
            base,
            bundle,
            epoch,
        }
    }
}

impl OverlayProvider for SchemaBundleOverlay<'_> {
    fn as_any(&self) -> &dyn std::any::Any {
        self.base.as_any()
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn for_each_overlay_flake(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        if g_id != 0 || self.bundle.is_empty() {
            self.base
                .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, callback);
            return;
        }

        // Slice the bundle to the requested sub-range in index order. Do this
        // before touching the base so we can skip buffering base flakes
        // entirely when the scan range misses the bundle — the common case
        // for queries that scan arbitrary ranges unrelated to schema triples.
        let flakes = self.bundle.flakes(index);
        let start = if leftmost {
            0
        } else if let Some(first_flake) = first {
            self.bundle.upper_bound(index, first_flake)
        } else {
            0
        };
        let end = if let Some(rhs_flake) = rhs {
            self.bundle.upper_bound(index, rhs_flake)
        } else {
            flakes.len()
        };
        if start >= end {
            self.base
                .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, callback);
            return;
        }

        // Bundle intersects the range — collect base flakes and linear-merge.
        let mut base_flakes: Vec<Flake> = Vec::new();
        self.base
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, &mut |f| {
                base_flakes.push(f.clone());
            });

        let mut bundle_iter = flakes[start..end].iter().filter(|f| f.t <= to_t).peekable();
        let mut base_iter = base_flakes.iter().peekable();

        loop {
            match (base_iter.peek(), bundle_iter.peek()) {
                (Some(b), Some(s)) => {
                    if index.compare(b, s).is_le() {
                        callback(base_iter.next().unwrap());
                    } else {
                        callback(bundle_iter.next().unwrap());
                    }
                }
                (Some(_), None) => callback(base_iter.next().unwrap()),
                (None, Some(_)) => callback(bundle_iter.next().unwrap()),
                (None, None) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::NoOverlay;

    #[test]
    fn empty_bundle_delegates_to_base() {
        let bundle = Arc::new(SchemaBundleFlakes::empty());
        let base = NoOverlay;
        let overlay = SchemaBundleOverlay::new(&base, bundle);

        let mut count = 0;
        overlay.for_each_overlay_flake(0, IndexType::Psot, None, None, true, i64::MAX, &mut |_| {
            count += 1;
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn non_default_graph_passes_through_to_base() {
        // Even when the bundle has projected flakes, a non-zero g_id query
        // must not see them — only g_id=0 receives the projection.
        let bundle = Arc::new(SchemaBundleFlakes::empty());
        let base = NoOverlay;
        let overlay = SchemaBundleOverlay::new(&base, bundle);

        let mut count = 0;
        overlay.for_each_overlay_flake(5, IndexType::Psot, None, None, true, i64::MAX, &mut |_| {
            count += 1;
        });
        assert_eq!(count, 0);
    }
}
