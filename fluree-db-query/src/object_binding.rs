use crate::binding::Binding;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::dict_novelty::{DictNovelty, StringDictNovelty, SubjectDictNovelty};
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_vocab::xsd_names;
use std::sync::{Arc, OnceLock};

fn encoded_i_val(o_i: u32) -> i32 {
    if o_i == u32::MAX {
        i32::MIN
    } else {
        o_i as i32
    }
}

/// Encoded representation for an inline numeric o_type, or `None` if the type
/// has no well-known `dt_id` and must stay on the materialized path.
///
/// Returns `(o_kind, dt_id)` for `EncodedLit`. The `dt_id` is the well-known
/// `DatatypeDictId` whose registry slot maps back to exactly this o_type
/// (`resolve(o_kind, dt_id, 0) == o_type`), so `DATATYPE()` and terminal
/// materialization reconstruct the correct datatype. Restricted to the four
/// numeric types with reserved dict ids — xsd:int / xsd:short / etc. have no
/// well-known id and fall through to materialization unchanged.
fn inline_numeric_encoding(o_type: u16) -> Option<(u8, u16)> {
    let ot = OType::from_u16(o_type);
    if ot == OType::XSD_INTEGER {
        Some((ObjKind::NUM_INT.as_u8(), DatatypeDictId::INTEGER.as_u16()))
    } else if ot == OType::XSD_LONG {
        Some((ObjKind::NUM_INT.as_u8(), DatatypeDictId::LONG.as_u16()))
    } else if ot == OType::XSD_DOUBLE {
        Some((ObjKind::NUM_F64.as_u8(), DatatypeDictId::DOUBLE.as_u16()))
    } else if ot == OType::XSD_FLOAT {
        Some((ObjKind::NUM_F64.as_u8(), DatatypeDictId::FLOAT.as_u16()))
    } else {
        None
    }
}

/// Encoded representation for embedded temporal `OType`s whose `o_key` is already
/// order-preserving and whose datatype has a stable dictionary id.
fn embedded_temporal_encoding(o_type: u16) -> Option<(u8, u16)> {
    let ot = OType::from_u16(o_type);
    if ot == OType::XSD_DATE {
        Some((ObjKind::DATE.as_u8(), DatatypeDictId::DATE.as_u16()))
    } else if ot == OType::XSD_TIME {
        Some((ObjKind::TIME.as_u8(), DatatypeDictId::TIME.as_u16()))
    } else if ot == OType::XSD_DATE_TIME {
        Some((
            ObjKind::DATE_TIME.as_u8(),
            DatatypeDictId::DATE_TIME.as_u16(),
        ))
    } else {
        None
    }
}

/// Build an `EncodedLit` for an inline numeric, or `None` for types that must
/// stay materialized (see [`inline_numeric_encoding`]).
pub(crate) fn inline_numeric_encoded_lit(
    o_type: u16,
    o_key: u64,
    p_id: u32,
    o_i: u32,
    t: i64,
) -> Option<Binding> {
    inline_numeric_encoding(o_type).map(|(o_kind, dt_id)| Binding::EncodedLit {
        o_kind,
        o_key,
        p_id,
        dt_id,
        lang_id: 0,
        i_val: encoded_i_val(o_i),
        t,
    })
}

/// Build a late-materialized object binding for the binary scan path.
///
/// `op` is `Some(true|false)` only in history mode (assert/retract) — it
/// then flows onto ref-valued bindings (`EncodedSid` / blank-node `Sid`)
/// alongside `t`, mirroring how literal-valued objects already carry the
/// metadata. Callers outside history mode pass `None`.
pub(crate) fn late_materialized_object_binding(
    o_type: u16,
    o_key: u64,
    p_id: u32,
    t: i64,
    o_i: u32,
    op: Option<bool>,
) -> Option<Binding> {
    let ot = OType::from_u16(o_type);
    match ot.decode_kind() {
        DecodeKind::IriRef => Some(Binding::EncodedSid {
            s_id: o_key,
            t: Some(t),
            op,
        }),
        DecodeKind::BlankNode => Some(Binding::Sid {
            sid: Sid::new(0, format!("_:b{o_key}")),
            t: Some(t),
            op,
        }),
        // Every string-dictionary datatype shares one `o_key` — the interned
        // lexical form — so the datatype is the whole of a string literal's
        // term identity, and `EncodedLit` can only carry it as a
        // `DatatypeDictId`. Only `xsd:string`, `rdf:langString` and
        // `@fulltext` have a reserved one. The other XSD string subtypes
        // (`xsd:anyURI`, `xsd:token`, `xsd:normalizedString`, `xsd:language`,
        // `xsd:base64Binary`, `xsd:hexBinary`) and every customer-defined
        // datatype are numbered per ledger, so encoding them meant borrowing
        // `xsd:string`'s id — which made `"abc"`, `"abc"^^xsd:anyURI` and
        // `"abc"^^ex:custom` one term (#1729). They stay materialized instead
        // and carry their exact datatype `Sid` on `Binding::Lit`, the same
        // rule the temporal subtypes below already follow.
        DecodeKind::StringDict => {
            let (dt_id, lang_id) = if ot.is_lang_string() {
                (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
            } else if ot == OType::FULLTEXT {
                (DatatypeDictId::FULL_TEXT.as_u16(), 0)
            } else if ot == OType::XSD_STRING {
                (DatatypeDictId::STRING.as_u16(), 0)
            } else {
                return None;
            };
            Some(Binding::EncodedLit {
                o_kind: ObjKind::LEX_ID.as_u8(),
                o_key,
                p_id,
                dt_id,
                lang_id,
                i_val: encoded_i_val(o_i),
                t,
            })
        }
        DecodeKind::JsonArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::JSON_ID.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::JSON.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        DecodeKind::VectorArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::VECTOR_ID.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::VECTOR.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        DecodeKind::NumBigArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::NUM_BIG.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::DECIMAL.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        // Inline integer/float values whose datatype has a reserved dict id:
        // keep them encoded so they hash/compare/clone as cheap ints through
        // DISTINCT and joins, with materialization deferred to projection.
        DecodeKind::I64 | DecodeKind::F64 => {
            inline_numeric_encoded_lit(o_type, o_key, p_id, o_i, t)
        }
        // Embedded temporal values are also order-preserving `o_key`s. Keep them
        // late-materialized so cyclic/path joins do not decode and re-intern them
        // for every intermediate row. Only date/time/dateTime have reserved
        // datatype dictionary ids today; the other temporal subtypes stay
        // materialized until their datatype ids are represented in EncodedLit.
        DecodeKind::Date | DecodeKind::Time | DecodeKind::DateTime => {
            embedded_temporal_encoding(o_type).map(|(o_kind, dt_id)| Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id: 0,
                i_val: encoded_i_val(o_i),
                t,
            })
        }
        _ => None,
    }
}

/// Convert a decoded binding to the encoded form the late-materialized scan
/// path emits for the same value (the inverse of
/// [`late_materialized_object_binding`]).
///
/// Equality/hash surfaces (DISTINCT, GROUP BY keys, MINUS, COUNT(DISTINCT))
/// compare `Binding`s structurally, and `Sid`/`Lit` never equal
/// `EncodedSid`/`EncodedLit` — so a stream mixing scan output with decoded
/// producers (VALUES, UNION branches, BIND) silently overcounts or fails to
/// match. Normalizing the decoded minority to encoded form keeps those
/// surfaces hashing cheap raw IDs.
///
/// One IRI has one canonical form here, whichever form it arrives in: the
/// scan encodes an IRI as `EncodedPid` in predicate position and as
/// `EncodedSid` in subject or object position, and a decoded producer carries
/// it as `Sid`/`Iri`. The canonical form is [`canonical_iri_id`]'s. Blank
/// nodes follow the same rule: the index stores a blank node as a subject,
/// so its decoded `Sid` resolves to the id its encoded form carries.
///
/// Returns `None` when the binding is already canonical or has no encoded
/// equivalent: neither of [`TermDicts`]' dictionaries holds the value, or its
/// datatype stays materialized on every lane. That is sound because every
/// lane, a batched one with novelty pending included, assigns ids from those
/// two dictionaries in the same order, so a value neither holds has no
/// encoded form anywhere.
///
/// The encoded identity fields are `(o_kind, o_key, dt_id, lang_id)` —
/// `i_val`/`t`/`op` are metadata excluded from `PartialEq`/`Hash`, and `p_id`
/// only participates for NUM_BIG (which this never produces).
pub(crate) fn encoded_equivalent(binding: &Binding, dicts: TermDicts<'_>) -> Option<Binding> {
    match binding {
        Binding::Sid { t, op, .. } => Some(canonical_iri_id(binding, dicts)?.into_binding(*t, *op)),
        Binding::Iri(_) | Binding::IriMatch { .. } => {
            Some(canonical_iri_id(binding, dicts)?.into_binding(None, None))
        }
        Binding::EncodedPid { .. } => encoded_iri_canonical(binding, dicts).flatten(),
        Binding::Lit {
            val,
            dtc,
            t,
            op: _,
            p_id,
        } => {
            let (o_kind, o_key, dt_id, lang_id) = match (val, dtc) {
                (FlakeValue::String(s), DatatypeConstraint::LangTag(tag)) => {
                    let str_id = dicts.string_id(s)?;
                    // A language tag first seen in novelty never reaches an
                    // encoded binding (the overlay keeps it raw), so the
                    // persisted table is the whole of this lookup.
                    let lang_id = dicts.store.find_lang_id(tag)?;
                    (
                        ObjKind::LEX_ID.as_u8(),
                        u64::from(str_id),
                        DatatypeDictId::LANG_STRING.as_u16(),
                        lang_id,
                    )
                }
                (FlakeValue::String(s), DatatypeConstraint::Explicit(dt)) => {
                    let dt_id = if is_xsd(dt, xsd_names::STRING) {
                        DatatypeDictId::STRING.as_u16()
                    } else if dt.namespace_code == fluree_vocab::namespaces::FLUREE_DB
                        && dt.name.as_ref() == "fullText"
                    {
                        DatatypeDictId::FULL_TEXT.as_u16()
                    } else {
                        return None;
                    };
                    let str_id = dicts.string_id(s)?;
                    (ObjKind::LEX_ID.as_u8(), u64::from(str_id), dt_id, 0)
                }
                // JSON shares the string dictionary, keyed by its serialized text.
                (FlakeValue::Json(s), _) => {
                    let str_id = dicts.string_id(s)?;
                    (
                        ObjKind::JSON_ID.as_u8(),
                        u64::from(str_id),
                        DatatypeDictId::JSON.as_u16(),
                        0,
                    )
                }
                (FlakeValue::Long(v), DatatypeConstraint::Explicit(dt)) => {
                    let dt_id = if is_xsd(dt, xsd_names::INTEGER) {
                        DatatypeDictId::INTEGER.as_u16()
                    } else if is_xsd(dt, xsd_names::LONG) {
                        DatatypeDictId::LONG.as_u16()
                    } else {
                        return None;
                    };
                    (
                        ObjKind::NUM_INT.as_u8(),
                        ObjKey::encode_i64(*v).as_u64(),
                        dt_id,
                        0,
                    )
                }
                (FlakeValue::Double(v), DatatypeConstraint::Explicit(dt)) => {
                    let dt_id = if is_xsd(dt, xsd_names::DOUBLE) {
                        DatatypeDictId::DOUBLE.as_u16()
                    } else if is_xsd(dt, xsd_names::FLOAT) {
                        DatatypeDictId::FLOAT.as_u16()
                    } else {
                        return None;
                    };
                    let key = ObjKey::encode_f64(*v).ok()?;
                    (ObjKind::NUM_F64.as_u8(), key.as_u64(), dt_id, 0)
                }
                // The temporal types `embedded_temporal_encoding` keeps
                // encoded. Their keys are the canonical value itself.
                (FlakeValue::Date(d), DatatypeConstraint::Explicit(dt))
                    if is_xsd(dt, xsd_names::DATE) =>
                {
                    (
                        ObjKind::DATE.as_u8(),
                        ObjKey::encode_date(d.days_since_epoch()).as_u64(),
                        DatatypeDictId::DATE.as_u16(),
                        0,
                    )
                }
                (FlakeValue::Time(t), DatatypeConstraint::Explicit(dt))
                    if is_xsd(dt, xsd_names::TIME) =>
                {
                    (
                        ObjKind::TIME.as_u8(),
                        ObjKey::encode_time(t.micros_since_midnight()).as_u64(),
                        DatatypeDictId::TIME.as_u16(),
                        0,
                    )
                }
                (FlakeValue::DateTime(dt_val), DatatypeConstraint::Explicit(dt))
                    if is_xsd(dt, xsd_names::DATE_TIME) =>
                {
                    (
                        ObjKind::DATE_TIME.as_u8(),
                        ObjKey::encode_datetime(dt_val.epoch_micros()).as_u64(),
                        DatatypeDictId::DATE_TIME.as_u16(),
                        0,
                    )
                }
                _ => return None,
            };
            Some(Binding::EncodedLit {
                o_kind,
                o_key,
                p_id: p_id.unwrap_or(0),
                dt_id,
                lang_id,
                i_val: i32::MIN,
                t: t.unwrap_or(0),
            })
        }
        _ => None,
    }
}

/// One store's term dictionaries as a query reads them: the persisted index
/// and the novelty dictionary layered over it.
///
/// Every id a query binds comes from these two, and every lane resolves a
/// term through them in one order, the persisted dictionary first, so a term
/// has one id whichever lane met it. That includes the batched lanes, which
/// bind a term minted since the last index by its novelty id while a plain
/// scan, with novelty pending, decodes the same term: an equality surface
/// that resolved decoded terms through the persisted dictionary alone would
/// key the two apart. The batched lanes' key lookups and every equality
/// surface resolve a decoded term through this; the scan's overlay
/// translation keeps the same order on its own path.
#[derive(Clone, Copy)]
pub(crate) struct TermDicts<'a> {
    store: &'a BinaryIndexStore,
    /// Present only when initialized and non-empty, so a query with nothing
    /// pending never probes it.
    novel_subjects: Option<&'a SubjectDictNovelty>,
    novel_strings: Option<&'a StringDictNovelty>,
    /// Per persisted predicate, whether its IRI is a subject minted in
    /// novelty, each found on the predicate's first key for the holder's
    /// lifetime ([`EqualityNorm`]) instead of a novelty probe per key.
    /// `None`: probe per key.
    novel_predicate_subjects: Option<&'a NovelPredicateSubjects>,
}

/// [`TermDicts`]' per-predicate memo of novelty subject ids: one slot per
/// persisted predicate, allocated on the first predicate key that needs it
/// and filled one predicate at a time, so a query probes the novelty
/// dictionary once per predicate it meets, never once per predicate the
/// index holds.
#[derive(Default)]
pub(crate) struct NovelPredicateSubjects(OnceLock<Box<[std::sync::atomic::AtomicU64]>>);

impl NovelPredicateSubjects {
    /// A slot not yet resolved.
    const UNRESOLVED: u64 = u64::MAX;
    /// A predicate whose IRI is no novelty subject.
    const NONE: u64 = u64::MAX - 1;

    fn get(
        &self,
        store: &BinaryIndexStore,
        subjects: &SubjectDictNovelty,
        p_id: u32,
    ) -> Option<u64> {
        use std::sync::atomic::{AtomicU64, Ordering};
        let slots = self.0.get_or_init(|| {
            (0..store.p_sid_table().len())
                .map(|_| AtomicU64::new(Self::UNRESOLVED))
                .collect()
        });
        let slot = slots.get(p_id as usize)?;
        match slot.load(Ordering::Relaxed) {
            Self::NONE => None,
            Self::UNRESOLVED => {
                let sid = &store.p_sid_table()[p_id as usize];
                let found = subjects.find_subject(sid.namespace_code, &sid.name);
                match found {
                    None => slot.store(Self::NONE, Ordering::Relaxed),
                    // A subject id sits below both sentinels; one that does
                    // not is answered unmemoized.
                    Some(s_id) if s_id < Self::NONE => slot.store(s_id, Ordering::Relaxed),
                    Some(_) => {}
                }
                found
            }
            s_id => Some(s_id),
        }
    }
}

impl<'a> TermDicts<'a> {
    pub(crate) fn new(store: &'a BinaryIndexStore, novelty: Option<&'a DictNovelty>) -> Self {
        let (subjects, strings) = novelty_layers(novelty);
        Self::from_layers(store, novelty, subjects, strings)
    }

    /// With which novelty layers hold entries already known
    /// ([`novelty_layers`]), so a caller that builds this per row does not
    /// walk the layer chain each time.
    #[inline]
    fn from_layers(
        store: &'a BinaryIndexStore,
        novelty: Option<&'a DictNovelty>,
        subjects: bool,
        strings: bool,
    ) -> Self {
        Self {
            store,
            novel_subjects: novelty.filter(|_| subjects).map(|dn| &dn.subjects),
            novel_strings: novelty.filter(|_| strings).map(|dn| &dn.strings),
            novel_predicate_subjects: None,
        }
    }

    /// The execution context's dictionaries, for the lanes that resolve terms
    /// to ids. Equality surfaces use [`EqualityNorm`], which also declines
    /// cross-ledger execution.
    pub(crate) fn of(ctx: &'a crate::context::ExecutionContext<'_>) -> Option<Self> {
        Some(Self::new(
            ctx.binary_store.as_deref()?,
            ctx.dict_novelty.as_deref(),
        ))
    }

    /// The dictionaries a novelty-aware graph view resolves through.
    pub(crate) fn of_view(view: &'a fluree_db_binary_index::BinaryGraphView) -> Self {
        Self::new(view.store(), view.dict_novelty().map(|dn| &**dn))
    }

    /// The subject id of `(ns_code, name)`: the persisted dictionary's, else
    /// the novelty dictionary's.
    #[inline]
    pub(crate) fn subject_id(&self, ns_code: u16, name: &str) -> std::io::Result<Option<u64>> {
        if let Some(s_id) = self.store.find_subject_id_by_parts(ns_code, name)? {
            return Ok(Some(s_id));
        }
        Ok(self.novel_subject_id(ns_code, name))
    }

    #[inline]
    fn novel_subject_id(&self, ns_code: u16, name: &str) -> Option<u64> {
        self.novel_subjects?.find_subject(ns_code, name)
    }

    /// The string-dictionary id of `value`, persisted first, then novelty.
    /// `None` also when the dictionary cannot be read.
    #[inline]
    fn string_id(&self, value: &str) -> Option<u32> {
        match self.store.find_string_id(value) {
            Ok(Some(str_id)) => Some(str_id),
            Ok(None) => self.novel_strings?.find_string(value),
            Err(_) => None,
        }
    }

    /// [`IriId`] of persisted predicate `p_id`'s IRI.
    #[inline]
    fn predicate_iri_id(&self, p_id: u32) -> IriId {
        if let Some(s_id) = self.store.predicate_subject_id(p_id) {
            return IriId::Subject(s_id);
        }
        // A predicate IRI first used as a subject or ref object since the
        // last index has a novelty subject id.
        if let Some(subjects) = self.novel_subjects {
            let novel = match self.novel_predicate_subjects {
                Some(memo) => memo.get(self.store, subjects, p_id),
                None => self
                    .store
                    .p_sid_table()
                    .get(p_id as usize)
                    .and_then(|sid| subjects.find_subject(sid.namespace_code, &sid.name)),
            };
            if let Some(s_id) = novel {
                return IriId::Subject(s_id);
            }
        }
        IriId::Predicate(p_id)
    }

    /// [`IriId`] of a decoded IRI. The predicate table answers first: an IRI
    /// outside every predicate's namespace, or of a name length no predicate
    /// there has, is rejected without hashing, and a predicate then costs a
    /// load instead of a subject-dictionary lookup. A predicate's subject id,
    /// when it has one, still wins ([`Self::predicate_iri_id`]).
    #[inline]
    fn decoded_iri_id(&self, sid: &Sid) -> Option<IriId> {
        if let Some(p_id) = self.store.predicate_id_for_sid(sid) {
            return Some(self.predicate_iri_id(p_id));
        }
        match self
            .store
            .find_subject_id_by_parts(sid.namespace_code, &sid.name)
        {
            Ok(Some(s_id)) => Some(IriId::Subject(s_id)),
            _ => self
                .novel_subject_id(sid.namespace_code, &sid.name)
                .map(IriId::Subject),
        }
    }
}

/// Whether an initialized novelty dictionary holds subjects, and strings.
fn novelty_layers(novelty: Option<&DictNovelty>) -> (bool, bool) {
    match novelty.filter(|dn| dn.is_initialized()) {
        Some(dn) => (!dn.subjects.is_empty(), !dn.strings.is_empty()),
        None => (false, false),
    }
}

/// The id an IRI canonically keys by in one store: its subject id when the
/// IRI has one, persisted or novelty, else its persisted predicate id.
///
/// The scan encodes one IRI as `EncodedPid` where a pattern reaches it as a
/// predicate and as `EncodedSid` where it is a subject or object, and decoded
/// producers (VALUES, BIND, scans with novelty pending) carry it as
/// `Sid`/`Iri`. Every equality surface that keys IRIs by id goes through
/// this, so the forms of one IRI key alike and no surface carries its own
/// copy of the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IriId {
    Subject(u64),
    Predicate(u32),
}

impl IriId {
    #[inline]
    fn into_binding(self, t: Option<i64>, op: Option<bool>) -> Binding {
        match self {
            IriId::Subject(s_id) => Binding::EncodedSid { s_id, t, op },
            IriId::Predicate(p_id) => Binding::EncodedPid { p_id },
        }
    }
}

/// The canonical form of an encoded IRI, without the decoded arms of
/// [`encoded_equivalent`]: `Some(Some(b))` replaces the binding with `b`,
/// `Some(None)` keeps it, `None` when the binding is not an encoded IRI. The
/// keyed surfaces ask this first, so an all-`EncodedSid` key returns at once
/// and an `EncodedPid` key costs the store's per-predicate load.
#[inline]
pub(crate) fn encoded_iri_canonical(
    binding: &Binding,
    dicts: TermDicts<'_>,
) -> Option<Option<Binding>> {
    match binding {
        Binding::EncodedSid { .. } => Some(None),
        // Canonical already unless its IRI is also a subject.
        Binding::EncodedPid { p_id } => Some(match dicts.predicate_iri_id(*p_id) {
            id @ IriId::Subject(_) => Some(id.into_binding(None, None)),
            IriId::Predicate(_) => None,
        }),
        _ => None,
    }
}

/// [`IriId`] of an IRI-valued binding; `None` for any other binding, for an
/// IRI no dictionary holds, or when the subject dictionary cannot be read.
/// A blank node resolves like any other subject.
#[inline]
pub(crate) fn canonical_iri_id(binding: &Binding, dicts: TermDicts<'_>) -> Option<IriId> {
    match binding {
        Binding::EncodedSid { s_id, .. } => Some(IriId::Subject(*s_id)),
        Binding::EncodedPid { p_id } => Some(dicts.predicate_iri_id(*p_id)),
        Binding::Sid { sid, .. } => dicts.decoded_iri_id(sid),
        Binding::Iri(iri) | Binding::IriMatch { iri, .. } => {
            dicts.decoded_iri_id(&dicts.store.encode_iri(iri.as_ref()))
        }
        _ => None,
    }
}

fn is_xsd(dt: &Sid, name: &str) -> bool {
    dt.namespace_code == fluree_vocab::namespaces::XSD && dt.name.as_ref() == name
}

/// Store, novelty dictionary and graph view for representation
/// normalization at equality surfaces (DISTINCT, GROUP BY, MINUS, OPTIONAL,
/// semijoin, subquery keys). The dictionaries ([`TermDicts`]) give a decoded
/// binding its encoded form; the graph view decodes arena-backed NUM_BIG
/// values to their canonical numeric form.
///
/// Present only for single-ledger binary execution — the only mode that
/// emits encoded bindings, and the only mode where one store's dictionaries
/// are authoritative for every row. Build it once per operator: the graph
/// view is not free to construct.
pub(crate) struct EqualityNorm {
    store: Arc<BinaryIndexStore>,
    novelty: Option<Arc<DictNovelty>>,
    /// Which novelty layers hold entries ([`novelty_layers`]), read once.
    novel_subjects: bool,
    novel_strings: bool,
    /// See [`TermDicts`]'s field of the same name.
    novel_predicate_subjects: NovelPredicateSubjects,
    gv: Option<fluree_db_binary_index::BinaryGraphView>,
}

impl EqualityNorm {
    #[inline]
    pub(crate) fn dicts(&self) -> TermDicts<'_> {
        TermDicts {
            novel_predicate_subjects: Some(&self.novel_predicate_subjects),
            ..TermDicts::from_layers(
                &self.store,
                self.novelty.as_deref(),
                self.novel_subjects,
                self.novel_strings,
            )
        }
    }

    pub(crate) fn parts(
        norm: &Option<Self>,
    ) -> (
        Option<TermDicts<'_>>,
        Option<&fluree_db_binary_index::BinaryGraphView>,
    ) {
        match norm {
            Some(n) => (Some(n.dicts()), n.gv.as_ref()),
            None => (None, None),
        }
    }
}

/// The dictionaries equality surfaces normalize through, when single-ledger
/// binary execution applies (see [`EqualityNorm`]).
pub(crate) fn equality_dicts<'a>(
    ctx: &'a crate::context::ExecutionContext<'_>,
) -> Option<TermDicts<'a>> {
    if ctx.is_multi_ledger() {
        return None;
    }
    TermDicts::of(ctx)
}

/// Build an [`EqualityNorm`] when single-ledger binary execution applies.
pub(crate) fn equality_norm(ctx: &crate::context::ExecutionContext<'_>) -> Option<EqualityNorm> {
    if ctx.is_multi_ledger() {
        return None;
    }
    let (novel_subjects, novel_strings) = novelty_layers(ctx.dict_novelty.as_deref());
    Some(EqualityNorm {
        store: ctx.binary_store.clone()?,
        novelty: ctx.dict_novelty.clone(),
        novel_subjects,
        novel_strings,
        novel_predicate_subjects: NovelPredicateSubjects::default(),
        gv: ctx.graph_view(),
    })
}

/// Normalize one binding for use in an equality/hash key.
pub(crate) fn normalize_for_key(
    binding: &Binding,
    dicts: Option<TermDicts<'_>>,
    gv: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Binding {
    normalize_for_key_cow(binding, dicts, gv).into_owned()
}

/// [`normalize_for_key`] without the clone: an already-encoded binding (the
/// common case on the indexed scan path) is returned borrowed, so a hot
/// equality surface such as `DISTINCT` can hash and probe a row without
/// copying it and only materializes the key for rows it actually keeps.
pub(crate) fn normalize_for_key_cow<'a>(
    binding: &'a Binding,
    dicts: Option<TermDicts<'_>>,
    gv: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> std::borrow::Cow<'a, Binding> {
    use std::borrow::Cow;
    // A key over an arena handle with no view to decode it would key by a
    // graph-scoped handle; member scans of a union of graphs decode them.
    debug_assert!(
        gv.is_some() || !is_arena_encoded(binding),
        "arena-backed literal keyed with no graph view: {binding:?}"
    );
    if let Some(canonical) = dicts.and_then(|d| encoded_iri_canonical(binding, d)) {
        return match canonical {
            Some(b) => Cow::Owned(b),
            None => Cow::Borrowed(binding),
        };
    }
    // Arena-keyed NUM_BIG values normalize by DECODING: handles are scoped
    // per (graph, predicate), so the encoded form is not a canonical key for
    // one value across predicates or against decoded rows (VALUES, BIND,
    // novelty raw-merge). The decoded BigDecimal/BigInt compares and hashes
    // by numeric value.
    if is_numbig_encoded(binding) {
        if let Some(gv) = gv {
            let materialized = crate::group_aggregate::materialize_encoded(binding, Some(gv));
            if !matches!(materialized, Binding::EncodedLit { .. }) {
                return Cow::Owned(materialized);
            }
        }
        return Cow::Borrowed(binding);
    }
    match dicts.and_then(|d| encoded_equivalent(binding, d)) {
        Some(encoded) => Cow::Owned(encoded),
        None => Cow::Borrowed(binding),
    }
}

/// Whether two bindings are one RDF term, across the forms one term takes:
/// encoded or decoded, and an IRI as `EncodedPid` (reached as a predicate) or
/// `EncodedSid` (a subject or object). `Binding`'s `PartialEq` compares forms
/// structurally and answers `false` across them.
///
/// Equal bindings answer at once, and two bindings of one form other than
/// NUM_BIG literals (whose encoded key is scoped per predicate) are two terms;
/// only a pair of different forms pays for canonicalization, through the same
/// [`normalize_for_key_cow`] the keyed equality surfaces use. `norm` is the
/// caller's, built once ([`equality_norm`]).
///
/// The commonest mixed pair, an encoded subject against a decoded one (a
/// batched lane's row against a scan with novelty pending), compares by
/// decoding the encoded side instead: the graph view memoizes that per
/// subject, where encoding the decoded side costs a dictionary lookup each
/// time.
///
/// An arena-backed literal (a big number or a vector) is only comparable
/// through a graph view: its handle names a value within one graph, so with
/// none this refuses rather than answer either way. Member scans of a union
/// of graphs bind those literals decoded, so no query reaches that refusal.
pub(crate) fn same_term(
    a: &Binding,
    b: &Binding,
    norm: &Option<EqualityNorm>,
) -> crate::error::Result<bool> {
    let gv = norm.as_ref().and_then(|n| n.gv.as_ref());
    if gv.is_none() && (is_arena_encoded(a) || is_arena_encoded(b)) {
        return Err(undecodable_arena_literal(a, b));
    }
    Ok(match one_form_answer(a, b) {
        Some(same) => same,
        None => {
            if let Some(same) = gv.and_then(|gv| encoded_against_decoded_subject(a, b, gv)) {
                return Ok(same);
            }
            let (dicts, gv) = EqualityNorm::parts(norm);
            normalize_for_key_cow(a, dicts, gv) == normalize_for_key_cow(b, dicts, gv)
        }
    })
}

/// The refusal [`same_term`] answers an arena-backed literal with when no
/// graph view can decode it.
#[cold]
fn undecodable_arena_literal(a: &Binding, b: &Binding) -> crate::error::QueryError {
    debug_assert!(
        false,
        "arena-backed literal compared with no graph view: {a:?} vs {b:?}"
    );
    crate::error::QueryError::Internal(format!(
        "cannot compare an arena-backed literal without a graph view to decode it: \
         {a:?} vs {b:?}"
    ))
}

/// [`same_term`] for an `EncodedSid` against a decoded `Sid`, through the
/// view's decode (the one a scan binds the decoded form with). `None` for any
/// other pair, or when the id cannot be decoded.
#[inline]
fn encoded_against_decoded_subject(
    a: &Binding,
    b: &Binding,
    gv: &fluree_db_binary_index::BinaryGraphView,
) -> Option<bool> {
    let (s_id, sid) = match (a, b) {
        (Binding::EncodedSid { s_id, .. }, Binding::Sid { sid, .. })
        | (Binding::Sid { sid, .. }, Binding::EncodedSid { s_id, .. }) => (*s_id, sid),
        _ => return None,
    };
    gv.resolve_subject_sid(s_id)
        .ok()
        .map(|decoded| decoded == *sid)
}

/// [`same_term`] for a caller that holds only the context, such as an inline
/// BIND: a pair that needs no canonicalization answers at once, an IRI or
/// dictionary literal canonicalizes through the context's dictionaries, and
/// an arena-backed literal builds the context's normalization to decode it.
pub(crate) fn same_term_in(
    a: &Binding,
    b: &Binding,
    ctx: Option<&crate::context::ExecutionContext<'_>>,
) -> crate::error::Result<bool> {
    if is_arena_encoded(a) || is_arena_encoded(b) {
        return same_term(a, b, &ctx.and_then(equality_norm));
    }
    Ok(match one_form_answer(a, b) {
        Some(same) => same,
        None => {
            let dicts = ctx.and_then(equality_dicts);
            normalize_for_key_cow(a, dicts, None) == normalize_for_key_cow(b, dicts, None)
        }
    })
}

/// The answer [`same_term`] gives without canonicalizing: `true` for equal
/// bindings, `false` for two of one form (other than NUM_BIG), `None` for a
/// pair of forms that must be canonicalized to compare.
#[inline]
fn one_form_answer(a: &Binding, b: &Binding) -> Option<bool> {
    if a == b {
        return Some(true);
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) && !is_numbig_encoded(a) {
        return Some(false);
    }
    None
}

/// True if this is an encoded literal whose key is a handle into a per-graph,
/// per-predicate arena (a big number or a vector): the handle names a value
/// only within the graph that bound it.
#[inline]
pub(crate) fn is_arena_encoded(binding: &Binding) -> bool {
    matches!(
        binding,
        Binding::EncodedLit { o_kind, .. }
            if *o_kind == fluree_db_core::ObjKind::NUM_BIG.as_u8()
                || *o_kind == fluree_db_core::ObjKind::VECTOR_ID.as_u8()
    )
}

/// True if this is an arena-backed (NUM_BIG) encoded literal.
pub(crate) fn is_numbig_encoded(binding: &Binding) -> bool {
    matches!(
        binding,
        Binding::EncodedLit { o_kind, .. }
            if *o_kind == fluree_db_core::ObjKind::NUM_BIG.as_u8()
    )
}

/// What an arena-backed literal needs to cross between a context and a scope
/// that reads another graph (a GRAPH scope): a handle names its value only
/// within the graph that bound it, so one crossing in either direction is
/// decoded through that graph. Nothing is decoded when both sides read the
/// same single graph, where a handle means the same value on either side.
pub(crate) struct ArenaCrossing {
    /// The outer graph: decodes the handles the seeding row carries in.
    enter: Option<fluree_db_binary_index::BinaryGraphView>,
    /// The scope's graph: decodes the handles its rows carry out.
    leave: Option<fluree_db_binary_index::BinaryGraphView>,
}

impl ArenaCrossing {
    /// The crossing between `outer` and a `scope` derived from it. An outer
    /// context that spans several graphs (a union) has no graph to decode a
    /// handle through, so everything leaving the scope for it is decoded.
    pub(crate) fn between(
        outer: &crate::context::ExecutionContext<'_>,
        scope: &crate::context::ExecutionContext<'_>,
    ) -> Self {
        let same_graph = outer.has_binary_store()
            && scope.has_binary_store()
            && outer.binary_g_id == scope.binary_g_id;
        if same_graph {
            return Self {
                enter: None,
                leave: None,
            };
        }
        Self {
            enter: outer.graph_view(),
            leave: scope.graph_view(),
        }
    }

    /// Decode, through the outer graph, the arena handles in a row that seeds
    /// the scope.
    pub(crate) fn enter(&self, row: &mut [Binding]) {
        let Some(gv) = self.enter.as_ref() else {
            return;
        };
        for binding in row.iter_mut().filter(|b| is_arena_encoded(b)) {
            *binding = crate::group_aggregate::materialize_encoded(binding, Some(gv));
        }
    }

    /// `binding`, bound inside the scope, decoded through the scope's graph
    /// when it is an arena handle.
    pub(crate) fn leave(&self, binding: Binding) -> Binding {
        match &self.leave {
            Some(gv) if is_arena_encoded(&binding) => {
                crate::group_aggregate::materialize_encoded(&binding, Some(gv))
            }
            _ => binding,
        }
    }
}

/// Build a materialized object binding for the binary scan path.
///
/// `op` mirrors the meaning in `late_materialized_object_binding`: it is
/// `Some(...)` only in history mode and is threaded onto the ref- and
/// literal-valued binding alike, so downstream `T(?v)` / `OP(?v)`
/// resolves uniformly across object types.
pub(crate) fn materialized_object_binding(
    store: &BinaryIndexStore,
    o_type: u16,
    p_id: u32,
    val: FlakeValue,
    t: Option<i64>,
    op: Option<bool>,
) -> Binding {
    match val {
        FlakeValue::Ref(sid) => Binding::Sid { sid, t, op },
        other => {
            let dtc = match store.resolve_lang_tag(o_type).map(Arc::from) {
                Some(lang) => DatatypeConstraint::LangTag(lang),
                None => DatatypeConstraint::Explicit(
                    store
                        .resolve_datatype_sid_for_value(o_type, &other)
                        .unwrap_or_else(|| Sid::new(0, "")),
                ),
            };
            Binding::Lit {
                val: other,
                dtc,
                t,
                op,
                p_id: Some(p_id),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_materialized_object_binding_keeps_dates_encoded() {
        let binding = late_materialized_object_binding(
            OType::XSD_DATE.as_u16(),
            12_345,
            7,
            0,
            u32::MAX,
            None,
        )
        .expect("xsd:date should stay encoded");

        assert!(matches!(
            binding,
            Binding::EncodedLit {
                o_kind,
                o_key: 12_345,
                p_id: 7,
                dt_id,
                ..
            } if o_kind == ObjKind::DATE.as_u8()
                && dt_id == DatatypeDictId::DATE.as_u16()
        ));
    }

    #[test]
    fn late_materialized_object_binding_keeps_datetime_encoded() {
        let binding = late_materialized_object_binding(
            OType::XSD_DATE_TIME.as_u16(),
            98_765,
            11,
            0,
            u32::MAX,
            None,
        )
        .expect("xsd:dateTime should stay encoded");

        assert!(matches!(
            binding,
            Binding::EncodedLit {
                o_kind,
                o_key: 98_765,
                p_id: 11,
                dt_id,
                ..
            } if o_kind == ObjKind::DATE_TIME.as_u8()
                && dt_id == DatatypeDictId::DATE_TIME.as_u16()
        ));
    }

    /// The three string-dictionary datatypes with a reserved `DatatypeDictId`
    /// keep their encoded form, and each gets its own id — the encoded triple
    /// is the whole of the term's identity.
    #[test]
    fn late_materialized_object_binding_keeps_reserved_string_datatypes_encoded() {
        for (o_type, want_dt, want_lang) in [
            (OType::XSD_STRING, DatatypeDictId::STRING, 0),
            (OType::FULLTEXT, DatatypeDictId::FULL_TEXT, 0),
            (OType::lang_string(3), DatatypeDictId::LANG_STRING, 3),
        ] {
            let binding =
                late_materialized_object_binding(o_type.as_u16(), 42, 5, 0, u32::MAX, None)
                    .unwrap_or_else(|| panic!("{o_type:?} should stay encoded"));
            assert!(
                matches!(
                    binding,
                    Binding::EncodedLit { o_kind, o_key: 42, dt_id, lang_id, .. }
                        if o_kind == ObjKind::LEX_ID.as_u8()
                            && dt_id == want_dt.as_u16()
                            && lang_id == want_lang
                ),
                "{o_type:?}"
            );
        }
    }

    /// Every other string-dictionary datatype has only a per-ledger id, which
    /// `EncodedLit` cannot carry — encoding one meant borrowing `xsd:string`'s
    /// id and losing the term's identity (#1729). They stay materialized, so
    /// the caller decodes and attaches the exact datatype `Sid`.
    #[test]
    fn late_materialized_object_binding_leaves_other_string_datatypes_materialized() {
        for o_type in [
            OType::XSD_ANY_URI,
            OType::XSD_TOKEN,
            OType::XSD_NORMALIZED_STRING,
            OType::XSD_LANGUAGE,
            OType::XSD_BASE64_BINARY,
            OType::XSD_HEX_BINARY,
            OType::customer_datatype(DatatypeDictId::RESERVED_COUNT),
        ] {
            assert_eq!(
                OType::from_u16(o_type.as_u16()).decode_kind(),
                DecodeKind::StringDict,
                "{o_type:?} is expected to be a string-dictionary type"
            );
            assert!(
                late_materialized_object_binding(o_type.as_u16(), 42, 5, 0, u32::MAX, None)
                    .is_none(),
                "{o_type:?} must not borrow another datatype's dictionary id"
            );
        }
    }

    /// The decode lane and the probe-substitution mirror
    /// ([`crate::binding::is_string_dict_term`]) have to agree on what the
    /// string-dictionary lane is, or a literal is one term to the join and
    /// another to equality.
    #[test]
    fn the_two_string_dict_relations_agree() {
        use crate::binding::is_string_dict_term;

        for o_type in [OType::XSD_STRING, OType::FULLTEXT, OType::lang_string(1)] {
            let b = late_materialized_object_binding(o_type.as_u16(), 1, 1, 0, u32::MAX, None)
                .unwrap_or_else(|| panic!("{o_type:?} should stay encoded"));
            assert!(is_string_dict_term(&b), "encoded {o_type:?}");
        }
        // What the decode lane declines arrives materialized, and the mirror
        // recognizes it there.
        for dt in [fluree_vocab::xsd_names::ANY_URI, "custom"] {
            let lit = Binding::lit(
                FlakeValue::String("abc".to_string()),
                Sid::new(fluree_vocab::namespaces::XSD, dt),
            );
            assert!(is_string_dict_term(&lit), "materialized {dt}");
        }
        // Numerics are outside the lane on both sides — the join stays lenient
        // across their subtypes.
        let n =
            late_materialized_object_binding(OType::XSD_INTEGER.as_u16(), 1, 1, 0, u32::MAX, None)
                .expect("xsd:integer stays encoded");
        assert!(!is_string_dict_term(&n));
        assert!(!is_string_dict_term(&Binding::lit(
            FlakeValue::Long(1),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::INTEGER
            ),
        )));
        // …including one a cast builds string-backed. `xsd:float(?o)` yields a
        // `String` value under `xsd:float`, and constraining a probe with it
        // would narrow a join that never touches the string dictionary.
        assert!(!is_string_dict_term(&Binding::lit(
            FlakeValue::String("1.5".to_string()),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::FLOAT
            ),
        )));
    }
}
