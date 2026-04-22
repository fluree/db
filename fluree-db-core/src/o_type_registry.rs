//! `OTypeRegistry` — transition bridge from V1 `(ObjKind, DatatypeDictId, LangId)` to V2 `OType`.
//!
//! Constructed at remap time from the datatype dictionary. The registry resolves
//! every `(o_kind, dt, lang_id)` triple into a single `OType` value.

use crate::ids::DatatypeDictId;
use crate::o_type::OType;
use crate::value_id::ObjKind;

/// Maps V1 fact identity columns to V2 `OType`.
///
/// Constructed from the ledger's datatype IRI list so that non-reserved
/// `DatatypeDictId` values (≥ `RESERVED_COUNT`) can be resolved to known
/// XSD subtypes or tagged as customer-defined types.
#[derive(Clone)]
pub struct OTypeRegistry {
    /// Pre-resolved OType for each DatatypeDictId value.
    /// Index = `DatatypeDictId.as_u16()`.
    /// For reserved values (0–14), populated from hardcoded mapping.
    /// For custom values (≥15), resolved from IRI or tagged as customer type.
    dt_otypes: Vec<OType>,
}

impl OTypeRegistry {
    /// Build a registry from the ledger's custom datatype IRIs.
    ///
    /// `custom_datatype_iris` contains one IRI string per non-reserved
    /// `DatatypeDictId` (positions `RESERVED_COUNT`, `RESERVED_COUNT+1`, …).
    /// Well-known XSD IRIs are resolved to their dedicated `OType` constants;
    /// unrecognized IRIs become `OType::customer_datatype(dt)`.
    pub fn new(custom_datatype_iris: &[String]) -> Self {
        let total = DatatypeDictId::RESERVED_COUNT as usize + custom_datatype_iris.len();
        let mut dt_otypes = Vec::with_capacity(total);

        // Reserved types (0–14).
        // These are placeholders indexed by DatatypeDictId value.
        // The actual resolve() method handles o_kind-specific routing;
        // these entries are used as a fast fallback for dt-dependent paths.
        dt_otypes.push(OType::IRI_REF); // 0: @id
        dt_otypes.push(OType::XSD_STRING); // 1: xsd:string
        dt_otypes.push(OType::XSD_BOOLEAN); // 2: xsd:boolean
        dt_otypes.push(OType::XSD_INTEGER); // 3: xsd:integer
        dt_otypes.push(OType::XSD_LONG); // 4: xsd:long
        dt_otypes.push(OType::XSD_DECIMAL); // 5: xsd:decimal
        dt_otypes.push(OType::XSD_DOUBLE); // 6: xsd:double
        dt_otypes.push(OType::XSD_FLOAT); // 7: xsd:float
        dt_otypes.push(OType::XSD_DATE_TIME); // 8: xsd:dateTime
        dt_otypes.push(OType::XSD_DATE); // 9: xsd:date
        dt_otypes.push(OType::XSD_TIME); // 10: xsd:time
        dt_otypes.push(OType::RESERVED); // 11: rdf:langString (special-cased in resolve)
        dt_otypes.push(OType::RDF_JSON); // 12: @json
        dt_otypes.push(OType::VECTOR); // 13: @vector
        dt_otypes.push(OType::FULLTEXT); // 14: @fulltext
        debug_assert_eq!(dt_otypes.len(), DatatypeDictId::RESERVED_COUNT as usize);

        // Custom datatypes (≥15).
        for (i, iri) in custom_datatype_iris.iter().enumerate() {
            let dt_id = DatatypeDictId::RESERVED_COUNT + i as u16;
            let otype = resolve_iri_to_otype(iri, dt_id);
            dt_otypes.push(otype);
        }

        Self { dt_otypes }
    }

    /// Build an empty registry (no custom datatypes). Suitable for tests
    /// and ledgers that only use built-in types.
    pub fn builtin_only() -> Self {
        Self::new(&[])
    }

    /// Resolve a V1 `(ObjKind, DatatypeDictId, lang_id)` triple to a V2 `OType`.
    ///
    /// This is the primary conversion function used during spool → run remap.
    #[inline]
    pub fn resolve(&self, o_kind: ObjKind, dt: DatatypeDictId, lang_id: u16) -> OType {
        // 1:1 o_kind → OType mappings (dt and lang_id ignored).
        match o_kind {
            ObjKind::NULL => OType::NULL,
            ObjKind::BOOL => OType::XSD_BOOLEAN,
            ObjKind::DATE => OType::XSD_DATE,
            ObjKind::TIME => OType::XSD_TIME,
            ObjKind::DATE_TIME => OType::XSD_DATE_TIME,
            ObjKind::VECTOR_ID => OType::VECTOR,
            ObjKind::JSON_ID => OType::RDF_JSON,
            ObjKind::NUM_BIG => OType::NUM_BIG_OVERFLOW,
            ObjKind::G_YEAR => OType::XSD_G_YEAR,
            ObjKind::G_YEAR_MONTH => OType::XSD_G_YEAR_MONTH,
            ObjKind::G_MONTH => OType::XSD_G_MONTH,
            ObjKind::G_DAY => OType::XSD_G_DAY,
            ObjKind::G_MONTH_DAY => OType::XSD_G_MONTH_DAY,
            ObjKind::YEAR_MONTH_DUR => OType::XSD_YEAR_MONTH_DURATION,
            ObjKind::DAY_TIME_DUR => OType::XSD_DAY_TIME_DURATION,
            ObjKind::GEO_POINT => OType::GEO_POINT,

            // Blank nodes are currently represented as REF_ID SIDs whose namespace code is
            // `namespaces::BLANK_NODE`. We intentionally map all REF_ID to `OType::IRI_REF`
            // here; code that needs to distinguish IRI vs. blank node should inspect the
            // `SubjectId::ns_code()` of the referenced `sid64` (or the `_:` prefix at the
            // term layer).
            ObjKind::REF_ID => OType::IRI_REF,

            // dt-dependent mappings.
            ObjKind::NUM_INT => self.resolve_by_dt(dt),
            ObjKind::NUM_F64 => self.resolve_by_dt(dt),
            ObjKind::LEX_ID => {
                if dt == DatatypeDictId::LANG_STRING {
                    OType::lang_string(lang_id)
                } else {
                    self.resolve_by_dt(dt)
                }
            }

            // Unknown / future ObjKind values.
            _ => OType::RESERVED,
        }
    }

    /// Look up the pre-resolved OType for a DatatypeDictId.
    #[inline]
    fn resolve_by_dt(&self, dt: DatatypeDictId) -> OType {
        let idx = dt.as_u16() as usize;
        if idx < self.dt_otypes.len() {
            self.dt_otypes[idx]
        } else {
            // dt value beyond what we know — treat as customer datatype.
            OType::customer_datatype(dt.as_u16())
        }
    }
}

// ============================================================================
// IRI → OType resolution (shared, deduped)
// ============================================================================
//
// Both `resolve_iri_to_otype` (positional-dict build) and
// `resolve_iri_to_otype_option` (runtime Sid→OType lookup) delegate to a
// single `KnownDatatype → OType` conversion. The vocabulary recognition
// itself lives in `fluree_vocab::datatype::KnownDatatype`, so all the
// per-crate "which local names does Fluree know about" tables share one
// source of truth.
//
// The split between the two public functions is only in the post-processing
// of unknown IRIs:
//
// - `resolve_iri_to_otype` maps `None` to `OType::customer_datatype(dt_id)`
//   and keeps `OType::RESERVED` for `rdf:langString` as the positional marker.
// - `resolve_iri_to_otype_option` maps both `None` and `RESERVED` to
//   `None`, because runtime callers cannot use the positional marker
//   without a `lang_id` context.

/// Map a recognized `KnownDatatype` to its corresponding `OType`.
///
/// `KnownDatatype::RdfLangString` returns `OType::RESERVED` — that's a
/// positional marker used by the `OTypeRegistry` positional-dict builder
/// at slot 11 to signal "special-case via `OType::lang_string(lang_id)`
/// at query time." Runtime callers that can't compute it should filter
/// the marker back to `None`.
fn known_datatype_to_otype(dt: fluree_vocab::datatype::KnownDatatype) -> OType {
    use fluree_vocab::datatype::KnownDatatype::*;
    match dt {
        // XSD string + subtypes
        XsdString => OType::XSD_STRING,
        XsdAnyUri => OType::XSD_ANY_URI,
        XsdNormalizedString => OType::XSD_NORMALIZED_STRING,
        XsdToken => OType::XSD_TOKEN,
        XsdLanguage => OType::XSD_LANGUAGE,
        XsdBase64Binary => OType::XSD_BASE64_BINARY,
        XsdHexBinary => OType::XSD_HEX_BINARY,

        // XSD boolean
        XsdBoolean => OType::XSD_BOOLEAN,

        // XSD integer family
        XsdInteger => OType::XSD_INTEGER,
        XsdLong => OType::XSD_LONG,
        XsdInt => OType::XSD_INT,
        XsdShort => OType::XSD_SHORT,
        XsdByte => OType::XSD_BYTE,
        XsdUnsignedLong => OType::XSD_UNSIGNED_LONG,
        XsdUnsignedInt => OType::XSD_UNSIGNED_INT,
        XsdUnsignedShort => OType::XSD_UNSIGNED_SHORT,
        XsdUnsignedByte => OType::XSD_UNSIGNED_BYTE,
        XsdNonNegativeInteger => OType::XSD_NON_NEGATIVE_INTEGER,
        XsdPositiveInteger => OType::XSD_POSITIVE_INTEGER,
        XsdNonPositiveInteger => OType::XSD_NON_POSITIVE_INTEGER,
        XsdNegativeInteger => OType::XSD_NEGATIVE_INTEGER,

        // XSD decimal family
        XsdDecimal => OType::XSD_DECIMAL,
        XsdFloat => OType::XSD_FLOAT,
        XsdDouble => OType::XSD_DOUBLE,

        // XSD temporal
        XsdDateTime => OType::XSD_DATE_TIME,
        XsdDate => OType::XSD_DATE,
        XsdTime => OType::XSD_TIME,
        XsdGYear => OType::XSD_G_YEAR,
        XsdGYearMonth => OType::XSD_G_YEAR_MONTH,
        XsdGMonth => OType::XSD_G_MONTH,
        XsdGDay => OType::XSD_G_DAY,
        XsdGMonthDay => OType::XSD_G_MONTH_DAY,

        // XSD duration
        XsdDuration => OType::XSD_DURATION,
        XsdDayTimeDuration => OType::XSD_DAY_TIME_DURATION,
        XsdYearMonthDuration => OType::XSD_YEAR_MONTH_DURATION,

        // RDF
        RdfJson => OType::RDF_JSON,
        RdfLangString => OType::RESERVED,

        // JSON-LD `@id` — used for IRI references. There is no dedicated
        // "id" OType variant today; refs reach the positional dict through
        // the `IRI_REF` o_kind at a different layer, so if we ever
        // encounter this through the IRI→OType path we return RESERVED
        // to signal "caller should use its own ref handling." In practice
        // this path is never hit.
        JsonLdId => OType::RESERVED,

        // Fluree built-ins
        FlureeEmbeddingVector => OType::VECTOR,
        FlureeFullText => OType::FULLTEXT,
    }
}

/// Resolve a canonical datatype identifier to its well-known `OType`, if
/// recognized.
///
/// Delegates vocabulary recognition to
/// `fluree_vocab::datatype::KnownDatatype::from_canonical_form`, then maps
/// the recognized variant to its specific `OType` via
/// [`known_datatype_to_otype`]. See its doc for the `RESERVED` semantics.
fn resolve_known_iri_to_otype(iri: &str) -> Option<OType> {
    fluree_vocab::datatype::KnownDatatype::from_canonical_form(iri).map(known_datatype_to_otype)
}

/// Resolve a datatype IRI to its `OType` for the positional-dict build path.
///
/// Unknown IRIs are assigned `OType::customer_datatype(dt_id)`. The
/// positional marker `OType::RESERVED` returned by the shared helper for
/// `rdf:langString` is kept as-is — the builder uses it to mark slot 11 as
/// "special-cased via `OType::lang_string(lang_id)` at query time."
fn resolve_iri_to_otype(iri: &str, dt_id: u16) -> OType {
    resolve_known_iri_to_otype(iri).unwrap_or_else(|| OType::customer_datatype(dt_id))
}

/// Resolve a datatype IRI to its `OType` for the runtime Sid→OType path.
///
/// Returns `None` for unrecognized IRIs **and** for `rdf:langString`
/// (whose positional-marker `OType::RESERVED` is not usable at runtime
/// without an accompanying `lang_id`). Callers that see `None` fall back
/// to their own handling — e.g. `find_dt_id` in the binary scan path.
///
/// Expects canonical full IRIs. Legacy CURIE / `@` shorthand forms that
/// historical v3 commits may carry are canonicalized upstream by the v3
/// reader (`legacy_v3::read_commit_v3` / `load_commit_ops_v3`) before any
/// IRI reaches this function.
pub fn resolve_iri_to_otype_option(iri: &str) -> Option<OType> {
    match resolve_known_iri_to_otype(iri) {
        Some(OType::RESERVED) => None,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::DatatypeDictId;
    use crate::value_id::ObjKind;

    #[test]
    fn one_to_one_kinds() {
        let reg = OTypeRegistry::builtin_only();
        assert_eq!(
            reg.resolve(ObjKind::NULL, DatatypeDictId::STRING, 0),
            OType::NULL
        );
        assert_eq!(
            reg.resolve(ObjKind::BOOL, DatatypeDictId::BOOLEAN, 0),
            OType::XSD_BOOLEAN
        );
        assert_eq!(
            reg.resolve(ObjKind::DATE, DatatypeDictId::DATE, 0),
            OType::XSD_DATE
        );
        assert_eq!(
            reg.resolve(ObjKind::TIME, DatatypeDictId::TIME, 0),
            OType::XSD_TIME
        );
        assert_eq!(
            reg.resolve(ObjKind::DATE_TIME, DatatypeDictId::DATE_TIME, 0),
            OType::XSD_DATE_TIME
        );
        assert_eq!(
            reg.resolve(ObjKind::REF_ID, DatatypeDictId::ID, 0),
            OType::IRI_REF
        );
        assert_eq!(
            reg.resolve(ObjKind::VECTOR_ID, DatatypeDictId::VECTOR, 0),
            OType::VECTOR
        );
        assert_eq!(
            reg.resolve(ObjKind::JSON_ID, DatatypeDictId::JSON, 0),
            OType::RDF_JSON
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_BIG, DatatypeDictId::DECIMAL, 0),
            OType::NUM_BIG_OVERFLOW
        );
        assert_eq!(
            reg.resolve(ObjKind::GEO_POINT, DatatypeDictId::STRING, 0),
            OType::GEO_POINT
        );
    }

    #[test]
    fn num_int_subtypes() {
        let reg = OTypeRegistry::builtin_only();
        assert_eq!(
            reg.resolve(ObjKind::NUM_INT, DatatypeDictId::INTEGER, 0),
            OType::XSD_INTEGER
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_INT, DatatypeDictId::LONG, 0),
            OType::XSD_LONG
        );
    }

    #[test]
    fn num_int_custom_subtypes() {
        let reg = OTypeRegistry::new(&[
            "http://www.w3.org/2001/XMLSchema#int".to_string(),
            "http://www.w3.org/2001/XMLSchema#short".to_string(),
            "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string(),
        ]);
        assert_eq!(
            reg.resolve(ObjKind::NUM_INT, DatatypeDictId::from_u16(15), 0),
            OType::XSD_INT
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_INT, DatatypeDictId::from_u16(16), 0),
            OType::XSD_SHORT
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_INT, DatatypeDictId::from_u16(17), 0),
            OType::XSD_UNSIGNED_LONG
        );
    }

    #[test]
    fn num_f64_subtypes() {
        let reg = OTypeRegistry::builtin_only();
        assert_eq!(
            reg.resolve(ObjKind::NUM_F64, DatatypeDictId::DOUBLE, 0),
            OType::XSD_DOUBLE
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_F64, DatatypeDictId::FLOAT, 0),
            OType::XSD_FLOAT
        );
        assert_eq!(
            reg.resolve(ObjKind::NUM_F64, DatatypeDictId::DECIMAL, 0),
            OType::XSD_DECIMAL
        );
    }

    #[test]
    fn lex_id_subtypes() {
        let reg = OTypeRegistry::builtin_only();
        assert_eq!(
            reg.resolve(ObjKind::LEX_ID, DatatypeDictId::STRING, 0),
            OType::XSD_STRING
        );
        assert_eq!(
            reg.resolve(ObjKind::LEX_ID, DatatypeDictId::FULL_TEXT, 0),
            OType::FULLTEXT
        );
    }

    #[test]
    fn lang_string() {
        let reg = OTypeRegistry::builtin_only();
        let ot = reg.resolve(ObjKind::LEX_ID, DatatypeDictId::LANG_STRING, 42);
        assert!(ot.is_lang_string());
        assert_eq!(ot.lang_id(), Some(42));
    }

    #[test]
    fn customer_datatype() {
        let reg = OTypeRegistry::new(&["http://example.org/myType".to_string()]);
        let ot = reg.resolve(ObjKind::LEX_ID, DatatypeDictId::from_u16(15), 0);
        assert!(ot.is_customer_datatype());
        assert_eq!(ot.payload(), 15);
    }

    #[test]
    fn temporal_kinds() {
        let reg = OTypeRegistry::builtin_only();
        assert_eq!(
            reg.resolve(ObjKind::G_YEAR, DatatypeDictId::STRING, 0),
            OType::XSD_G_YEAR
        );
        assert_eq!(
            reg.resolve(ObjKind::G_YEAR_MONTH, DatatypeDictId::STRING, 0),
            OType::XSD_G_YEAR_MONTH
        );
        assert_eq!(
            reg.resolve(ObjKind::G_MONTH, DatatypeDictId::STRING, 0),
            OType::XSD_G_MONTH
        );
        assert_eq!(
            reg.resolve(ObjKind::G_DAY, DatatypeDictId::STRING, 0),
            OType::XSD_G_DAY
        );
        assert_eq!(
            reg.resolve(ObjKind::G_MONTH_DAY, DatatypeDictId::STRING, 0),
            OType::XSD_G_MONTH_DAY
        );
        assert_eq!(
            reg.resolve(ObjKind::YEAR_MONTH_DUR, DatatypeDictId::STRING, 0),
            OType::XSD_YEAR_MONTH_DURATION
        );
        assert_eq!(
            reg.resolve(ObjKind::DAY_TIME_DUR, DatatypeDictId::STRING, 0),
            OType::XSD_DAY_TIME_DURATION
        );
    }

    #[test]
    fn unknown_dt_beyond_registry() {
        let reg = OTypeRegistry::builtin_only();
        // dt=100 is beyond the registry's known range — falls back to customer_datatype.
        let ot = reg.resolve(ObjKind::NUM_INT, DatatypeDictId::from_u16(100), 0);
        assert!(ot.is_customer_datatype());
        assert_eq!(ot.payload(), 100);
    }

    #[test]
    fn custom_string_subtypes() {
        let reg = OTypeRegistry::new(&[
            "http://www.w3.org/2001/XMLSchema#anyURI".to_string(),
            "http://www.w3.org/2001/XMLSchema#normalizedString".to_string(),
            "http://www.w3.org/2001/XMLSchema#hexBinary".to_string(),
        ]);
        assert_eq!(
            reg.resolve(ObjKind::LEX_ID, DatatypeDictId::from_u16(15), 0),
            OType::XSD_ANY_URI
        );
        assert_eq!(
            reg.resolve(ObjKind::LEX_ID, DatatypeDictId::from_u16(16), 0),
            OType::XSD_NORMALIZED_STRING
        );
        assert_eq!(
            reg.resolve(ObjKind::LEX_ID, DatatypeDictId::from_u16(17), 0),
            OType::XSD_HEX_BINARY
        );
    }
}
