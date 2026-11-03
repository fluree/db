//! What a mapping proves about duplicate triples.
//!
//! R2RML (§11) defines the output of a mapping as an RDF dataset: a SET of
//! triples. A logical table is a bag of rows, so two rows can mint the same
//! triple (a history table holding several versions of one customer, a fact
//! table holding an exact duplicate row). A lane that turns rows into
//! solutions without removing duplicates answers correctly only where it can
//! prove no two rows mint the same triple for the pattern at hand. This module
//! holds that proof, so every lane asks the same question the same way:
//!
//! - [`TriplesMap::subject_unique_under`]: no two rows of the map's logical
//!   table mint the same subject, given what is known about the table's keys
//!   ([`TableKeys`]).
//! - [`TriplesMap::emits_distinct_per_row`]: one row never mints the same
//!   `(predicate, object)` pair twice.
//! - [`TriplesMap::subjects_provably_disjoint`]: no subject one map mints is
//!   ever minted by the other.
//!
//! Each answers `false` whenever it cannot decide, so a caller that trusts a
//! `true` and deduplicates on `false` never returns a duplicate.

use serde::{Deserialize, Deserializer, Serialize};

use super::{ConstantValue, ObjectMap, PredicateMap, SubjectMap, TermType, TriplesMap};
use crate::materialize::template_is_injective;

/// What is known to identify a row of one logical table: zero or more keys,
/// each a set of column names no two rows agree on once rows with a NULL in
/// any of them are set aside.
///
/// The union of what the mapping declares (`f:key` on the logical table) and
/// what the source reports for the table (an Iceberg table's identifier
/// fields, for one). Both are trusted, not checked against the data.
///
/// Normalized on construction: each key's columns sorted and deduplicated, the
/// keys themselves sorted and deduplicated, and an empty key dropped (a key
/// with no columns would claim the table holds at most one row, which nothing
/// here ever means). Two `TableKeys` holding the same keys therefore compare,
/// and serialize, equal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TableKeys {
    keys: Vec<Vec<String>>,
}

impl<'de> Deserialize<'de> for TableKeys {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<Vec<String>>::deserialize(deserializer).map(Self::new)
    }
}

impl TableKeys {
    /// No key known.
    pub fn none() -> Self {
        Self::default()
    }

    /// The given keys, normalized (see the type's doc).
    pub fn new<K, S>(keys: impl IntoIterator<Item = K>) -> Self
    where
        K: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::none().union(keys)
    }

    /// These keys and `more`, normalized.
    pub fn union<K, S>(mut self, more: impl IntoIterator<Item = K>) -> Self
    where
        K: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for key in more {
            let mut columns: Vec<String> = key.into_iter().map(Into::into).collect();
            columns.sort();
            columns.dedup();
            if !columns.is_empty() {
                self.keys.push(columns);
            }
        }
        self.keys.sort();
        self.keys.dedup();
        self
    }

    /// These keys and the ones a source reported for the table
    /// (`R2rmlTableProvider::table_keys`), `None` meaning it reported none.
    pub fn with_reported(self, reported: Option<Vec<Vec<String>>>) -> Self {
        match reported {
            Some(keys) => self.union(keys),
            None => self,
        }
    }

    /// Whether no key is known.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Each key's columns, sorted.
    pub fn iter(&self) -> impl Iterator<Item = &[String]> {
        self.keys.iter().map(Vec::as_slice)
    }

    /// Whether some key lies wholly within `columns`: two rows that agree on
    /// every one of `columns` (none NULL) are then the same row.
    pub fn has_key_within<'a>(&self, columns: impl IntoIterator<Item = &'a str>) -> bool {
        let columns: Vec<&str> = columns.into_iter().collect();
        self.keys
            .iter()
            .any(|key| key.iter().all(|col| columns.contains(&col.as_str())))
    }
}

/// Where a subject map's term comes from, in the precedence the materializer
/// applies (`materialize_subject_from_batch`): a constant wins over a column,
/// a column over a template.
enum SubjectSource<'a> {
    Constant(&'a str),
    Column(&'a str),
    Template(&'a str),
    /// No value source: the materializer mints a per-row blank node from the
    /// row's position in its batch, or refuses.
    Unspecified,
}

impl SubjectMap {
    fn source(&self) -> SubjectSource<'_> {
        if let Some(c) = &self.constant {
            SubjectSource::Constant(c)
        } else if let Some(c) = &self.column {
            SubjectSource::Column(c)
        } else if let Some(t) = &self.template {
            SubjectSource::Template(t)
        } else {
            SubjectSource::Unspecified
        }
    }

    /// The kind of term the map mints: a constant subject is always an IRI,
    /// whatever `rr:termType` says (the materializer ignores it there).
    fn minted_term_type(&self) -> TermType {
        if self.constant.is_some() {
            TermType::Iri
        } else {
            self.term_type
        }
    }
}

impl TriplesMap {
    /// Whether no two rows of this map's logical table can mint the same
    /// subject, given `keys` for that table.
    ///
    /// True only when the subject is a function of the row that cannot map
    /// two distinct rows to one term:
    ///
    /// - an `rr:column` subject over a column that is by itself a key; or
    /// - an `rr:template` subject whose columns contain a key, and whose
    ///   expansion is injective ([`template_is_injective`]): distinct values
    ///   of its columns always expand to distinct strings.
    ///
    /// Two rows that differ on the key then differ on a subject column, and
    /// so mint different subjects; a row with a NULL in a subject column
    /// mints no subject at all. A constant subject (every row mints the same
    /// one) and a subject map with no value source are never unique.
    pub fn subject_unique_under(&self, keys: &TableKeys) -> bool {
        let sm = &self.subject_map;
        if sm.term_type == TermType::Literal {
            return false;
        }
        match sm.source() {
            SubjectSource::Column(column) => keys.has_key_within([column]),
            SubjectSource::Template(template) => {
                // The columns the expansion actually reads, from the template
                // itself rather than the cached list beside it.
                let columns = super::extract_template_columns(template);
                template_is_injective(template)
                    && keys.has_key_within(columns.iter().map(String::as_str))
            }
            SubjectSource::Constant(_) | SubjectSource::Unspecified => false,
        }
    }

    /// Whether one row of this map never mints the same `(predicate, object)`
    /// pair twice, for `predicate`.
    ///
    /// Two sources of objects for one predicate can collide: two
    /// predicate-object maps naming it (or one naming it beside one whose
    /// predicate is computed per row, which may compute it), and for
    /// `rdf:type` an `rr:class` beside an explicit `rdf:type` map. The pair
    /// is distinct only when there is at most one such source, or every
    /// source is a constant and no two constants are equal. A single
    /// referencing object map counts once: the parents it resolves to are a
    /// set.
    pub fn emits_distinct_per_row(&self, predicate: &str) -> bool {
        let mut constants: Vec<Emitted<'_>> = Vec::new();
        let mut computed = 0usize;
        if predicate == fluree_vocab::rdf::TYPE {
            constants.extend(self.subject_map.classes.iter().map(|c| Emitted::Iri(c)));
        }
        for pom in &self.predicate_object_maps {
            let may_mint = match &pom.predicate_map {
                PredicateMap::Constant(p) => p == predicate,
                PredicateMap::Template { .. } | PredicateMap::Column(_) => true,
            };
            if !may_mint {
                continue;
            }
            match &pom.object_map {
                ObjectMap::Constant { value } => constants.push(Emitted::Constant(value)),
                ObjectMap::Column { .. }
                | ObjectMap::Template { .. }
                | ObjectMap::RefObjectMap(_) => computed += 1,
            }
        }
        match computed {
            0 => pairwise_distinct(&constants),
            1 => constants.is_empty(),
            _ => false,
        }
    }

    /// Whether one row of this map never mints the same triple twice, over
    /// every predicate (a pattern whose predicate is a variable).
    ///
    /// [`Self::emits_distinct_per_row`] for each predicate the map names, and
    /// no predicate computed per row beside any other source (it may compute
    /// any of their predicates).
    pub fn emits_distinct_triples_per_row(&self) -> bool {
        let computed_predicate = self
            .predicate_object_maps
            .iter()
            .any(|pom| !pom.predicate_map.is_constant());
        if computed_predicate {
            return self.predicate_object_maps.len() + self.subject_map.classes.len() <= 1;
        }
        let mut predicates: Vec<&str> = self
            .predicate_object_maps
            .iter()
            .filter_map(|pom| pom.predicate_map.as_constant())
            .collect();
        if !self.subject_map.classes.is_empty() {
            predicates.push(fluree_vocab::rdf::TYPE);
        }
        predicates.sort_unstable();
        predicates.dedup();
        predicates
            .into_iter()
            .all(|p| self.emits_distinct_per_row(p))
    }

    /// Whether no subject this map mints is ever minted by `other`.
    ///
    /// An IRI is never a blank node; two constants are disjoint when they
    /// differ; a constant and a template when the constant does not start
    /// with the template's constant prefix; two templates when neither
    /// constant prefix is a prefix of the other
    /// ([`templates_provably_disjoint`]). Anything involving an `rr:column`
    /// subject, or a subject map with no value source, is not provable.
    pub fn subjects_provably_disjoint(&self, other: &TriplesMap) -> bool {
        let (a, b) = (&self.subject_map, &other.subject_map);
        if a.minted_term_type() != b.minted_term_type() {
            return true;
        }
        match (a.source(), b.source()) {
            (SubjectSource::Constant(x), SubjectSource::Constant(y)) => x != y,
            (SubjectSource::Constant(c), SubjectSource::Template(t))
            | (SubjectSource::Template(t), SubjectSource::Constant(c)) => {
                !c.starts_with(constant_prefix(t))
            }
            (SubjectSource::Template(x), SubjectSource::Template(y)) => {
                templates_provably_disjoint(x, y)
            }
            _ => false,
        }
    }
}

/// An object one row can mint from a constant source.
#[derive(PartialEq)]
enum Emitted<'a> {
    /// An `rr:class`, as the object of `rdf:type`.
    Iri(&'a str),
    /// An `rr:constant` object map.
    Constant(&'a ConstantValue),
}

impl Emitted<'_> {
    /// Whether the two mint the same term: an `rr:class` IRI and a constant
    /// IRI object are the same term when their IRIs are equal.
    fn same_term(&self, other: &Self) -> bool {
        let iri = |e: &Self| match e {
            Emitted::Iri(i) => Some(*i),
            Emitted::Constant(ConstantValue::Iri(i)) => Some(i.as_str()),
            Emitted::Constant(ConstantValue::Literal { .. }) => None,
        };
        match (iri(self), iri(other)) {
            (Some(a), Some(b)) => a == b,
            (None, None) => self == other,
            _ => false,
        }
    }
}

fn pairwise_distinct(emitted: &[Emitted<'_>]) -> bool {
    emitted
        .iter()
        .enumerate()
        .all(|(i, a)| emitted[i + 1..].iter().all(|b| !a.same_term(b)))
}

/// The constant prefix of an `rr:template`: everything before the first `{`
/// placeholder (the whole string when there is none). The expansion writes it
/// verbatim, so every term the template mints starts with it.
pub fn constant_prefix(template: &str) -> &str {
    match template.find('{') {
        Some(i) => &template[..i],
        None => template,
    }
}

/// Whether two templates provably mint disjoint sets of strings: neither
/// constant prefix is a prefix of the other, so any two expansions differ
/// within the constant region, before a placeholder value can matter. Equal
/// prefixes count as overlapping (conservative).
pub fn templates_provably_disjoint(a: &str, b: &str) -> bool {
    let pa = constant_prefix(a);
    let pb = constant_prefix(b);
    !pa.starts_with(pb) && !pb.starts_with(pa)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{PredicateObjectMap, RefObjectMap};

    const RDF_TYPE: &str = fluree_vocab::rdf::TYPE;
    const NAME: &str = "http://example.org/name";
    const CUSTOMER: &str = "http://example.org/Customer";

    fn keys(k: &[&[&str]]) -> TableKeys {
        TableKeys::new(k.iter().map(|cols| cols.iter().copied()))
    }

    fn map(template: &str) -> TriplesMap {
        TriplesMap::new("http://example.org/m#M", "sets.customers_history")
            .with_subject_template(template)
    }

    fn pom(predicate: &str, object_map: ObjectMap) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::constant(predicate),
            object_map,
        }
    }

    #[test]
    fn table_keys_normalize_and_drop_empty_keys() {
        let k = TableKeys::new([vec!["b", "a", "a"], vec![], vec!["a", "b"], vec!["c"]]);
        let got: Vec<&[String]> = k.iter().collect();
        assert_eq!(
            got,
            vec![
                &["a".to_string(), "b".to_string()][..],
                &["c".to_string()][..]
            ]
        );
        // An empty key must never be kept: it would put every column set
        // "within" a key and certify any subject unique.
        assert!(TableKeys::new([Vec::<String>::new()]).is_empty());
        assert!(!TableKeys::new([Vec::<String>::new()]).has_key_within(["anything"]));
        // Same keys, either order, compare equal.
        assert_eq!(
            TableKeys::new([vec!["x"], vec!["y", "z"]]),
            TableKeys::new([vec!["z", "y"], vec!["x"]])
        );
    }

    #[test]
    fn table_keys_union_with_reported_keys() {
        let declared = keys(&[&["customer_id"]]);
        assert_eq!(declared.clone().with_reported(None), declared);
        let both = declared.with_reported(Some(vec![vec!["order_id".to_string()]]));
        assert!(both.has_key_within(["order_id"]));
        assert!(both.has_key_within(["customer_id", "name"]));
        assert!(!both.has_key_within(["name"]));
    }

    #[test]
    fn table_keys_serialize_as_lists_and_deserialize_normalized() {
        let k = keys(&[&["valid_from", "customer_id"]]);
        let json = serde_json::to_string(&k).unwrap();
        assert_eq!(json, r#"[["customer_id","valid_from"]]"#);
        let back: TableKeys = serde_json::from_str(r#"[["valid_from","customer_id"],[]]"#).unwrap();
        assert_eq!(back, k);
    }

    #[test]
    fn a_template_subject_is_unique_when_its_columns_hold_a_key() {
        let tm = map("http://example.org/customer/{customer_id}");
        assert!(tm.subject_unique_under(&keys(&[&["customer_id"]])));
        // No key, or a key the subject does not cover (a history table's
        // natural key carries the version column).
        assert!(!tm.subject_unique_under(&TableKeys::none()));
        assert!(!tm.subject_unique_under(&keys(&[&["customer_id", "valid_from"]])));
        assert!(!tm.subject_unique_under(&keys(&[&["name"]])));
        // One key among several suffices.
        assert!(tm.subject_unique_under(&keys(&[&["name"], &["customer_id"]])));

        // A composite subject covering a composite key.
        let tm = map("http://example.org/customer/{customer_id}/{valid_from}");
        assert!(tm.subject_unique_under(&keys(&[&["customer_id", "valid_from"]])));
        assert!(tm.subject_unique_under(&keys(&[&["customer_id"]])));
    }

    #[test]
    fn a_template_that_can_merge_two_values_is_never_unique() {
        let key = keys(&[&["a", "b"]]);
        // Adjacent placeholders: ("1", "23") and ("12", "3") both give "123".
        assert!(!map("http://example.org/x/{a}{b}").subject_unique_under(&key));
        // Separators the expansion can also produce inside a value: '-' and
        // '.' are unreserved, ':' and ';' are passed through unescaped, and
        // '%' starts every escape the expansion writes.
        for sep in ["-", ".", ":", ";", "_", "~", "%2F", "a"] {
            let t = format!("http://example.org/x/{{a}}{sep}{{b}}");
            assert!(!map(&t).subject_unique_under(&key), "{t}");
        }
        // A separator holding a character the expansion always encodes.
        for sep in ["/", "#", "?", "/-", "-/", " ", "|", "\u{e9}"] {
            let t = format!("http://example.org/x/{{a}}{sep}{{b}}");
            assert!(map(&t).subject_unique_under(&key), "{t}");
        }
    }

    #[test]
    fn a_column_subject_is_unique_over_a_key_column_alone() {
        let mut tm = map("unused");
        tm.subject_map = SubjectMap::column("customer_iri");
        assert!(tm.subject_unique_under(&keys(&[&["customer_iri"]])));
        assert!(!tm.subject_unique_under(&keys(&[&["customer_iri", "valid_from"]])));
        assert!(!tm.subject_unique_under(&TableKeys::none()));
        // A column beside a template: the materializer reads the column.
        let mut both = map("http://example.org/customer/{customer_id}");
        both.subject_map.column = Some("customer_iri".into());
        assert!(!both.subject_unique_under(&keys(&[&["customer_id"]])));
        assert!(both.subject_unique_under(&keys(&[&["customer_iri"]])));
    }

    #[test]
    fn a_constant_or_sourceless_subject_is_never_unique() {
        let mut tm = map("http://example.org/customer/{customer_id}");
        tm.subject_map.constant = Some("http://example.org/the-one".into());
        assert!(!tm.subject_unique_under(&keys(&[&["customer_id"]])));
        let mut tm = map("unused");
        tm.subject_map = SubjectMap::default().as_blank_node();
        assert!(!tm.subject_unique_under(&keys(&[&["customer_id"]])));
        // A literal subject mints nothing a caller could rely on.
        let mut tm = map("http://example.org/customer/{customer_id}");
        tm.subject_map.term_type = TermType::Literal;
        assert!(!tm.subject_unique_under(&keys(&[&["customer_id"]])));
    }

    #[test]
    fn a_blank_node_template_subject_is_unique_like_an_iri_one() {
        let mut tm = map("customer{customer_id}");
        tm.subject_map.term_type = TermType::BlankNode;
        assert!(tm.subject_unique_under(&keys(&[&["customer_id"]])));
    }

    #[test]
    fn the_subject_columns_are_read_from_the_template_itself() {
        // A stale cached column list must not certify a key the expansion
        // does not read.
        let mut tm = map("http://example.org/customer/{name}");
        tm.subject_map.template_columns = vec!["customer_id".into()];
        assert!(!tm.subject_unique_under(&keys(&[&["customer_id"]])));
    }

    #[test]
    fn one_object_source_per_predicate_is_distinct() {
        let tm = map("http://example.org/customer/{customer_id}")
            .with_class(CUSTOMER)
            .with_predicate_object(pom(NAME, ObjectMap::column("name")))
            .with_predicate_object(pom("http://example.org/city", ObjectMap::column("city")));
        assert!(tm.emits_distinct_per_row(NAME));
        assert!(tm.emits_distinct_per_row(RDF_TYPE));
        assert!(tm.emits_distinct_per_row("http://example.org/absent"));
        assert!(tm.emits_distinct_triples_per_row());
    }

    #[test]
    fn two_object_sources_for_one_predicate_can_collide() {
        let tm = map("http://example.org/customer/{customer_id}")
            .with_predicate_object(pom(NAME, ObjectMap::column("name")))
            .with_predicate_object(pom(NAME, ObjectMap::column("nickname")));
        assert!(!tm.emits_distinct_per_row(NAME));
        assert!(!tm.emits_distinct_triples_per_row());

        // A column beside a constant: the column can hold the constant.
        let tm = map("http://example.org/customer/{customer_id}")
            .with_predicate_object(pom(NAME, ObjectMap::column("name")))
            .with_predicate_object(pom(NAME, ObjectMap::constant_literal("anon")));
        assert!(!tm.emits_distinct_per_row(NAME));

        // Two references, even to different parents.
        let tm = map("http://example.org/order/{order_id}")
            .with_predicate_object(pom(
                NAME,
                ObjectMap::RefObjectMap(RefObjectMap::new("<#A>", "a_id", "id")),
            ))
            .with_predicate_object(pom(
                NAME,
                ObjectMap::RefObjectMap(RefObjectMap::new("<#B>", "b_id", "id")),
            ));
        assert!(!tm.emits_distinct_per_row(NAME));
    }

    #[test]
    fn distinct_constants_for_one_predicate_are_distinct() {
        let tm = map("http://example.org/customer/{customer_id}")
            .with_predicate_object(pom(NAME, ObjectMap::constant_literal("a")))
            .with_predicate_object(pom(NAME, ObjectMap::constant_literal("b")))
            .with_predicate_object(pom(NAME, ObjectMap::constant_iri("http://example.org/a")));
        assert!(tm.emits_distinct_per_row(NAME));
        let tm = tm.with_predicate_object(pom(NAME, ObjectMap::constant_literal("a")));
        assert!(!tm.emits_distinct_per_row(NAME));
    }

    #[test]
    fn a_class_and_an_explicit_type_map_can_collide() {
        let base = || {
            map("http://example.org/customer/{customer_id}")
                .with_class(CUSTOMER)
                .with_class("http://example.org/Party")
        };
        assert!(base().emits_distinct_per_row(RDF_TYPE));
        // The same class stated twice mints rdf:type twice.
        assert!(!base().with_class(CUSTOMER).emits_distinct_per_row(RDF_TYPE));
        // An explicit rdf:type naming one of the classes.
        let tm = base().with_predicate_object(pom(RDF_TYPE, ObjectMap::constant_iri(CUSTOMER)));
        assert!(!tm.emits_distinct_per_row(RDF_TYPE));
        // ... or a computed rdf:type.
        let tm = base().with_predicate_object(pom(RDF_TYPE, ObjectMap::column_iri("kind")));
        assert!(!tm.emits_distinct_per_row(RDF_TYPE));
        // An explicit rdf:type naming another class is fine.
        let tm = base().with_predicate_object(pom(
            RDF_TYPE,
            ObjectMap::constant_iri("http://example.org/Other"),
        ));
        assert!(tm.emits_distinct_per_row(RDF_TYPE));
        assert!(tm.emits_distinct_triples_per_row());
        // A class is an IRI: a literal of the same text is another term.
        let tm = base().with_predicate_object(pom(RDF_TYPE, ObjectMap::constant_literal(CUSTOMER)));
        assert!(tm.emits_distinct_per_row(RDF_TYPE));
    }

    #[test]
    fn a_computed_predicate_may_mint_any_predicate() {
        let computed = PredicateObjectMap {
            predicate_map: PredicateMap::Column("attr".into()),
            object_map: ObjectMap::column("value"),
        };
        let tm = map("http://example.org/customer/{customer_id}")
            .with_predicate_object(pom(NAME, ObjectMap::column("name")))
            .with_predicate_object(computed.clone());
        assert!(!tm.emits_distinct_per_row(NAME));
        // A predicate only the computed map can mint has one source.
        assert!(tm.emits_distinct_per_row("http://example.org/anything"));
        assert!(!tm.emits_distinct_triples_per_row());
        // Alone, it is the row's one source.
        let alone =
            map("http://example.org/customer/{customer_id}").with_predicate_object(computed);
        assert!(alone.emits_distinct_per_row(NAME));
        assert!(alone.emits_distinct_triples_per_row());
        assert!(!alone.with_class(CUSTOMER).emits_distinct_triples_per_row());
    }

    #[test]
    fn subject_disjointness_across_maps() {
        let t = |template: &str| map(template);
        let customer = t("http://example.org/customer/{customer_id}");
        assert!(customer.subjects_provably_disjoint(&t("http://example.org/order/{order_id}")));
        // The same template (a history table and a current table over one
        // entity) mints the same subjects.
        assert!(!customer.subjects_provably_disjoint(&t("http://example.org/customer/{id}")));
        // One prefix extends the other.
        assert!(!customer.subjects_provably_disjoint(&t("http://example.org/customer/{a}/{b}")));
        assert!(!t("http://example.org/c{id}").subjects_provably_disjoint(&customer));

        // Constants.
        let mut one = map("unused");
        one.subject_map = SubjectMap::constant("http://example.org/catalog");
        let mut other = map("unused");
        other.subject_map = SubjectMap::constant("http://example.org/catalog2");
        assert!(one.subjects_provably_disjoint(&other));
        assert!(!one.subjects_provably_disjoint(&one.clone()));
        assert!(one.subjects_provably_disjoint(&customer));
        let mut inside = map("unused");
        inside.subject_map = SubjectMap::constant("http://example.org/customer/7");
        assert!(!inside.subjects_provably_disjoint(&customer));
        assert!(!customer.subjects_provably_disjoint(&inside));

        // An IRI is never a blank node, even from the same template; a
        // constant subject is an IRI whatever its term type says.
        let mut blank = t("http://example.org/customer/{customer_id}");
        blank.subject_map.term_type = TermType::BlankNode;
        assert!(blank.subjects_provably_disjoint(&customer));
        let mut const_blank = map("unused");
        const_blank.subject_map =
            SubjectMap::constant("http://example.org/customer/7").as_blank_node();
        assert!(!const_blank.subjects_provably_disjoint(&customer));

        // A column subject can hold anything.
        let mut column = map("unused");
        column.subject_map = SubjectMap::column("iri");
        assert!(!column.subjects_provably_disjoint(&customer));
        assert!(!customer.subjects_provably_disjoint(&column));
    }

    #[test]
    fn templates_disjoint_only_on_diverging_prefix() {
        assert!(templates_provably_disjoint(
            "http://ex/person/{id}",
            "http://ex/order/{id}"
        ));
        assert!(!templates_provably_disjoint(
            "http://ex/person/{id}",
            "http://ex/person/{id}"
        ));
        assert!(!templates_provably_disjoint(
            "http://ex/p/{id}",
            "http://ex/p/{id}/x"
        ));
        assert_eq!(constant_prefix("http://ex/p/{id}"), "http://ex/p/");
        assert_eq!(constant_prefix("http://ex/p"), "http://ex/p");
    }
}
