//! The config reader's vocabulary: the predicates `config_resolver` reads
//! a ledger's configuration with. The staged-config guard (`config_guard`)
//! checks exactly these, the diagnostics (`config_resolver::diagnose`)
//! report exactly these, and a fast-forward decides whether an adopted change
//! carries config settings by them. A test records every predicate the reader
//! actually reads and requires it to be listed here, and every listed one to
//! be read, so the lists cannot drift from the reader.

use fluree_db_core::Sid;
use fluree_vocab::config_iris;
use fluree_vocab::namespaces::FLUREE_DB;
use rustc_hash::FxHashSet;
use std::sync::OnceLock;

/// The setting-group pointers of a config node, with the group each names.
pub(crate) const GROUP_POINTERS: &[&str] = &[
    config_iris::POLICY_DEFAULTS,
    config_iris::SHACL_DEFAULTS,
    config_iris::REASONING_DEFAULTS,
    config_iris::DATALOG_DEFAULTS,
    config_iris::TRANSACT_DEFAULTS,
    config_iris::FULL_TEXT_DEFAULTS,
    config_iris::SERVING_DEFAULTS,
    config_iris::QUERY_DEFAULTS,
    config_iris::INDEXING_DEFAULTS,
];

/// Edges from a config node to another config node. The node an edge points
/// at is part of the configuration, so its fields belong in the config graph
/// with the edge.
pub(crate) const CONFIG_EDGES: &[&str] = &[
    config_iris::POLICY_DEFAULTS,
    config_iris::SHACL_DEFAULTS,
    config_iris::REASONING_DEFAULTS,
    config_iris::DATALOG_DEFAULTS,
    config_iris::TRANSACT_DEFAULTS,
    config_iris::FULL_TEXT_DEFAULTS,
    config_iris::SERVING_DEFAULTS,
    config_iris::QUERY_DEFAULTS,
    config_iris::INDEXING_DEFAULTS,
    config_iris::GRAPH_OVERRIDES,
    config_iris::OVERRIDE_CONTROL,
    config_iris::SHAPES_SOURCE,
    config_iris::POLICY_SOURCE,
    config_iris::SCHEMA_SOURCE,
    config_iris::RULES_SOURCE,
    config_iris::CONSTRAINTS_SOURCE,
    config_iris::GRAPH_SOURCE,
    config_iris::TRUST_POLICY_PRED,
    config_iris::ROLLBACK_GUARD,
    config_iris::ONTOLOGY_IMPORT_MAP,
    config_iris::GRAPH_REF_PROP,
    config_iris::FULL_TEXT_PROPERTY,
];

/// Config predicates the reader takes a single value of.
pub(crate) const SINGLE_VALUED: &[&str] = &[
    config_iris::POLICY_DEFAULTS,
    config_iris::SHACL_DEFAULTS,
    config_iris::REASONING_DEFAULTS,
    config_iris::DATALOG_DEFAULTS,
    config_iris::TRANSACT_DEFAULTS,
    config_iris::FULL_TEXT_DEFAULTS,
    config_iris::SERVING_DEFAULTS,
    config_iris::QUERY_DEFAULTS,
    config_iris::INDEXING_DEFAULTS,
    config_iris::DEFAULT_ALLOW,
    config_iris::POLICY_SOURCE,
    config_iris::SHACL_ENABLED,
    config_iris::SHAPES_SOURCE,
    config_iris::VALIDATION_MODE,
    config_iris::OVERRIDE_CONTROL,
    config_iris::CONTROL_MODE,
    config_iris::SCHEMA_SOURCE,
    config_iris::FOLLOW_OWL_IMPORTS,
    config_iris::REASONING_MAX_FACTS,
    config_iris::REASONING_MAX_SECONDS,
    config_iris::REASONING_MAX_MEMORY_MB,
    config_iris::DATALOG_ENABLED,
    config_iris::RULES_SOURCE,
    config_iris::ALLOW_QUERY_TIME_RULES,
    config_iris::SERVE_QUERY,
    config_iris::SERVE_BLOCKS,
    config_iris::PUBLIC_VISIBILITY,
    config_iris::UNION_DEFAULT_GRAPH,
    config_iris::GRAPH_PLACEMENT,
    config_iris::UNIQUE_ENABLED,
    config_iris::DEFAULT_LANGUAGE,
    config_iris::TARGET_GRAPH,
    config_iris::GRAPH_SOURCE,
    config_iris::LEDGER_PRED,
    config_iris::GRAPH_SELECTOR,
    config_iris::AT_T,
    config_iris::TRUST_POLICY_PRED,
    config_iris::TRUST_MODE,
    config_iris::ROLLBACK_GUARD,
    config_iris::MIN_T,
    config_iris::ONTOLOGY_IRI,
    config_iris::GRAPH_REF_PROP,
    config_iris::FULL_TEXT_TARGET,
];

/// Config predicates the reader takes several values of.
pub(crate) const MULTI_VALUED: &[&str] = &[
    fluree_vocab::policy_iris::POLICY_CLASS,
    config_iris::REASONING_MODES,
    config_iris::ALLOWED_IDENTITIES,
];

/// The classes that make a subject a config node by being typed in the
/// config graph: the ledger's config subject, and a per-graph override.
pub(crate) const CONFIG_CLASSES: &[&str] = &[config_iris::LEDGER_CONFIG, config_iris::GRAPH_CONFIG];

/// The documented IRI of a ledger's config subject,
/// `urn:fluree:<ledger_id>:config:ledger`. It names the config subject
/// whether or not the config graph holds it yet, so its config fields
/// anywhere but the config graph are a config written to the wrong graph.
pub(crate) fn conventional_config_subject(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}:config:ledger")
}

/// Whether `p` is a predicate the config reader reads: a group pointer or
/// source edge ([`CONFIG_EDGES`]), a single-valued setting
/// ([`SINGLE_VALUED`]) or a multi-valued one ([`MULTI_VALUED`]). Only these
/// are a config node's fields. Other `f:` predicates written to a user graph
/// are data (an edge annotation's `f:reifies*`, a property's
/// `f:enforceUnique`, a policy document's `f:allow`).
pub(crate) fn is_config_field(p: &Sid) -> bool {
    static FIELDS: OnceLock<FxHashSet<&'static str>> = OnceLock::new();
    p.namespace_code == FLUREE_DB
        && FIELDS
            .get_or_init(|| {
                CONFIG_EDGES
                    .iter()
                    .chain(SINGLE_VALUED)
                    .chain(MULTI_VALUED)
                    .map(|iri| {
                        iri.strip_prefix(fluree_vocab::fluree::DB)
                            .expect("a Fluree vocabulary IRI")
                    })
                    .collect()
            })
            .contains(&*p.name)
}

/// Records the predicates the config reader reads, for the vocabulary test.
#[cfg(test)]
pub(crate) mod read_recorder {
    use std::cell::RefCell;
    use std::collections::BTreeSet;

    thread_local! {
        static READ: RefCell<Option<BTreeSet<String>>> = const { RefCell::new(None) };
    }

    /// Note that the reader looked `iri` up (while recording on this thread).
    pub(crate) fn note(iri: &str) {
        READ.with(|read| {
            if let Some(read) = read.borrow_mut().as_mut() {
                read.insert(iri.to_string());
            }
        });
    }

    /// Record on this thread until [`stop`].
    pub(crate) fn start() {
        READ.with(|read| *read.borrow_mut() = Some(BTreeSet::new()));
    }

    pub(crate) fn stop() -> BTreeSet<String> {
        READ.with(|read| read.borrow_mut().take().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::namespaces::RDF;

    fn f(local: &str) -> Sid {
        Sid::new(FLUREE_DB, local)
    }

    /// Only predicates the config reader reads are a group's fields. Data in
    /// the Fluree vocabulary (an edge annotation's `f:reifies*`, a property's
    /// `f:enforceUnique`, a policy's `f:allow`) is not, so writing it to a
    /// user graph reads no config.
    #[test]
    fn config_fields_are_the_reader_vocabulary() {
        for field in [
            "shaclEnabled",
            "shaclDefaults",
            "defaultAllow",
            "policyClass",
            "reasoningModes",
            "allowedIdentities",
            "graphOverrides",
            "uniqueEnabled",
            "queryDefaults",
            "unionDefaultGraph",
        ] {
            assert!(is_config_field(&f(field)), "{field}");
        }
        for data in [
            fluree_vocab::db::REIFIES_SUBJECT,
            fluree_vocab::db::REIFIES_GRAPH,
            "enforceUnique",
            "allow",
        ] {
            assert!(!is_config_field(&f(data)), "{data}");
        }
        assert!(!is_config_field(&Sid::new(RDF, "shaclEnabled")));
    }

    /// The pointer that names each setting group of a [`LedgerConfig`]
    /// (`f:graphOverrides` aside: several values, read as overrides). The
    /// destructure names every field, so a group added to `LedgerConfig`
    /// stops this compiling until it is listed here, and so reaches the
    /// checks below.
    fn setting_group_pointers(
        config: &fluree_db_core::ledger_config::LedgerConfig,
    ) -> Vec<(&'static str, bool)> {
        let fluree_db_core::ledger_config::LedgerConfig {
            config_id: _,
            policy,
            shacl,
            reasoning,
            datalog,
            transact,
            full_text,
            serving,
            query,
            indexing,
            graph_overrides: _,
        } = config;
        vec![
            (config_iris::POLICY_DEFAULTS, policy.is_some()),
            (config_iris::SHACL_DEFAULTS, shacl.is_some()),
            (config_iris::REASONING_DEFAULTS, reasoning.is_some()),
            (config_iris::DATALOG_DEFAULTS, datalog.is_some()),
            (config_iris::TRANSACT_DEFAULTS, transact.is_some()),
            (config_iris::FULL_TEXT_DEFAULTS, full_text.is_some()),
            (config_iris::SERVING_DEFAULTS, serving.is_some()),
            (config_iris::QUERY_DEFAULTS, query.is_some()),
            (config_iris::INDEXING_DEFAULTS, indexing.is_some()),
        ]
    }

    /// Every setting group is a config edge the guard follows (R1), a
    /// single-valued setting (R3), and a pointer ledger info diagnoses.
    #[test]
    fn every_setting_group_is_guarded_and_diagnosed() {
        let groups = setting_group_pointers(&Default::default());
        assert_eq!(groups.len(), GROUP_POINTERS.len());
        for (pointer, _) in groups {
            assert!(CONFIG_EDGES.contains(&pointer), "{pointer}: a config edge");
            assert!(SINGLE_VALUED.contains(&pointer), "{pointer}: single-valued");
            assert!(GROUP_POINTERS.contains(&pointer), "{pointer}: diagnosed");
        }
        assert!(CONFIG_EDGES.contains(&config_iris::GRAPH_OVERRIDES));
        assert!(!SINGLE_VALUED.contains(&config_iris::GRAPH_OVERRIDES));
    }

    /// The terms of `fluree_vocab::config_iris` that are not config fields:
    /// classes, the IRI values a field takes, and `f:enforceUnique`, which
    /// annotates a property in the data.
    const NOT_FIELDS: &[&str] = &[
        config_iris::LEDGER_CONFIG,
        config_iris::GRAPH_CONFIG,
        config_iris::GRAPH_REF,
        config_iris::TRUST_POLICY,
        config_iris::FULL_TEXT_DEFAULTS_CLASS,
        config_iris::FULL_TEXT_PROPERTY_CLASS,
        config_iris::VALIDATION_REJECT,
        config_iris::VALIDATION_WARN,
        config_iris::DEFAULT_GRAPH,
        config_iris::TXN_META_GRAPH,
        config_iris::TRUSTED,
        config_iris::SIGNED_INDEX,
        config_iris::COMMIT_VERIFIED,
        config_iris::OVERRIDE_NONE,
        config_iris::OVERRIDE_ALL,
        config_iris::IDENTITY_RESTRICTED,
        config_iris::ENFORCE_UNIQUE,
        config_iris::PLACEMENT_AUTO,
        config_iris::PLACEMENT_DEDICATED,
    ];

    /// Every term `fluree_vocab::config_iris` defines is a config field the
    /// guard checks (an edge, a single-valued or a multi-valued setting) or
    /// is listed above as not a field. The terms are read from the vocabulary
    /// source, so a setting added to any group, or a new group, fails here
    /// until the guard lists it.
    #[test]
    fn every_config_vocabulary_term_is_a_field_or_listed() {
        let source = include_str!("../../fluree-vocab/src/lib.rs");
        let module = source
            .split_once("pub mod config_iris {")
            .expect("the config_iris module")
            .1;
        let module = &module[..module.find("\n}").expect("the module's end")];
        let terms: Vec<(&str, &str)> = module
            .lines()
            .filter_map(|line| {
                let rest = line.trim().strip_prefix("pub const ")?;
                let (name, rest) = rest.split_once(": &str = \"")?;
                Some((name, rest.strip_suffix("\";")?))
            })
            .collect();
        assert!(terms.len() > 50, "read {} config_iris terms", terms.len());
        let unlisted: Vec<&str> = terms
            .iter()
            .filter(|(_, iri)| {
                ![CONFIG_EDGES, SINGLE_VALUED, MULTI_VALUED, NOT_FIELDS]
                    .iter()
                    .any(|list| list.contains(iri))
            })
            .map(|(name, _)| *name)
            .collect();
        assert!(
            unlisted.is_empty(),
            "config_iris terms neither guarded nor listed as non-fields: {unlisted:?}"
        );
    }

    /// The predicates the config reader reads are exactly the vocabulary
    /// listed here. The reader resolves a config that sets every group, a
    /// per-graph override and every nested node (sources, trust policy,
    /// rollback guard, ontology import map, full-text property, override
    /// control), recording each predicate it looks up. Each one must be a
    /// listed field (or the RDF list predicates a collection is read with),
    /// and each listed field must have been read: a setting the reader gains
    /// fails here until the guard and diagnostics know it, and a listed field
    /// the reader stops reading fails here too.
    #[tokio::test]
    async fn the_vocabulary_is_what_the_reader_reads() {
        use std::collections::BTreeSet;
        let fluree = crate::FlureeBuilder::memory().build_memory();
        fluree.create_ledger("vocab/reader").await.unwrap();
        let trig = "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            GRAPH <urn:fluree:vocab/reader:main#config> {
              <urn:c:main> a f:LedgerConfig ;
                f:policyDefaults <urn:c:policy> ; f:shaclDefaults <urn:c:shacl> ;
                f:reasoningDefaults <urn:c:reasoning> ; f:datalogDefaults <urn:c:datalog> ;
                f:transactDefaults <urn:c:transact> ; f:fullTextDefaults <urn:c:ft> ;
                f:servingDefaults <urn:c:serving> ; f:queryDefaults <urn:c:query> ;
                f:indexingDefaults <urn:c:indexing> ;
                f:graphOverrides <urn:c:g1> .
              <urn:c:policy> f:defaultAllow false ; f:policySource <urn:c:ref> ;
                f:policyClass ex:Reader ; f:overrideControl <urn:c:oc> .
              <urn:c:oc> f:controlMode f:IdentityRestricted ; f:allowedIdentities <did:key:z6Mk> .
              <urn:c:ref> a f:GraphRef ; f:graphSource <urn:c:src> ;
                f:trustPolicy <urn:c:trust> ; f:rollbackGuard <urn:c:rg> .
              <urn:c:src> f:ledger <urn:other:main> ; f:graphSelector f:defaultGraph ; f:atT 3 .
              <urn:c:trust> f:trustMode f:Trusted .
              <urn:c:rg> f:minT 1 .
              <urn:c:shacl> f:shaclEnabled true ; f:shapesSource <urn:c:ref> ;
                f:validationMode f:ValidationWarn ; f:overrideControl f:OverrideAll .
              <urn:c:reasoning> f:reasoningModes <urn:c:modes> ; f:schemaSource <urn:c:ref> ;
                f:followOwlImports true ; f:reasoningMaxFacts 10 ; f:reasoningMaxSeconds 5 ;
                f:reasoningMaxMemoryMb 64 ; f:ontologyImportMap <urn:c:oim> .
              <urn:c:modes> rdf:first \"rdfs\" ; rdf:rest rdf:nil .
              <urn:c:oim> f:ontologyIri <http://example.org/onto> ; f:graphRef <urn:c:ref> .
              <urn:c:datalog> f:datalogEnabled true ; f:rulesSource <urn:c:ref> ;
                f:allowQueryTimeRules false .
              <urn:c:transact> f:uniqueEnabled true ; f:constraintsSource <urn:c:ref> .
              <urn:c:ft> f:defaultLanguage \"en\" ; f:property <urn:c:ftp> .
              <urn:c:ftp> f:target ex:title .
              <urn:c:serving> f:serveQuery true ; f:serveBlocks true ; f:publicVisibility true .
              <urn:c:query> f:unionDefaultGraph true .
              <urn:c:indexing> f:graphPlacement f:PlacementAuto .
              <urn:c:g1> a f:GraphConfig ; f:targetGraph ex:g1 ; f:shaclDefaults <urn:c:shacl2> .
              <urn:c:shacl2> f:shaclEnabled false .
            }";
        fluree
            .graph("vocab/reader:main")
            .transact()
            .upsert_turtle(trig)
            .commit()
            .await
            .expect("the maximal config");
        let state = fluree.ledger("vocab/reader:main").await.unwrap();

        read_recorder::start();
        let config = crate::config_resolver::resolve_ledger_config(
            &state.snapshot,
            state.novelty.as_ref(),
            state.t(),
        )
        .await;
        let read = read_recorder::stop();
        config.expect("resolves").expect("a config");

        let fields: BTreeSet<&str> = CONFIG_EDGES
            .iter()
            .chain(SINGLE_VALUED)
            .chain(MULTI_VALUED)
            .copied()
            .collect();
        let list_terms = [fluree_vocab::rdf::FIRST, fluree_vocab::rdf::REST];
        let unlisted: Vec<&String> = read
            .iter()
            .filter(|iri| !fields.contains(iri.as_str()) && !list_terms.contains(&iri.as_str()))
            .collect();
        assert!(
            unlisted.is_empty(),
            "the reader reads predicates the vocabulary does not list: {unlisted:?}"
        );
        let unread: Vec<&&str> = fields.iter().filter(|iri| !read.contains(**iri)).collect();
        assert!(
            unread.is_empty(),
            "the vocabulary lists predicates the reader never reads: {unread:?}"
        );
    }

    /// The repair recipes in `docs/ledger-config/writing-config.md` follow
    /// every config edge: the first one's `VALUES ?edge` list is the config
    /// edges, and each one's path is the config edges plus the RDF list a
    /// `f:reasoningModes` value may be. A config edge added here fails until
    /// the recipes have it.
    #[test]
    fn the_repair_recipe_follows_every_config_edge() {
        use std::collections::BTreeSet;
        let doc = include_str!("../../docs/ledger-config/writing-config.md");
        let section = doc
            .split_once("## Repairing a config split across graphs")
            .expect("the repair section")
            .1;
        let section = section.split_once("\n## ").map_or(section, |(s, _)| s);
        let recipes: Vec<&str> = section
            .split("```sparql")
            .skip(1)
            .map(|rest| rest.split_once("```").expect("the recipe").0)
            .collect();
        assert_eq!(
            recipes.len(),
            2,
            "two recipes: from the config subject, and without one"
        );
        let expand = |term: &str| -> String {
            if let Some(local) = term.strip_prefix("f:") {
                format!("{}{local}", fluree_vocab::fluree::DB)
            } else if let Some(local) = term.strip_prefix("rdf:") {
                format!("http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}")
            } else {
                panic!("unexpected term {term:?} in the repair recipe")
            }
        };
        let values: BTreeSet<String> = recipes[0]
            .split_once("VALUES ?edge {")
            .and_then(|(_, rest)| rest.split_once('}'))
            .expect("the VALUES ?edge list")
            .0
            .split_whitespace()
            .map(expand)
            .collect();
        let edges: BTreeSet<String> = CONFIG_EDGES.iter().map(ToString::to_string).collect();
        assert_eq!(values, edges, "VALUES ?edge is the config edges");
        let mut path_terms = edges;
        path_terms.insert(config_iris::REASONING_MODES.to_string());
        path_terms.insert(fluree_vocab::rdf::REST.to_string());
        for recipe in recipes {
            let path: BTreeSet<String> = recipe
                .split_once("?root (")
                .and_then(|(_, rest)| rest.split_once(")*"))
                .expect("the path")
                .0
                .split('|')
                .map(|term| expand(term.trim()))
                .collect();
            assert_eq!(
                path, path_terms,
                "the path is the config edges and f:reasoningModes lists"
            );
        }
    }
}
