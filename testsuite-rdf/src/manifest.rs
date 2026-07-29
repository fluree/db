//! W3C rdf-tests manifest reader — parsed with Fluree's own Turtle parser.
//!
//! Dogfooding is the point: the manifests are Turtle, and a harness that
//! parsed them with a third-party reader could report a green suite while the
//! parser under test could not read the suite's own index. If a manifest
//! construct ever defeats the parser, that is a finding, not an excuse for a
//! fallback reader.
//!
//! Manifests are read in [`ParserOptions::conformant`] mode, so `mf:entries`
//! arrives as a real `rdf:first`/`rdf:rest` spine and [`list_items`] walks it.
//! That makes loading a manifest the first exercise of the conformant
//! collection path on every run.

use std::collections::{HashMap, VecDeque};

use anyhow::{Context, Result};
use fluree_graph_ir::{GraphCollectorSink, Term, Triple};
use fluree_graph_turtle::{parse_with_options, ParserOptions};

use crate::files::{read_file_to_string, resolve_relative_iri};
use crate::vocab::{mf, rdf, rdfs, rdft};

/// A single test case extracted from a manifest.
#[derive(Debug, Clone)]
pub struct Test {
    /// Test IRI — the identifier used by the skip register.
    pub id: String,
    /// `rdf:type` values in the `mf:` or `rdft:` namespaces.
    pub kinds: Vec<String>,
    /// `mf:name`.
    pub name: Option<String>,
    /// `rdfs:comment`.
    pub comment: Option<String>,
    /// `mf:action` — the document to parse (absolute URL).
    pub action: Option<String>,
    /// `mf:result` — the gold N-Triples document, on eval tests.
    pub result: Option<String>,
    /// Base IRI the action document is parsed against: the suite's
    /// `mf:assumedTestBase` joined with the action's file name, which for
    /// every suite in rdf-tests equals the action's own published URL.
    pub base: Option<String>,
    /// URL of the manifest this test came from — the suite label in reports.
    pub manifest: String,
}

impl Test {
    /// Whether the test declares the given `rdf:type`.
    pub fn is_kind(&self, kind: &str) -> bool {
        self.kinds.iter().any(|k| k == kind)
    }
}

/// Iterator over manifest test cases, loading `mf:include`d manifests lazily.
pub struct TestManifest {
    /// Triples of the manifest currently loaded, indexed by subject.
    by_subject: HashMap<Term, Vec<Triple>>,
    tests_to_do: VecDeque<String>,
    manifests_to_do: VecDeque<String>,
    /// URL of the manifest currently loaded.
    current_manifest: String,
    /// `mf:assumedTestBase` of the manifest currently loaded.
    assumed_base: Option<String>,
}

impl TestManifest {
    /// Create an iterator over one or more top-level manifest URLs.
    pub fn new(manifest_urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            by_subject: HashMap::new(),
            tests_to_do: VecDeque::new(),
            manifests_to_do: manifest_urls.into_iter().map(|u| u.into()).collect(),
            current_manifest: String::new(),
            assumed_base: None,
        }
    }
}

impl Iterator for TestManifest {
    type Item = Result<Test>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(test_id) = self.tests_to_do.pop_front() {
                match self.parse_test(&test_id) {
                    Ok(Some(test)) => return Some(Ok(test)),
                    Ok(None) => continue, // withdrawn/rejected
                    Err(e) => return Some(Err(e)),
                }
            }

            let manifest_url = self.manifests_to_do.pop_front()?;
            if let Err(e) = self.load_manifest(&manifest_url) {
                return Some(Err(
                    e.context(format!("Failed to load manifest: {manifest_url}"))
                ));
            }
        }
    }
}

impl TestManifest {
    fn load_manifest(&mut self, url: &str) -> Result<()> {
        let raw = read_file_to_string(url).with_context(|| format!("Reading manifest {url}"))?;

        // `<>` in a manifest denotes the manifest itself, and every entry is a
        // fragment of it, so the document base has to be the manifest's URL.
        let content = format!("@base <{url}> .\n{raw}");

        let mut sink = GraphCollectorSink::new();
        parse_with_options(&content, &mut sink, ParserOptions::conformant())
            .with_context(|| format!("Parsing manifest {url} with Fluree's Turtle parser"))?;

        // Replacing (not accumulating) is safe: `tests_to_do` holds IRI
        // strings and is drained before the next manifest loads (the `loop` in
        // `Iterator::next` enforces it), and rdf-tests has no cross-manifest
        // test references.
        self.by_subject.clear();
        for triple in sink.into_graph().into_triples() {
            self.by_subject
                .entry(triple.s.clone())
                .or_default()
                .push(triple);
        }
        self.current_manifest = url.to_string();

        let manifest_subject = self.find_manifest_subject(url);

        self.assumed_base = manifest_subject
            .as_ref()
            .and_then(|s| self.object_for(s, mf::ASSUMED_TEST_BASE))
            .and_then(term_to_string);

        if let Some(subj) = &manifest_subject {
            for include in self.list_items(subj, mf::INCLUDE) {
                self.manifests_to_do
                    .push_back(resolve_relative_iri(url, &include));
            }
            for entry in self.list_items(subj, mf::ENTRIES) {
                self.tests_to_do.push_back(entry);
            }
        }

        Ok(())
    }

    /// The node typed `mf:Manifest`, falling back to whatever carries
    /// `mf:entries`, then to the document IRI.
    fn find_manifest_subject(&self, url: &str) -> Option<Term> {
        for (subject, triples) in &self.by_subject {
            if triples
                .iter()
                .any(|t| t.p.as_iri() == Some(rdf::TYPE) && t.o.as_iri() == Some(mf::MANIFEST))
            {
                return Some(subject.clone());
            }
        }
        for (subject, triples) in &self.by_subject {
            if triples.iter().any(|t| t.p.as_iri() == Some(mf::ENTRIES)) {
                return Some(subject.clone());
            }
        }
        Some(Term::iri(url))
    }

    /// Walk an `rdf:first`/`rdf:rest` collection to its members.
    ///
    /// Under [`ParserOptions::conformant`] a Turtle collection IS a spine, so
    /// this is plain RDF list traversal. The `visited` guard keeps a cyclic
    /// or malformed list from hanging the harness.
    fn list_items(&self, subject: &Term, predicate: &str) -> Vec<String> {
        let Some(head) = self.object_for(subject, predicate) else {
            return Vec::new();
        };

        let mut items = Vec::new();
        let mut node = head.clone();
        let mut visited = std::collections::HashSet::new();

        loop {
            if node.as_iri() == Some(rdf::NIL) {
                break;
            }
            if !visited.insert(node.clone()) {
                break; // cycle
            }
            match self.object_for(&node, rdf::FIRST).and_then(term_to_string) {
                Some(item) => items.push(item),
                None => break, // not a well-formed list cell
            }
            match self.object_for(&node, rdf::REST) {
                Some(rest) => node = rest.clone(),
                None => break,
            }
        }

        items
    }

    fn parse_test(&self, test_id: &str) -> Result<Option<Test>> {
        let subject = Term::iri(test_id);

        if let Some(approval) = self.object_for(&subject, rdft::APPROVAL) {
            let iri = approval.as_iri();
            if iri == Some(rdft::REJECTED) || iri == Some(rdft::WITHDRAWN) {
                return Ok(None);
            }
        }

        // Both namespaces matter here: the SPARQL manifests type their tests
        // in `mf:`, the RDF syntax manifests in `rdft:`.
        let kinds: Vec<String> = self
            .triples_of(&subject)
            .filter(|t| t.p.as_iri() == Some(rdf::TYPE))
            .filter_map(|t| t.o.as_iri().map(String::from))
            .filter(|iri| iri.starts_with(rdft::NS) || iri.starts_with(mf::NS))
            .collect();

        let name = self.object_for(&subject, mf::NAME).and_then(term_to_string);
        let comment = self
            .object_for(&subject, rdfs::COMMENT)
            .and_then(term_to_string);

        let action = self
            .object_for(&subject, mf::ACTION)
            .and_then(term_to_string)
            .map(|a| resolve_relative_iri(&self.current_manifest, &a));
        let result = self
            .object_for(&subject, mf::RESULT)
            .and_then(term_to_string)
            .map(|r| resolve_relative_iri(&self.current_manifest, &r));

        // The suite's contract is that a test file is parsed as if published
        // at `assumedTestBase/<file>`; since the manifest sits in that same
        // directory, the resolved action URL already IS that IRI. Recomposing
        // it from `assumedTestBase` keeps the two honest about each other.
        let base = action.as_ref().map(|a| match (&self.assumed_base, a) {
            (Some(dir), a) => match a.rfind('/') {
                Some(pos) => format!("{}{}", dir, &a[pos + 1..]),
                None => a.clone(),
            },
            (None, a) => a.clone(),
        });

        Ok(Some(Test {
            id: test_id.to_string(),
            kinds,
            name,
            comment,
            action,
            result,
            base,
            manifest: self.current_manifest.clone(),
        }))
    }

    fn triples_of(&self, subject: &Term) -> impl Iterator<Item = &Triple> {
        self.by_subject.get(subject).into_iter().flatten()
    }

    fn object_for(&self, subject: &Term, predicate: &str) -> Option<&Term> {
        self.triples_of(subject)
            .find(|t| t.p.as_iri() == Some(predicate))
            .map(|t| &t.o)
    }
}

fn term_to_string(term: &Term) -> Option<String> {
    match term {
        Term::Iri(iri) => Some(iri.to_string()),
        Term::Literal { value, .. } => Some(value.lexical()),
        Term::BlankNode(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TURTLE_MANIFEST: &str =
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl";

    /// The load-bearing dogfood check: our parser reads the suite's own index,
    /// and the entry list comes back through the conformant spine.
    #[test]
    fn our_parser_reads_the_turtle_manifest() {
        let tests: Vec<Test> = TestManifest::new([TURTLE_MANIFEST])
            .collect::<Result<Vec<_>>>()
            .expect("manifest must parse with Fluree's own Turtle parser");

        assert!(
            tests.len() > 250,
            "expected the full rdf11 Turtle manifest, got {} entries",
            tests.len()
        );

        let iri_subject = tests
            .iter()
            .find(|t| t.id.ends_with("#IRI_subject"))
            .expect("#IRI_subject must be discovered");
        assert!(iri_subject.is_kind(rdft::TURTLE_EVAL));
        assert_eq!(
            iri_subject.action.as_deref(),
            Some("https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/IRI_subject.ttl")
        );
        assert_eq!(
            iri_subject.base.as_deref(),
            iri_subject.action.as_deref(),
            "a test's base is its own published URL"
        );
        assert!(iri_subject.result.is_some());
    }

    #[test]
    fn every_discovered_test_carries_a_recognized_kind() {
        for test in TestManifest::new([TURTLE_MANIFEST]) {
            let test = test.unwrap();
            assert!(
                !test.kinds.is_empty(),
                "{} has no mf:/rdft: rdf:type — manifest parse regression",
                test.id
            );
        }
    }
}
