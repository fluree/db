use std::collections::VecDeque;

use anyhow::{Context, Result};
use fluree_graph_ir::{Graph, GraphCollectorSink, Term};
use fluree_graph_turtle::parse;

use crate::files::{read_file_to_string, resolve_relative_iri};
use crate::vocab::{mf, qt, rdf, rdfs, rdft};

/// A single W3C test case extracted from a manifest.
#[derive(Debug)]
pub struct Test {
    /// Test IRI (unique identifier).
    pub id: String,
    /// Test type URIs (e.g., `mf:PositiveSyntaxTest11`).
    pub kinds: Vec<String>,
    /// Human-readable test name.
    pub name: Option<String>,
    /// Test comment/description.
    pub comment: Option<String>,
    /// Action URL (for syntax tests: the query file).
    pub action: Option<String>,
    /// Query file URL (for evaluation tests).
    pub query: Option<String>,
    /// Default graph data URL.
    pub data: Option<String>,
    /// Named graph data: (graph_name, data_url).
    pub graph_data: Vec<(String, String)>,
    /// Expected result URL.
    pub result: Option<String>,
}

/// Iterator over W3C test cases, loading manifests lazily.
pub struct TestManifest {
    graph: Graph,
    tests_to_do: VecDeque<String>,
    manifests_to_do: VecDeque<String>,
}

impl TestManifest {
    /// Create a new manifest iterator from one or more top-level manifest URLs.
    pub fn new(manifest_urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            graph: Graph::new(),
            tests_to_do: VecDeque::new(),
            manifests_to_do: manifest_urls.into_iter().map(|u| u.into()).collect(),
        }
    }
}

impl Iterator for TestManifest {
    type Item = Result<Test>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to yield the next test from the current manifest
            if let Some(test_id) = self.tests_to_do.pop_front() {
                match self.parse_test(&test_id) {
                    Ok(Some(test)) => return Some(Ok(test)),
                    Ok(None) => continue, // Rejected test, skip it
                    Err(e) => return Some(Err(e)),
                }
            }

            // No more tests in current manifest — load the next one
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
    /// Load a manifest file, extracting its entries and included sub-manifests.
    fn load_manifest(&mut self, url: &str) -> Result<()> {
        let raw_content =
            read_file_to_string(url).with_context(|| format!("Reading manifest {url}"))?;

        // Prepend @base directive so relative IRIs (including <>) resolve correctly.
        // The manifest files use <> to refer to themselves, which requires a base.
        let content = format!("@base <{url}> .\n{raw_content}");

        let mut sink = GraphCollectorSink::new();
        parse(&content, &mut sink).with_context(|| format!("Parsing manifest {url}"))?;

        // NOTE: This replaces (not accumulates) the graph. This is intentional:
        // `tests_to_do` captures test IDs as strings, and `parse_test()` runs for
        // each test ID *before* the next `load_manifest()` call (enforced by the
        // `loop` in `Iterator::next`). Cross-manifest test references are not
        // supported by the W3C test suite structure.
        self.graph = sink.finish();

        // Find the manifest subject (type mf:Manifest or has mf:entries/mf:include)
        let manifest_subject = self.find_manifest_subject(url);

        // Extract mf:include — sub-manifests to load
        if let Some(ref subj) = manifest_subject {
            let includes = self.get_list_items(subj, mf::INCLUDE);
            for include_url in includes {
                let resolved = resolve_relative_iri(url, &include_url);
                self.manifests_to_do.push_back(resolved);
            }
        }

        // Extract mf:entries — test IRIs to process
        if let Some(ref subj) = manifest_subject {
            let entries = self.get_list_items(subj, mf::ENTRIES);
            for entry_id in entries {
                self.tests_to_do.push_back(entry_id);
            }
        }

        Ok(())
    }

    /// Find the manifest subject node. Look for the node typed mf:Manifest,
    /// or fall back to the base IRI.
    fn find_manifest_subject(&self, url: &str) -> Option<Term> {
        // Look for ?s rdf:type mf:Manifest
        for triple in self.graph.iter() {
            if triple.p.as_iri() == Some(rdf::TYPE) && triple.o.as_iri() == Some(mf::MANIFEST) {
                return Some(triple.s.clone());
            }
        }
        // Fall back: look for any subject that has mf:entries
        for triple in self.graph.iter() {
            if triple.p.as_iri() == Some(mf::ENTRIES) {
                return Some(triple.s.clone());
            }
        }
        // Last resort: use the manifest URL itself
        Some(Term::iri(url))
    }

    /// Get list items for a subject+predicate, using Fluree's list_index approach.
    ///
    /// Fluree's Turtle parser emits object-position collections as triples with
    /// `list_index` set (rather than rdf:first/rdf:rest chains).
    fn get_list_items(&self, subject: &Term, predicate: &str) -> Vec<String> {
        let mut items: Vec<(i32, String)> = self
            .graph
            .iter()
            .filter(|t| t.s == *subject && t.p.as_iri() == Some(predicate))
            .filter_map(|t| {
                let index = t.list_index.unwrap_or(0);
                let value = term_to_string(&t.o)?;
                Some((index, value))
            })
            .collect();

        items.sort_by_key(|(idx, _)| *idx);
        items.into_iter().map(|(_, v)| v).collect()
    }

    /// Parse a test entry from the loaded graph.
    /// Returns None if the test is rejected (dawgt:Rejected).
    fn parse_test(&self, test_id: &str) -> Result<Option<Test>> {
        let subject = Term::iri(test_id);

        // Check for rejection
        if let Some(approval) = self.object_for(&subject, rdft::APPROVAL) {
            if approval.as_iri() == Some(rdft::REJECTED) {
                return Ok(None);
            }
        }

        // Extract kinds (rdf:type values, excluding generic types)
        let kinds: Vec<String> = self
            .graph
            .iter()
            .filter(|t| t.s == subject && t.p.as_iri() == Some(rdf::TYPE))
            .filter_map(|t| t.o.as_iri().map(String::from))
            .filter(|iri| iri.starts_with(mf::NS))
            .collect();

        let name = self.object_for(&subject, mf::NAME).and_then(term_to_string);

        let comment = self
            .object_for(&subject, rdfs::COMMENT)
            .and_then(term_to_string);

        // Parse action — can be a simple IRI or a blank node with structured data
        let action_term = self.object_for(&subject, mf::ACTION);
        let (action, query, data, graph_data) = match action_term {
            Some(term) if term.is_iri() => {
                // Simple action: just a URL (used for syntax tests)
                (term_to_string(term), None, None, vec![])
            }
            Some(term) if term.is_blank() => {
                // Structured action: blank node with qt:query, qt:data, etc.
                let query = self.object_for(term, qt::QUERY).and_then(term_to_string);
                let data = self.object_for(term, qt::DATA).and_then(term_to_string);
                let graph_data = self.get_graph_data(term);
                (None, query, data, graph_data)
            }
            _ => (None, None, None, vec![]),
        };

        let result = self
            .object_for(&subject, mf::RESULT)
            .and_then(term_to_string);

        Ok(Some(Test {
            id: test_id.to_string(),
            kinds,
            name,
            comment,
            action,
            query,
            data,
            graph_data,
            result,
        }))
    }

    /// Find the first object for a given subject+predicate.
    fn object_for<'a>(&'a self, subject: &Term, predicate: &str) -> Option<&'a Term> {
        self.graph
            .iter()
            .find(|t| t.s == *subject && t.p.as_iri() == Some(predicate))
            .map(|t| &t.o)
    }

    /// Extract named graph data from a structured action node.
    fn get_graph_data(&self, action: &Term) -> Vec<(String, String)> {
        self.graph
            .iter()
            .filter(|t| t.s == *action && t.p.as_iri() == Some(qt::GRAPH_DATA))
            .filter_map(|t| {
                if t.o.is_iri() {
                    // Simple named graph: IRI is both the graph name and data URL
                    let url = t.o.as_iri()?.to_string();
                    Some((url.clone(), url))
                } else if t.o.is_blank() {
                    // Labeled graph data
                    let label = self
                        .object_for(&t.o, rdfs::LABEL)
                        .and_then(term_to_string)?;
                    let graph_url = term_to_string(&t.o)?;
                    Some((label, graph_url))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Convert a Term to its string representation (IRI string or literal value).
fn term_to_string(term: &Term) -> Option<String> {
    match term {
        Term::Iri(iri) => Some(iri.to_string()),
        Term::Literal { value, .. } => Some(value.lexical()),
        Term::BlankNode(_) => None,
    }
}
