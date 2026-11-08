//! RDF Dataset Canonicalization, [RDFC-1.0](https://www.w3.org/TR/rdf-canon/).
//!
//! [`canonicalize`] labels a dataset's blank nodes `c14n0`, `c14n1`, … from
//! the dataset's content alone and writes it as canonical N-Quads: two
//! datasets holding the same statements, whatever their blank node labels and
//! statement order, give the same text, ready to compare, hash or sign.
//!
//! Most datasets canonicalize in about linear time, but a poison dataset
//! (many blank nodes that no statement tells apart) can take time
//! exponential in its size. Calls to the algorithm's Hash N-Degree Quads step
//! are counted, and a dataset that needs more than its limit is refused (see
//! [`CanonicalizeOptions::max_deep_calls`]).
//!
//! RDFC-1.0 defines RDF 1.1 datasets. A triple term (RDF 1.2) is written
//! `<<( s p o )>>`, as N-Quads 1.2 writes it, and a blank node inside one is
//! hashed as a component of the quad that holds it, in the object position.

use fluree_graph_ir::datatype::iri as dt_iri;
use fluree_graph_ir::{Dataset, Term};
use sha2::{Digest, Sha256, Sha384};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

/// The hash function canonicalization uses: SHA-256 unless asked otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashAlgorithm {
    #[default]
    Sha256,
    Sha384,
}

/// The default limit on calls to Hash N-Degree Quads, per blank node of the
/// dataset, with [`MIN_DEEP_CALLS`] the least. A blank node that shares its
/// first-degree hash costs at least one call, so the limit grows with the
/// dataset. The W3C suite's hardest "poison" graphs that must canonicalize
/// take under 512 calls; its 10-node clique, which must be refused, takes far
/// more than this allows.
pub const DEEP_CALLS_PER_BLANK_NODE: usize = 16;

/// The least default limit on calls to Hash N-Degree Quads.
pub const MIN_DEEP_CALLS: usize = 1024;

/// How [`canonicalize`] runs.
#[derive(Debug, Clone, Copy, Default)]
pub struct CanonicalizeOptions {
    pub hash: HashAlgorithm,
    /// The most calls to Hash N-Degree Quads, recursive calls included,
    /// before the dataset is refused as too complex; by default
    /// [`DEEP_CALLS_PER_BLANK_NODE`] per blank node, at least
    /// [`MIN_DEEP_CALLS`].
    pub max_deep_calls: Option<usize>,
}

/// A canonicalized dataset.
#[derive(Debug, Clone)]
pub struct Canonical {
    /// The dataset, its blank nodes relabeled canonically and its duplicate
    /// statements gone.
    pub dataset: Dataset,
    /// The canonical N-Quads: one line per quad, in code point order.
    pub nquads: String,
    /// Each blank node label of the input and its canonical label, in the
    /// order they were issued.
    pub issued: Vec<(String, String)>,
}

/// Why a dataset could not be canonicalized.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonicalizeError {
    #[error(
        "the dataset is too complex to canonicalize: telling its blank nodes apart takes more \
         than {0} deep comparisons"
    )]
    TooComplex(usize),
}

/// Canonicalize `dataset` (RDFC-1.0).
pub fn canonicalize(
    dataset: &Dataset,
    options: &CanonicalizeOptions,
) -> Result<Canonical, CanonicalizeError> {
    let quads = quads_of(dataset);
    let mut state = State::new(&quads, options);

    // Each blank node's first-degree hash; a hash that names one blank node
    // gives it its canonical label, in hash order.
    let mut by_hash: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    let labels = state.labels.clone();
    for label in labels {
        let hash = state.first_degree(label);
        by_hash.entry(hash).or_default().push(label);
    }
    let mut shared = Vec::new();
    for (hash, labels) in by_hash {
        match labels.as_slice() {
            [label] => {
                state.canonical.issue(label);
            }
            _ => shared.push((hash, labels)),
        }
    }

    // Blank nodes sharing a first-degree hash are told apart by the paths
    // through their neighbours: label them in the order of those paths'
    // hashes, each with its neighbours in the order the path met them.
    for (_, labels) in shared {
        let mut paths = Vec::new();
        for label in labels {
            if state.canonical.get(label).is_some() {
                continue;
            }
            let mut issuer = Issuer::new("b");
            issuer.issue(label);
            paths.push(state.n_degree(label, issuer)?);
        }
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, issuer) in paths {
            for (existing, _) in &issuer.issued {
                state.canonical.issue(existing);
            }
        }
    }

    let canonical = &state.canonical;
    let relabel = |label: &str| canonical.get(label).unwrap_or(label).to_string();
    let mut lines: Vec<String> = quads.iter().map(|q| quad_line(q, &relabel)).collect();
    lines.sort_unstable();
    let mut relabeled = Dataset::new();
    for quad in &quads {
        let term = |t: &Term| relabel_term(t, canonical);
        relabeled.add_quad(
            term(&quad.s),
            term(&quad.p),
            term(&quad.o),
            quad.g.as_ref().map(term).as_ref(),
        );
    }
    Ok(Canonical {
        dataset: relabeled,
        nquads: lines.concat(),
        issued: state.canonical.issued,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Quad {
    s: Term,
    p: Term,
    o: Term,
    g: Option<Term>,
}

/// The dataset's quads, a reification as its `rdf:reifies` quad, each once.
fn quads_of(dataset: &Dataset) -> Vec<Quad> {
    let reifies = Term::iri(dt_iri::RDF_REIFIES);
    let mut seen = HashSet::new();
    let mut quads = Vec::new();
    for (name, graph) in dataset.graphs() {
        let g = name.cloned();
        let triples = graph.iter().map(|t| Quad {
            s: t.s.clone(),
            p: t.p.clone(),
            o: t.o.clone(),
            g: g.clone(),
        });
        let reifications = graph.reifications().iter().map(|r| Quad {
            s: r.reifier.clone(),
            p: reifies.clone(),
            o: Term::triple(r.triple.s.clone(), r.triple.p.clone(), r.triple.o.clone()),
            g: g.clone(),
        });
        for quad in triples.chain(reifications) {
            if seen.insert(quad.clone()) {
                quads.push(quad);
            }
        }
    }
    quads
}

/// A blank node's label: `Term::BlankNode`, or an IRI spelled `_:label` as a
/// graph read from a ledger carries one.
fn blank_label(term: &Term) -> Option<&str> {
    match term {
        Term::BlankNode(id) => Some(id.as_str()),
        Term::Iri(iri) => iri.strip_prefix("_:"),
        _ => None,
    }
}

/// The blank node labels in `term`, a triple term's included.
fn blank_labels<'t>(term: &'t Term, out: &mut Vec<&'t str>) {
    match term {
        Term::TripleTerm(parts) => parts.iter().for_each(|part| blank_labels(part, out)),
        other => out.extend(blank_label(other)),
    }
}

fn relabel_term(term: &Term, canonical: &Issuer) -> Term {
    match term {
        Term::TripleTerm(parts) => {
            let [s, p, o] = &**parts;
            Term::triple(
                relabel_term(s, canonical),
                relabel_term(p, canonical),
                relabel_term(o, canonical),
            )
        }
        other => match blank_label(other) {
            Some(label) => Term::blank(canonical.get(label).unwrap_or(label)),
            None => other.clone(),
        },
    }
}

/// Issues identifiers `{prefix}0`, `{prefix}1`, … to blank nodes, each once,
/// remembering the order it issued them in.
#[derive(Clone)]
struct Issuer {
    prefix: &'static str,
    issued: Vec<(String, String)>,
    index: HashMap<String, usize>,
}

impl Issuer {
    fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            issued: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn get(&self, existing: &str) -> Option<&str> {
        self.index.get(existing).map(|&i| self.issued[i].1.as_str())
    }

    fn issue(&mut self, existing: &str) -> String {
        if let Some(id) = self.get(existing) {
            return id.to_string();
        }
        let id = format!("{}{}", self.prefix, self.issued.len());
        self.index.insert(existing.to_string(), self.issued.len());
        self.issued.push((existing.to_string(), id.clone()));
        id
    }
}

struct State<'q> {
    /// The blank nodes, in the order the quads first mention them, so that
    /// nodes no hash tells apart are labeled in the input's order.
    labels: Vec<&'q str>,
    /// Each blank node and the quads that mention it.
    mentions: HashMap<&'q str, Vec<&'q Quad>>,
    first_degree: HashMap<&'q str, String>,
    canonical: Issuer,
    hash: HashAlgorithm,
    deep_calls: usize,
    max_deep_calls: usize,
}

impl<'q> State<'q> {
    fn new(quads: &'q [Quad], options: &CanonicalizeOptions) -> Self {
        let mut mentions: HashMap<&'q str, Vec<&'q Quad>> = HashMap::new();
        let mut order = Vec::new();
        for quad in quads {
            let mut labels = Vec::new();
            for component in [&quad.s, &quad.o].into_iter().chain(&quad.g) {
                blank_labels(component, &mut labels);
            }
            let mut seen_here = HashSet::new();
            for label in labels {
                if !seen_here.insert(label) {
                    continue;
                }
                let quads = mentions.entry(label).or_default();
                if quads.is_empty() {
                    order.push(label);
                }
                quads.push(quad);
            }
        }
        Self {
            max_deep_calls: options
                .max_deep_calls
                .unwrap_or_else(|| (order.len() * DEEP_CALLS_PER_BLANK_NODE).max(MIN_DEEP_CALLS)),
            labels: order,
            mentions,
            first_degree: HashMap::new(),
            canonical: Issuer::new("c14n"),
            hash: options.hash,
            deep_calls: 0,
        }
    }

    fn digest(&self, data: &str) -> String {
        let bytes: Vec<u8> = match self.hash {
            HashAlgorithm::Sha256 => Sha256::digest(data.as_bytes()).to_vec(),
            HashAlgorithm::Sha384 => Sha384::digest(data.as_bytes()).to_vec(),
        };
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }

    /// Hash First Degree Quads: the hash of the quads that mention
    /// `reference`, it written `_:a` and every other blank node `_:z`.
    fn first_degree(&mut self, reference: &'q str) -> String {
        if let Some(hash) = self.first_degree.get(reference) {
            return hash.clone();
        }
        let blind = |label: &str| if label == reference { "a" } else { "z" }.to_string();
        let mut lines: Vec<String> = self.mentions[reference]
            .iter()
            .map(|quad| quad_line(quad, &blind))
            .collect();
        lines.sort_unstable();
        let hash = self.digest(&lines.concat());
        self.first_degree.insert(reference, hash.clone());
        hash
    }

    /// Hash Related Blank Node: `related`'s hash as seen from a quad it
    /// shares in `position` (`s`, `o` or `g`).
    fn related(
        &mut self,
        related: &'q str,
        quad: &Quad,
        issuer: &Issuer,
        position: char,
    ) -> String {
        let mut input = String::from(position);
        if position != 'g' {
            push_term(&mut input, &quad.p, &|label| label.to_string());
        }
        if let Some(id) = self.canonical.get(related) {
            input.push_str("_:");
            input.push_str(id);
        } else if let Some(id) = issuer.get(related) {
            input.push_str("_:");
            input.push_str(id);
        } else {
            input.push_str(&self.first_degree(related));
        }
        self.digest(&input)
    }

    /// Hash N-Degree Quads: a hash of the paths from `identifier` through the
    /// blank nodes it shares quads with, choosing for each group of
    /// indistinguishable neighbours the order whose path is least, and the
    /// issuer those paths labeled them with.
    fn n_degree(
        &mut self,
        identifier: &'q str,
        mut issuer: Issuer,
    ) -> Result<(String, Issuer), CanonicalizeError> {
        self.deep_calls += 1;
        if self.deep_calls > self.max_deep_calls {
            return Err(CanonicalizeError::TooComplex(self.max_deep_calls));
        }
        let mut related_by_hash: BTreeMap<String, Vec<&'q str>> = BTreeMap::new();
        let quads = self.mentions[identifier].clone();
        for quad in quads {
            let positions = [
                (Some(&quad.s), 's'),
                (Some(&quad.o), 'o'),
                (quad.g.as_ref(), 'g'),
            ];
            for (component, position) in positions {
                let mut labels = Vec::new();
                if let Some(component) = component {
                    blank_labels(component, &mut labels);
                }
                for related in labels.into_iter().filter(|label| *label != identifier) {
                    let hash = self.related(related, quad, &issuer, position);
                    related_by_hash.entry(hash).or_default().push(related);
                }
            }
        }

        let mut data = String::new();
        for (related_hash, blank_nodes) in related_by_hash {
            data.push_str(&related_hash);
            let mut chosen_path = String::new();
            let mut chosen_issuer = None;
            for permutation in permutations(&blank_nodes) {
                let Some((path, issuer_copy)) = self.path(&permutation, &issuer, &chosen_path)?
                else {
                    continue;
                };
                if chosen_path.is_empty() || path < chosen_path {
                    chosen_path = path;
                    chosen_issuer = Some(issuer_copy);
                }
            }
            data.push_str(&chosen_path);
            if let Some(chosen) = chosen_issuer {
                issuer = chosen;
            }
        }
        Ok((self.digest(&data), issuer))
    }

    /// One permutation's path and the issuer it leaves, or `None` once the
    /// path cannot be less than `chosen_path`.
    fn path(
        &mut self,
        permutation: &[&'q str],
        issuer: &Issuer,
        chosen_path: &str,
    ) -> Result<Option<(String, Issuer)>, CanonicalizeError> {
        let beaten = |path: &str| {
            !chosen_path.is_empty() && path.len() >= chosen_path.len() && path > chosen_path
        };
        let mut issuer_copy = issuer.clone();
        let mut path = String::new();
        let mut recursion = Vec::new();
        for &related in permutation {
            if let Some(id) = self.canonical.get(related) {
                path.push_str("_:");
                path.push_str(id);
            } else {
                if issuer_copy.get(related).is_none() {
                    recursion.push(related);
                }
                path.push_str("_:");
                path.push_str(&issuer_copy.issue(related));
            }
            if beaten(&path) {
                return Ok(None);
            }
        }
        for related in recursion {
            let (hash, result_issuer) = self.n_degree(related, issuer_copy.clone())?;
            path.push_str("_:");
            path.push_str(&issuer_copy.issue(related));
            path.push('<');
            path.push_str(&hash);
            path.push('>');
            issuer_copy = result_issuer;
            if beaten(&path) {
                return Ok(None);
            }
        }
        Ok(Some((path, issuer_copy)))
    }
}

/// Every ordering of `items`, duplicates included, as the algorithm asks,
/// made one at a time: a group of n indistinguishable neighbours has n!.
struct Permutations<'i, 'a> {
    items: &'i [&'a str],
    order: Vec<usize>,
    started: bool,
}

fn permutations<'i, 'a>(items: &'i [&'a str]) -> Permutations<'i, 'a> {
    Permutations {
        items,
        order: (0..items.len()).collect(),
        started: false,
    }
}

impl<'a> Iterator for Permutations<'_, 'a> {
    type Item = Vec<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            // The next order in lexicographic order of the indices.
            let order = &mut self.order;
            let pivot = (1..order.len()).rev().find(|&i| order[i - 1] < order[i])?;
            let swap = (pivot..order.len())
                .rev()
                .find(|&j| order[j] > order[pivot - 1])?;
            order.swap(pivot - 1, swap);
            order[pivot..].reverse();
        }
        self.started = true;
        Some(self.order.iter().map(|&i| self.items[i]).collect())
    }
}

/// `quad` in canonical N-Quads, its blank nodes labeled by `label`.
fn quad_line(quad: &Quad, label: &dyn Fn(&str) -> String) -> String {
    let mut line = String::new();
    push_term(&mut line, &quad.s, label);
    line.push(' ');
    push_term(&mut line, &quad.p, label);
    line.push(' ');
    push_term(&mut line, &quad.o, label);
    if let Some(g) = &quad.g {
        line.push(' ');
        push_term(&mut line, g, label);
    }
    line.push_str(" .\n");
    line
}

/// One term in canonical N-Quads: IRIs and text as they are, a literal's
/// quotes, backslashes and control characters escaped, and `xsd:string`
/// left implicit.
fn push_term(out: &mut String, term: &Term, label: &dyn Fn(&str) -> String) {
    if let Some(blank) = blank_label(term) {
        out.push_str("_:");
        out.push_str(&label(blank));
        return;
    }
    match term {
        Term::Iri(iri) => {
            out.push('<');
            out.push_str(iri);
            out.push('>');
        }
        Term::Literal {
            value,
            datatype,
            language,
        } => {
            out.push('"');
            push_escaped(out, &value.lexical());
            out.push('"');
            if let Some(language) = language {
                out.push('@');
                out.push_str(language);
            } else if datatype.as_iri() != dt_iri::XSD_STRING {
                out.push_str("^^<");
                out.push_str(datatype.as_iri());
                out.push('>');
            }
        }
        Term::TripleTerm(parts) => {
            out.push_str("<<( ");
            for part in parts.iter() {
                push_term(out, part, label);
                out.push(' ');
            }
            out.push_str(")>>");
        }
        Term::BlankNode(_) => unreachable!("handled as a blank label above"),
    }
}

/// Canonical N-Quads string escaping: `\b \t \n \f \r \" \\`, `\uXXXX`
/// (uppercase) for the other control characters, and nothing else.
fn push_escaped(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\u{0}'..='\u{1f}' | '\u{7f}' => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            other => out.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(nquads: &str) -> Dataset {
        let mut sink = fluree_graph_ir::GraphCollectorSink::with_named_graphs();
        fluree_graph_turtle::parse_nquads(nquads, &mut sink).unwrap();
        sink.into_dataset()
    }

    fn canonical(nquads: &str) -> Canonical {
        canonicalize(&dataset(nquads), &CanonicalizeOptions::default()).unwrap()
    }

    /// Two writings of one dataset, with other blank node labels, another
    /// statement order and a duplicate, give the same canonical N-Quads.
    #[test]
    fn relabeled_and_reordered_datasets_canonicalize_alike() {
        let one = "_:x <http://e/p> _:y .\n_:y <http://e/q> \"v\" <http://e/g> .\n";
        let two = "_:m <http://e/q> \"v\" <http://e/g> .\n_:k <http://e/p> _:m .\n_:k <http://e/p> _:m .\n";
        let (one, two) = (canonical(one), canonical(two));
        assert_eq!(one.nquads, two.nquads);
        assert_eq!(
            one.nquads,
            "_:c14n0 <http://e/q> \"v\" <http://e/g> .\n_:c14n1 <http://e/p> _:c14n0 .\n"
        );
        assert_eq!(two.dataset.len(), 2, "the duplicate is gone");
        assert_eq!(
            one.issued,
            [("y".into(), "c14n0".into()), ("x".into(), "c14n1".into())]
        );
    }

    /// A triple term's blank nodes are labeled with the quad's, and it is
    /// written in N-Quads 1.2's form.
    #[test]
    fn a_triple_term_is_canonicalized_whole() {
        let doc = "_:r <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( _:s <http://e/p> \"o\" )>> .\n\
                   _:s <http://e/p> \"o\" .\n";
        let c = canonical(doc);
        assert!(c.nquads.contains("<<( _:c14n"), "{}", c.nquads);
        assert!(
            !c.nquads.contains("_:r") && !c.nquads.contains("_:s "),
            "{}",
            c.nquads
        );
    }

    #[test]
    fn literals_are_escaped_canonically() {
        let doc = "<http://e/s> <http://e/p> \"tab\\there\\u0001 \\u00E9 'q' \\\"\" .\n";
        assert_eq!(
            canonical(doc).nquads,
            "<http://e/s> <http://e/p> \"tab\\there\\u0001 é 'q' \\\"\" .\n"
        );
    }

    /// A dataset whose blank nodes need more deep comparisons than allowed
    /// is refused rather than worked on without end.
    #[test]
    fn too_complex_a_dataset_is_refused() {
        let ring: String = (0..6)
            .map(|i| format!("_:n{i} <http://e/p> _:n{} .\n", (i + 1) % 6))
            .collect();
        let tight = CanonicalizeOptions {
            max_deep_calls: Some(2),
            ..CanonicalizeOptions::default()
        };
        assert_eq!(
            canonicalize(&dataset(&ring), &tight).unwrap_err(),
            CanonicalizeError::TooComplex(2)
        );
        assert!(canonicalize(&dataset(&ring), &CanonicalizeOptions::default()).is_ok());
    }
}
