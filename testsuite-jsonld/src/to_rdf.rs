//! toRdf tests: the input, expanded and read by the JSON-LD adapter, against
//! `expect` (N-Quads) by RDF dataset isomorphism (tests/README.md).
//!
//! Both sides become [`Quad`]s the same way. Literals compare by lexical form
//! (the canonical form for a value the graph holds as a number), datatype and
//! case-folded language tag. The graph IR keeps a `@list` as indexed values of
//! its property; each one is written out as the `rdf:first` / `rdf:rest`
//! chain that it means.

use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use fluree_graph_ir::{Dataset, GraphCollectorSink, Term};
use fluree_graph_json_ld::adapter::to_graph_events;

use crate::expand::expanded_input;
use crate::iso::{isomorphic, Quad, T};
use crate::manifest::{Kind, TestCase};

const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

pub fn run(test: &TestCase) -> Result<()> {
    let result = output(test);
    match test.kind {
        Kind::Negative => match result {
            Ok(quads) => bail!(
                "expected the error '{}', got\n{}",
                test.expect_error_code.as_deref().unwrap_or("?"),
                crate::iso::show(&quads)
            ),
            Err(_) => Ok(()),
        },
        Kind::PositiveSyntax => result.map(drop),
        Kind::Positive => {
            let actual = result?;
            let path = test.expect.as_ref().expect("positive test has expect");
            let expected = expected(&std::fs::read_to_string(path)?)
                .with_context(|| format!("reading {}", path.display()))?;
            if isomorphic(&actual, &expected) {
                Ok(())
            } else {
                bail!(
                    "dataset differs\n  expected:\n{}\n  actual:\n{}",
                    crate::iso::show(&expected),
                    crate::iso::show(&actual)
                )
            }
        }
    }
}

fn output(test: &TestCase) -> Result<BTreeSet<Quad>> {
    let expanded = expanded_input(test)?;
    let mut sink = GraphCollectorSink::with_named_graphs();
    to_graph_events(&expanded, &mut sink)?;
    Ok(quads(&sink.into_dataset()))
}

fn expected(nquads: &str) -> Result<BTreeSet<Quad>> {
    let mut sink = GraphCollectorSink::with_named_graphs();
    fluree_graph_turtle::nquads::parse_nquads(nquads, &mut sink)?;
    Ok(quads(&sink.into_dataset()))
}

fn quads(dataset: &Dataset) -> BTreeSet<Quad> {
    let mut out = BTreeSet::new();
    let mut lists = 0usize;
    for (name, graph) in dataset.graphs() {
        let g = name.map(term);
        let mut quad = |s: T, p: T, o: T| out.insert((s, p, o, g.clone()));

        // Members of one list run 0, 1, 2, …; an index of 0 starts the next.
        let mut open: Vec<(T, T, Vec<T>)> = Vec::new();
        for triple in graph.iter() {
            let (s, p, o) = (term(&triple.s), term(&triple.p), term(&triple.o));
            match triple.list_index {
                None => {
                    quad(s, p, o);
                }
                Some(0) => open.push((s, p, vec![o])),
                Some(_) => match open
                    .iter_mut()
                    .rev()
                    .find(|(ls, lp, _)| *ls == s && *lp == p)
                {
                    Some(list) => list.2.push(o),
                    None => open.push((s, p, vec![o])),
                },
            }
        }
        for (s, p, members) in open {
            // `\u{1}` cannot begin a blank node label in either input.
            let nodes: Vec<T> = (0..members.len())
                .map(|i| T::Blank(format!("\u{1}list{lists}.{i}")))
                .collect();
            lists += 1;
            quad(s, p, nodes[0].clone());
            for (i, member) in members.into_iter().enumerate() {
                let rest = nodes
                    .get(i + 1)
                    .cloned()
                    .unwrap_or_else(|| T::Iri(format!("{RDF}nil")));
                quad(nodes[i].clone(), T::Iri(format!("{RDF}first")), member);
                quad(nodes[i].clone(), T::Iri(format!("{RDF}rest")), rest);
            }
        }

        for reification in graph.reifications() {
            let t = &reification.triple;
            let triple = T::Triple(Box::new([term(&t.s), term(&t.p), term(&t.o)]));
            quad(
                term(&reification.reifier),
                T::Iri(format!("{RDF}reifies")),
                triple,
            );
        }
    }
    out
}

fn term(t: &Term) -> T {
    match t {
        Term::Iri(iri) => T::Iri(iri.to_string()),
        Term::BlankNode(b) => T::Blank(b.as_str().to_string()),
        Term::Literal {
            value,
            datatype,
            language,
        } => T::Literal {
            lexical: value.lexical(),
            datatype: datatype.as_iri().to_string(),
            language: language.as_deref().map(str::to_ascii_lowercase),
        },
        Term::TripleTerm(spo) => T::Triple(Box::new([term(&spo[0]), term(&spo[1]), term(&spo[2])])),
    }
}
