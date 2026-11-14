//! RDF dataset isomorphism: a bijection between the blank nodes of two
//! datasets that makes them equal.
//!
//! Blank nodes are first partitioned by colour refinement (each one's colour
//! is a hash of the quads it appears in, with every other blank node replaced
//! by its colour, iterated until the partition stops splitting); the search
//! then only pairs nodes of the same colour, and checks each quad as soon as
//! all of its blank nodes are paired.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum T {
    Iri(String),
    Blank(String),
    Literal {
        lexical: String,
        datatype: String,
        language: Option<String>,
    },
    Triple(Box<[T; 3]>),
}

/// Subject, predicate, object, graph name (`None` for the default graph).
pub type Quad = (T, T, T, Option<T>);

pub fn show(quads: &BTreeSet<Quad>) -> String {
    quads
        .iter()
        .map(|(s, p, o, g)| match g {
            Some(g) => format!("    {} {} {} {} .", nq(s), nq(p), nq(o), nq(g)),
            None => format!("    {} {} {} .", nq(s), nq(p), nq(o)),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn nq(t: &T) -> String {
    match t {
        T::Iri(iri) => format!("<{iri}>"),
        T::Blank(label) => format!("_:{}", label.trim_start_matches("_:").replace('\u{1}', "")),
        T::Literal {
            lexical,
            datatype,
            language,
        } => match language {
            Some(lang) => format!("{lexical:?}@{lang}"),
            None => format!("{lexical:?}^^<{datatype}>"),
        },
        T::Triple(spo) => format!("<<( {} {} {} )>>", nq(&spo[0]), nq(&spo[1]), nq(&spo[2])),
    }
}

fn quad_terms(q: &Quad) -> impl Iterator<Item = &T> {
    [&q.0, &q.1, &q.2].into_iter().chain(q.3.as_ref())
}

fn blanks_in<'a>(t: &'a T, out: &mut Vec<&'a str>) {
    match t {
        T::Blank(label) => out.push(label),
        T::Triple(spo) => spo.iter().for_each(|t| blanks_in(t, out)),
        _ => {}
    }
}

fn quad_blanks(q: &Quad) -> Vec<&str> {
    let mut out = Vec::new();
    quad_terms(q).for_each(|t| blanks_in(t, &mut out));
    out
}

fn map_term(t: &T, f: &impl Fn(&str) -> T) -> T {
    match t {
        T::Blank(label) => f(label),
        T::Triple(spo) => T::Triple(Box::new([
            map_term(&spo[0], f),
            map_term(&spo[1], f),
            map_term(&spo[2], f),
        ])),
        other => other.clone(),
    }
}

fn map_quad(q: &Quad, f: &impl Fn(&str) -> T) -> Quad {
    (
        map_term(&q.0, f),
        map_term(&q.1, f),
        map_term(&q.2, f),
        q.3.as_ref().map(|g| map_term(g, f)),
    )
}

fn hash_of(value: &impl Hash) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

/// Each blank node's colour, refined until the partition is stable.
fn colours(quads: &BTreeSet<Quad>) -> HashMap<String, u64> {
    let mut by_blank: BTreeMap<&str, Vec<&Quad>> = BTreeMap::new();
    for q in quads {
        for b in quad_blanks(q) {
            by_blank.entry(b).or_default().push(q);
        }
    }
    let mut colour: HashMap<String, u64> = by_blank.keys().map(|b| (b.to_string(), 0)).collect();
    let mut classes = 1;
    loop {
        let next: HashMap<String, u64> = by_blank
            .iter()
            .map(|(b, qs)| {
                let mut signature: Vec<u64> = qs
                    .iter()
                    .map(|q| {
                        hash_of(&map_quad(q, &|label| {
                            if label == *b {
                                T::Iri("\u{1}self".to_string())
                            } else {
                                T::Iri(format!("\u{1}{}", colour[label]))
                            }
                        }))
                    })
                    .collect();
                signature.sort_unstable();
                (b.to_string(), hash_of(&(colour[*b], signature)))
            })
            .collect();
        let next_classes = next.values().collect::<BTreeSet<_>>().len();
        colour = next;
        if next_classes == classes {
            return colour;
        }
        classes = next_classes;
    }
}

pub fn isomorphic(a: &BTreeSet<Quad>, b: &BTreeSet<Quad>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let (ca, cb) = (colours(a), colours(b));
    let mut sa: Vec<u64> = ca.values().copied().collect();
    let mut sb: Vec<u64> = cb.values().copied().collect();
    sa.sort_unstable();
    sb.sort_unstable();
    if sa != sb {
        return false;
    }

    // Pair the rarest colours first: they leave the fewest choices.
    let mut count: HashMap<u64, usize> = HashMap::new();
    sa.iter().for_each(|c| *count.entry(*c).or_default() += 1);
    let mut order: Vec<&String> = ca.keys().collect();
    order.sort_by_key(|blank| (count[&ca[*blank]], ca[*blank], (*blank).clone()));

    let mut mapping: HashMap<String, String> = HashMap::new();
    let mut used: BTreeSet<String> = BTreeSet::new();
    search(a, b, &ca, &cb, &order, &mut mapping, &mut used)
}

fn search(
    a: &BTreeSet<Quad>,
    b: &BTreeSet<Quad>,
    ca: &HashMap<String, u64>,
    cb: &HashMap<String, u64>,
    order: &[&String],
    mapping: &mut HashMap<String, String>,
    used: &mut BTreeSet<String>,
) -> bool {
    let Some((next, rest)) = order.split_first() else {
        return a
            .iter()
            .all(|q| b.contains(&map_quad(q, &|l| T::Blank(mapping[l].clone()))));
    };
    for (candidate, colour) in cb {
        if *colour != ca[*next] || used.contains(candidate) {
            continue;
        }
        mapping.insert((*next).clone(), candidate.clone());
        used.insert(candidate.clone());
        let consistent = a.iter().all(|q| {
            let blanks = quad_blanks(q);
            !blanks.contains(&next.as_str())
                || !blanks.iter().all(|l| mapping.contains_key(*l))
                || b.contains(&map_quad(q, &|l| T::Blank(mapping[l].clone())))
        });
        if consistent && search(a, b, ca, cb, rest, mapping, used) {
            return true;
        }
        mapping.remove(*next);
        used.remove(candidate);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> T {
        T::Iri(s.to_string())
    }
    fn blank(s: &str) -> T {
        T::Blank(s.to_string())
    }

    #[test]
    fn relabelled_blank_nodes_are_isomorphic() {
        let a: BTreeSet<Quad> = [
            (blank("x"), iri("p"), blank("y"), None),
            (blank("y"), iri("p"), iri("o"), None),
        ]
        .into();
        let b: BTreeSet<Quad> = [
            (blank("m"), iri("p"), blank("n"), None),
            (blank("n"), iri("p"), iri("o"), None),
        ]
        .into();
        assert!(isomorphic(&a, &b));
    }

    #[test]
    fn a_different_shape_is_not() {
        let a: BTreeSet<Quad> = [
            (blank("x"), iri("p"), blank("y"), None),
            (blank("y"), iri("p"), iri("o"), None),
        ]
        .into();
        let b: BTreeSet<Quad> = [
            (blank("m"), iri("p"), blank("n"), None),
            (blank("m"), iri("p"), iri("o"), None),
        ]
        .into();
        assert!(!isomorphic(&a, &b));
    }

    #[test]
    fn symmetric_blank_nodes_still_pair() {
        // A cycle of identical nodes: colour refinement cannot split it, so
        // the search has to.
        let ring = |n: [&str; 3]| -> BTreeSet<Quad> {
            (0..3)
                .map(|i| (blank(n[i]), iri("p"), blank(n[(i + 1) % 3]), None))
                .collect()
        };
        assert!(isomorphic(&ring(["a", "b", "c"]), &ring(["z", "y", "x"])));
    }
}
