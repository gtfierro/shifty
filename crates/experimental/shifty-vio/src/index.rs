//! An index over the reference graph, and path evaluation over it.

use crate::nt::{self, RDF_TYPE, Triple};
use shifty_algebra::Path;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

/// Adjacency in both directions. Every list is sorted, so iteration order
/// never depends on hashing.
#[derive(Debug, Default)]
pub(crate) struct Index {
    pub triples: Rc<BTreeSet<Triple>>,
    out: HashMap<String, HashMap<String, Vec<String>>>,
    inc: HashMap<String, HashMap<String, Vec<String>>>,
    objects: HashMap<String, Vec<String>>,
    predicates: Vec<String>,
    typed: Vec<String>,
}

const EMPTY: &[String] = &[];

impl Index {
    pub fn new(triples: BTreeSet<Triple>) -> Self {
        let mut out: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut inc: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut objects: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut typed = BTreeSet::new();
        // `triples` is sorted by subject, then predicate, then object, so
        // each `out` list is sorted as it is built.
        for (s, p, o) in &triples {
            out.entry(s.clone())
                .or_default()
                .entry(p.clone())
                .or_default()
                .push(o.clone());
            inc.entry(o.clone())
                .or_default()
                .entry(p.clone())
                .or_default()
                .push(s.clone());
            objects.entry(p.clone()).or_default().insert(o.clone());
            if p == RDF_TYPE {
                typed.insert(s.clone());
            }
        }
        for preds in inc.values_mut() {
            for subjects in preds.values_mut() {
                subjects.sort();
            }
        }
        let mut predicates: Vec<String> = objects.keys().cloned().collect();
        predicates.sort();
        Self {
            triples: Rc::new(triples),
            out,
            inc,
            objects: objects
                .into_iter()
                .map(|(p, os)| (p, os.into_iter().collect()))
                .collect(),
            predicates,
            typed: typed.into_iter().collect(),
        }
    }

    pub fn contains(&self, t: &Triple) -> bool {
        self.triples.contains(t)
    }

    /// Whether `node` is the subject of any triple.
    pub fn has_outgoing(&self, node: &str) -> bool {
        self.out.contains_key(node)
    }

    pub fn predicates(&self) -> &[String] {
        &self.predicates
    }

    /// Every object of `pred`, sorted.
    pub fn objects_of(&self, pred: &str) -> &[String] {
        self.objects.get(pred).map_or(EMPTY, Vec::as_slice)
    }

    fn outgoing(&self, node: &str, pred: &str) -> &[String] {
        self.out
            .get(node)
            .and_then(|m| m.get(pred))
            .map_or(EMPTY, Vec::as_slice)
    }

    fn incoming(&self, node: &str, pred: &str) -> &[String] {
        self.inc
            .get(node)
            .and_then(|m| m.get(pred))
            .map_or(EMPTY, Vec::as_slice)
    }

    pub fn types_of(&self, node: &str) -> &[String] {
        self.outgoing(node, RDF_TYPE)
    }

    pub fn instances_of(&self, class: &str) -> &[String] {
        self.incoming(class, RDF_TYPE)
    }

    pub fn typed_nodes(&self) -> &[String] {
        &self.typed
    }

    pub fn eval_path(&self, node: &str, path: &Path) -> BTreeSet<String> {
        self.step(&BTreeSet::from([node.to_owned()]), path)
    }

    fn step(&self, nodes: &BTreeSet<String>, path: &Path) -> BTreeSet<String> {
        match path {
            Path::Id => nodes.clone(),
            Path::Pred(p) => {
                let p = p.to_string();
                nodes
                    .iter()
                    .flat_map(|n| self.outgoing(n, &p))
                    .cloned()
                    .collect()
            }
            Path::Inverse(inner) => match inner.as_ref() {
                Path::Pred(p) => {
                    let p = p.to_string();
                    nodes
                        .iter()
                        .flat_map(|n| self.incoming(n, &p))
                        .cloned()
                        .collect()
                }
                // The inverse of a complex path: test every subject.
                inner => self
                    .out
                    .keys()
                    .filter(|s| {
                        let reached = self.step(&BTreeSet::from([(*s).clone()]), inner);
                        !reached.is_disjoint(nodes)
                    })
                    .cloned()
                    .collect(),
            },
            Path::Seq(parts) => parts
                .iter()
                .fold(nodes.clone(), |cur, part| self.step(&cur, part)),
            Path::Alt(parts) => parts.iter().flat_map(|p| self.step(nodes, p)).collect(),
            Path::Star(inner) => {
                let mut seen = nodes.clone();
                let mut frontier = nodes.clone();
                while !frontier.is_empty() {
                    frontier = self
                        .step(&frontier, inner)
                        .into_iter()
                        .filter(|n| !seen.contains(n))
                        .collect();
                    seen.extend(frontier.iter().cloned());
                }
                seen
            }
        }
    }
}

/// One step of a predicate word: the predicate, and whether it is walked
/// backwards.
pub(crate) type Step = (String, bool);

/// The shortest predicate word through `path`: stars taken as zero steps,
/// the first realizable alternative taken. `None` when no edit can realize
/// the path.
pub(crate) fn minimal_word(path: &Path) -> Option<Vec<Step>> {
    match path {
        Path::Id | Path::Star(_) => Some(Vec::new()),
        Path::Pred(p) => Some(vec![(p.to_string(), false)]),
        Path::Inverse(inner) => {
            minimal_word(inner).map(|w| w.into_iter().rev().map(|(p, inv)| (p, !inv)).collect())
        }
        Path::Seq(parts) => {
            let mut out = Vec::new();
            for part in parts {
                out.extend(minimal_word(part)?);
            }
            (!out.is_empty()).then_some(out)
        }
        Path::Alt(parts) => parts
            .iter()
            .filter_map(minimal_word)
            .find(|w| !w.is_empty()),
    }
}

pub(crate) fn is_type_path(path: &Path) -> bool {
    minimal_word(path).is_some_and(|w| w.len() == 1 && w[0].0 == RDF_TYPE && !w[0].1)
}

/// The one edge linking `owner` to `value` along a one-step word.
pub(crate) fn single_edge(owner: &str, word: Option<&[Step]>, value: &str) -> Option<Triple> {
    let [(pred, inverse)] = word? else {
        return None;
    };
    Some(if *inverse {
        (value.to_owned(), pred.clone(), owner.to_owned())
    } else {
        (owner.to_owned(), pred.clone(), value.to_owned())
    })
}

pub(crate) fn short_word(word: &[Step]) -> String {
    word.iter()
        .map(|(p, _)| nt::short(p))
        .collect::<Vec<_>>()
        .join(" / ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::NamedNode;

    fn p(name: &str) -> Path {
        Path::Pred(NamedNode::new_unchecked(format!("http://ex/{name}")))
    }

    fn ex(name: &str) -> String {
        format!("<http://ex/{name}>")
    }

    #[test]
    fn paths_and_words() {
        let triples = BTreeSet::from([
            (ex("a"), ex("p"), ex("b")),
            (ex("b"), ex("q"), ex("c")),
            (ex("c"), ex("q"), ex("d")),
        ]);
        let index = Index::new(triples);
        let seq = Path::Seq(vec![p("p"), Path::Star(Box::new(p("q")))]);
        assert_eq!(
            index.eval_path(&ex("a"), &seq),
            BTreeSet::from([ex("b"), ex("c"), ex("d")])
        );
        let inv = Path::Inverse(Box::new(Path::Seq(vec![p("p"), p("q")])));
        assert_eq!(index.eval_path(&ex("c"), &inv), BTreeSet::from([ex("a")]));
        assert_eq!(
            minimal_word(&inv),
            Some(vec![(ex("q"), true), (ex("p"), true)])
        );
        assert_eq!(minimal_word(&seq), Some(vec![(ex("p"), false)]));
        assert_eq!(
            minimal_word(&Path::Seq(vec![Path::Star(Box::new(p("q")))])),
            None
        );
    }
}
