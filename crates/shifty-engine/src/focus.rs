//! Focus-node domains independent of the evaluation read graph.

use crate::frozen::{FrozenIndexedDataset, GraphSel, TermId};
use crate::path::{node_of, term_of};
use oxrdf::{Graph, NamedNode, Term};
use std::collections::HashSet;

pub(crate) trait FocusNodes {
    fn subjects_of(&self, predicate: &NamedNode) -> Vec<Term>;
    fn objects_of(&self, predicate: &NamedNode) -> Vec<Term>;
    fn all_nodes(&self) -> HashSet<Term>;
    fn contains_term(&self, term: &Term) -> bool;
}

impl FocusNodes for Graph {
    fn subjects_of(&self, predicate: &NamedNode) -> Vec<Term> {
        let mut seen = HashSet::new();
        self.triples_for_predicate(predicate.as_ref())
            .filter_map(|triple| {
                let term = term_of(triple.subject.into_owned());
                seen.insert(term.clone()).then_some(term)
            })
            .collect()
    }

    fn objects_of(&self, predicate: &NamedNode) -> Vec<Term> {
        let mut seen = HashSet::new();
        self.triples_for_predicate(predicate.as_ref())
            .filter_map(|triple| {
                let term = triple.object.into_owned();
                seen.insert(term.clone()).then_some(term)
            })
            .collect()
    }

    fn all_nodes(&self) -> HashSet<Term> {
        let mut nodes = HashSet::new();
        for triple in self.iter() {
            nodes.insert(term_of(triple.subject.into_owned()));
            nodes.insert(triple.object.into_owned());
        }
        nodes
    }

    fn contains_term(&self, term: &Term) -> bool {
        node_of(term).is_some_and(|node| self.triples_for_subject(&node).next().is_some())
            || self.triples_for_object(term).next().is_some()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FocusScope {
    Data,
    Default,
}

pub(crate) struct IndexedFocus<'a> {
    dataset: &'a FrozenIndexedDataset,
    scope: FocusScope,
}

impl<'a> IndexedFocus<'a> {
    pub(crate) fn new(dataset: &'a FrozenIndexedDataset, scope: FocusScope) -> Self {
        Self { dataset, scope }
    }

    fn scan(
        &self,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Box<dyn Iterator<Item = [TermId; 3]> + '_> {
        match self.scope {
            FocusScope::Data => self.dataset.scan_data(subject, predicate, object),
            FocusScope::Default => self
                .dataset
                .scan(subject, predicate, object, GraphSel::Default),
        }
    }
}

impl FocusNodes for IndexedFocus<'_> {
    fn subjects_of(&self, predicate: &NamedNode) -> Vec<Term> {
        let p = self.dataset.intern(&Term::NamedNode(predicate.clone()));
        let mut seen = HashSet::new();
        self.scan(None, Some(p), None)
            .filter_map(|[subject, _, _]| {
                let term = self.dataset.externalize_id(subject);
                seen.insert(term.clone()).then_some(term)
            })
            .collect()
    }

    fn objects_of(&self, predicate: &NamedNode) -> Vec<Term> {
        let p = self.dataset.intern(&Term::NamedNode(predicate.clone()));
        let mut seen = HashSet::new();
        self.scan(None, Some(p), None)
            .filter_map(|[_, _, object]| {
                let term = self.dataset.externalize_id(object);
                seen.insert(term.clone()).then_some(term)
            })
            .collect()
    }

    fn all_nodes(&self) -> HashSet<Term> {
        let mut nodes = HashSet::new();
        for [subject, _, object] in self.scan(None, None, None) {
            nodes.insert(self.dataset.externalize_id(subject));
            nodes.insert(self.dataset.externalize_id(object));
        }
        nodes
    }

    fn contains_term(&self, term: &Term) -> bool {
        let id = self.dataset.intern(term);
        self.scan(Some(id), None, None).next().is_some()
            || self.scan(None, None, Some(id)).next().is_some()
    }
}
