//! Immutable dictionary-encoded RDF dataset built at the inference→validation
//! boundary. Implements `spareval::QueryableDataset` so Oxigraph's prepared
//! SPARQL evaluator can execute queries against it without a mutable Store.
//!
//! Stage 2 of `docs/05-sparql-execution.md`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::rc::Rc;

use crate::path_plan::ReachStep;
use oxrdf::{Graph, NamedNode, Term};
use shifty_opt::ClosureKind;
use spareval::{InternalQuad, QueryableDataset};

/// IRI under which the shapes graph is loaded into the named-graph slot.
pub(crate) const SHAPES_GRAPH_IRI: &str = "urn:x-shacl:shapes-graph";

/// Dictionary index for a term. Chosen as `u32` to keep triple arrays at 12
/// bytes each; the 4B ceiling is unreachable in practice.
pub type TermId = u32;

/// Which graph a scan reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GraphSel {
    /// The default (data) graph.
    Default,
    /// A specific named graph, identified by its graph-IRI's `TermId`.
    Named(TermId),
}

// ── Term dictionary ──────────────────────────────────────────────────────────

struct TermDictInner {
    id_to_term: Vec<Term>,
    term_to_id: HashMap<Term, TermId>,
}

/// Bidirectional Term ↔ TermId map. Interior-mutable so that
/// `QueryableDataset::internalize_term` (which takes `&self`) can lazily
/// assign IDs to query constants that did not appear in the loaded triples.
struct TermDictionary(RefCell<TermDictInner>);

impl TermDictionary {
    fn new() -> Self {
        Self(RefCell::new(TermDictInner {
            id_to_term: Vec::new(),
            term_to_id: HashMap::new(),
        }))
    }

    /// Return existing ID or assign a new one.
    fn intern(&self, term: Term) -> TermId {
        let mut inner = self.0.borrow_mut();
        if let Some(&id) = inner.term_to_id.get(&term) {
            return id;
        }
        let id = inner.id_to_term.len() as TermId;
        inner.term_to_id.insert(term.clone(), id);
        inner.id_to_term.push(term);
        id
    }

    #[cfg(test)]
    fn get(&self, term: &Term) -> Option<TermId> {
        self.0.borrow().term_to_id.get(term).copied()
    }

    fn externalize(&self, id: TermId) -> Option<Term> {
        self.0.borrow().id_to_term.get(id as usize).cloned()
    }
}

// ── Triple index ─────────────────────────────────────────────────────────────

/// Three sorted arrays giving the six access patterns documented in §161:
/// `spo` covers S-first and membership; `pos` covers P-first lookups;
/// `osp` covers O-first lookups.
struct TripleIndex {
    spo: Vec<[TermId; 3]>, // elements are [s, p, o]
    pos: Vec<[TermId; 3]>, // elements are [p, o, s]
    osp: Vec<[TermId; 3]>, // elements are [o, s, p]
}

impl TripleIndex {
    fn build(mut triples: Vec<[TermId; 3]>) -> Self {
        triples.sort_unstable();
        triples.dedup();

        let mut pos: Vec<[TermId; 3]> = triples.iter().map(|&[s, p, o]| [p, o, s]).collect();
        pos.sort_unstable();

        let mut osp: Vec<[TermId; 3]> = triples.iter().map(|&[s, p, o]| [o, s, p]).collect();
        osp.sort_unstable();

        Self {
            spo: triples,
            pos,
            osp,
        }
    }

    fn contains(&self, s: TermId, p: TermId, o: TermId) -> bool {
        self.spo.binary_search(&[s, p, o]).is_ok()
    }

    fn extend(&mut self, triples: Vec<[TermId; 3]>) {
        if triples.is_empty() {
            return;
        }
        self.spo.extend(triples.iter().copied());
        self.spo.sort_unstable();
        self.spo.dedup();

        self.pos.extend(triples.iter().map(|&[s, p, o]| [p, o, s]));
        self.pos.sort_unstable();
        self.pos.dedup();

        self.osp
            .extend(triples.into_iter().map(|[s, p, o]| [o, s, p]));
        self.osp.sort_unstable();
        self.osp.dedup();
    }

    fn range_s(&self, s: TermId) -> &[[TermId; 3]] {
        let lo = self.spo.partition_point(|t| t[0] < s);
        let hi = self.spo.partition_point(|t| t[0] <= s);
        &self.spo[lo..hi]
    }

    fn range_sp(&self, s: TermId, p: TermId) -> &[[TermId; 3]] {
        let lo = self.spo.partition_point(|t| (t[0], t[1]) < (s, p));
        let hi = self.spo.partition_point(|t| (t[0], t[1]) <= (s, p));
        &self.spo[lo..hi]
    }

    fn range_p(&self, p: TermId) -> &[[TermId; 3]] {
        let lo = self.pos.partition_point(|t| t[0] < p);
        let hi = self.pos.partition_point(|t| t[0] <= p);
        &self.pos[lo..hi]
    }

    fn range_po(&self, p: TermId, o: TermId) -> &[[TermId; 3]] {
        let lo = self.pos.partition_point(|t| (t[0], t[1]) < (p, o));
        let hi = self.pos.partition_point(|t| (t[0], t[1]) <= (p, o));
        &self.pos[lo..hi]
    }

    fn range_o(&self, o: TermId) -> &[[TermId; 3]] {
        let lo = self.osp.partition_point(|t| t[0] < o);
        let hi = self.osp.partition_point(|t| t[0] <= o);
        &self.osp[lo..hi]
    }
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// Dataset statistics used in stage 3+ for join-order planning (doc §198).
pub struct DatasetStatistics {
    pub triple_count: u64,
    pub distinct_subjects: u64,
    pub distinct_objects: u64,
    /// Triples per predicate ID.
    pub predicate_cardinality: HashMap<TermId, u64>,
    subjects: HashSet<TermId>,
    objects: HashSet<TermId>,
}

impl DatasetStatistics {
    fn compute(triples: &TripleIndex) -> Self {
        let triple_count = triples.spo.len() as u64;
        let subjects: HashSet<TermId> = triples.spo.iter().map(|t| t[0]).collect();
        let objects: HashSet<TermId> = triples.spo.iter().map(|t| t[2]).collect();
        let mut predicate_cardinality: HashMap<TermId, u64> = HashMap::new();
        for &[_, p, _] in &triples.spo {
            *predicate_cardinality.entry(p).or_insert(0) += 1;
        }

        Self {
            triple_count,
            distinct_subjects: subjects.len() as u64,
            distinct_objects: objects.len() as u64,
            predicate_cardinality,
            subjects,
            objects,
        }
    }

    fn extend(&mut self, triples: &[[TermId; 3]]) {
        self.triple_count += triples.len() as u64;
        for &[subject, predicate, object] in triples {
            self.subjects.insert(subject);
            self.objects.insert(object);
            *self.predicate_cardinality.entry(predicate).or_insert(0) += 1;
        }
        self.distinct_subjects = self.subjects.len() as u64;
        self.distinct_objects = self.objects.len() as u64;
    }
}

// ── FrozenIndexedDataset ─────────────────────────────────────────────────────

/// Immutable, dictionary-encoded snapshot of a post-inference RDF dataset.
/// Intended to be built once at the inference→validation boundary and shared
/// across all per-focus-node SPARQL evaluations.
pub struct FrozenIndexedDataset {
    terms: TermDictionary,
    default_graph: TripleIndex,
    /// Named graphs keyed by the graph-IRI's TermId. Each value is a
    /// simple SPO-sorted vec (named graphs are typically small).
    named_graphs: HashMap<TermId, Vec<[TermId; 3]>>,
    reach_cache: RefCell<ReachCache>,
    pub stats: DatasetStatistics,
}

const MAX_CACHED_REACH_IDS: usize = 1_000_000;

#[derive(PartialEq, Eq, Hash)]
struct ReachCacheKey {
    node: TermId,
    step: ReachStep,
    kind: ClosureKind,
    graph: GraphSel,
}

#[derive(Default)]
struct ReachCache {
    entries: HashMap<ReachCacheKey, Rc<HashSet<TermId>>>,
    cached_ids: usize,
}

impl FrozenIndexedDataset {
    /// Build from a single graph loaded into the default graph slot.
    pub fn from_graph(graph: &Graph) -> Self {
        let terms = TermDictionary::new();
        let triples = intern_graph(graph, &terms);
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);
        Self {
            terms,
            default_graph,
            named_graphs: HashMap::new(),
            reach_cache: RefCell::new(ReachCache::default()),
            stats,
        }
    }

    /// Build a default graph from the set union of two source graphs without
    /// materializing an intermediate `Graph`.
    pub fn from_graph_union(left: &Graph, right: &Graph) -> Self {
        let terms = TermDictionary::new();
        let mut triples = intern_graph(left, &terms);
        triples.extend(intern_graph(right, &terms));
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);
        Self {
            terms,
            default_graph,
            named_graphs: HashMap::new(),
            reach_cache: RefCell::new(ReachCache::default()),
            stats,
        }
    }

    /// Intern a term against this dataset's dictionary, returning its `TermId`.
    /// Unknown terms (e.g. query constants absent from the data) receive a fresh
    /// id that matches no stored triple — exactly the semantics a scan needs.
    pub(crate) fn intern(&self, term: &Term) -> TermId {
        self.terms.intern(term.clone())
    }

    /// Build a [`PlanStats`] for use by the query planner. Converts TermId-keyed
    /// statistics to Term-keyed form so the planner can look up predicate
    /// cardinalities without touching the dictionary directly.
    pub(crate) fn plan_stats(&self) -> shifty_opt::PlanStats {
        let predicate_cardinality: HashMap<Term, u64> = self
            .stats
            .predicate_cardinality
            .iter()
            .filter_map(|(&id, &count)| Some((self.externalize(id)?, count)))
            .collect();
        let distinct_predicates = predicate_cardinality.len() as u64;
        shifty_opt::PlanStats {
            total_triples: self.stats.triple_count,
            distinct_subjects: self.stats.distinct_subjects,
            distinct_objects: self.stats.distinct_objects,
            distinct_predicates,
            predicate_cardinality,
        }
    }

    /// Map a `TermId` back to its RDF term. Returns `None` only for ids that did
    /// not originate from this dataset.
    pub(crate) fn externalize(&self, id: TermId) -> Option<Term> {
        self.terms.externalize(id)
    }

    /// Like [`externalize`](Self::externalize) but for ids that are guaranteed to
    /// originate from this dataset (e.g. a [`scan`](Self::scan) result), so the
    /// lookup cannot fail.
    pub(crate) fn externalize_id(&self, id: TermId) -> Term {
        self.terms
            .externalize(id)
            .expect("scanned TermId originates from this dataset")
    }

    pub(crate) fn contains_ids(&self, subject: TermId, predicate: TermId, object: TermId) -> bool {
        self.default_graph.contains(subject, predicate, object)
    }

    pub(crate) fn encode_triple(&self, triple: &oxrdf::Triple) -> [TermId; 3] {
        [
            self.terms.intern(triple.subject.clone().into()),
            self.terms.intern(Term::NamedNode(triple.predicate.clone())),
            self.terms.intern(triple.object.clone()),
        ]
    }

    pub(crate) fn cached_reach(
        &self,
        node: TermId,
        step: &ReachStep,
        kind: ClosureKind,
        graph: GraphSel,
    ) -> Option<Rc<HashSet<TermId>>> {
        self.reach_cache
            .borrow()
            .entries
            .get(&ReachCacheKey {
                node,
                step: step.clone(),
                kind,
                graph,
            })
            .cloned()
    }

    pub(crate) fn cache_reach(
        &self,
        node: TermId,
        step: &ReachStep,
        kind: ClosureKind,
        graph: GraphSel,
        result: Rc<HashSet<TermId>>,
    ) {
        let mut cache = self.reach_cache.borrow_mut();
        if cache.cached_ids.saturating_add(result.len()) > MAX_CACHED_REACH_IDS {
            return;
        }
        let key = ReachCacheKey {
            node,
            step: step.clone(),
            kind,
            graph,
        };
        if cache.entries.contains_key(&key) {
            return;
        }
        cache.cached_ids += result.len();
        cache.entries.insert(key, result);
    }

    /// Scan triples in the selected graph matching an optional S/P/O pattern,
    /// yielding `(subject, predicate, object)` term-id triples. Reuses the same
    /// sorted-index access paths as the `QueryableDataset` impl.
    pub(crate) fn scan(
        &self,
        s: Option<TermId>,
        p: Option<TermId>,
        o: Option<TermId>,
        graph: GraphSel,
    ) -> Box<dyn Iterator<Item = [TermId; 3]> + '_> {
        let iter = match graph {
            GraphSel::Default => default_graph_quads(self, s, p, o),
            GraphSel::Named(g) => named_graph_quads(self, g, s, p, o),
        };
        Box::new(iter.map(|q| {
            let q = q.expect("infallible");
            [q.subject, q.predicate, q.object]
        }))
    }

    pub(crate) fn triples_for_predicate(
        &self,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = (Term, Term)> + '_> {
        let p = self.intern(&Term::NamedNode(predicate.clone()));
        Box::new(
            self.scan(None, Some(p), None, GraphSel::Default)
                .map(|[subject, _, object]| {
                    (self.externalize_id(subject), self.externalize_id(object))
                }),
        )
    }

    pub(crate) fn outgoing(
        &self,
        subject: &Term,
    ) -> Box<dyn Iterator<Item = (NamedNode, Term)> + '_> {
        let s = self.intern(subject);
        Box::new(
            self.scan(Some(s), None, None, GraphSel::Default)
                .filter_map(|[_, predicate, object]| {
                    let Term::NamedNode(predicate) = self.externalize_id(predicate) else {
                        return None;
                    };
                    Some((predicate, self.externalize_id(object)))
                }),
        )
    }

    /// Add a committed inference batch to the default-graph indexes.
    pub(crate) fn extend_triples<'a>(
        &mut self,
        triples: impl IntoIterator<Item = &'a oxrdf::Triple>,
    ) {
        let mut encoded: Vec<_> = triples
            .into_iter()
            .map(|triple| self.encode_triple(triple))
            .collect();
        encoded.sort_unstable();
        encoded.dedup();
        encoded.retain(|&[s, p, o]| !self.default_graph.contains(s, p, o));
        self.stats.extend(&encoded);
        self.default_graph.extend(encoded);
        *self.reach_cache.borrow_mut() = ReachCache::default();
    }

    /// Build with `context` in the default graph and `shapes` in the named
    /// graph `urn:x-shacl:shapes-graph`, mirroring what `SparqlExecutor::build`
    /// does with the Oxigraph Store.
    pub fn from_graphs(context: &Graph, shapes: &Graph) -> Self {
        let terms = TermDictionary::new();
        let triples = intern_graph(context, &terms);
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);

        let shapes_iri = NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid");
        let graph_id = terms.intern(Term::NamedNode(shapes_iri));
        let mut named_triples = intern_graph(shapes, &terms);
        named_triples.sort_unstable();
        named_triples.dedup();

        let mut named_graphs = HashMap::new();
        named_graphs.insert(graph_id, named_triples);

        Self {
            terms,
            default_graph,
            named_graphs,
            reach_cache: RefCell::new(ReachCache::default()),
            stats,
        }
    }

    /// Build a union default graph while also exposing `shapes` through the
    /// named `$shapesGraph` slot.
    pub fn from_graph_union_with_shapes(data: &Graph, shapes: &Graph) -> Self {
        let terms = TermDictionary::new();
        let mut triples = intern_graph(data, &terms);
        triples.extend(intern_graph(shapes, &terms));
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);

        let shapes_iri = NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid");
        let graph_id = terms.intern(Term::NamedNode(shapes_iri));
        let mut named_triples = intern_graph(shapes, &terms);
        named_triples.sort_unstable();
        named_triples.dedup();
        let mut named_graphs = HashMap::new();
        named_graphs.insert(graph_id, named_triples);

        Self {
            terms,
            default_graph,
            named_graphs,
            reach_cache: RefCell::new(ReachCache::default()),
            stats,
        }
    }
}

fn intern_graph(graph: &Graph, terms: &TermDictionary) -> Vec<[TermId; 3]> {
    graph
        .iter()
        .map(|triple| {
            let s = terms.intern(triple.subject.into_owned().into());
            let p = terms.intern(Term::NamedNode(triple.predicate.into_owned()));
            let o = terms.intern(triple.object.into_owned());
            [s, p, o]
        })
        .collect()
}

// ── QueryableDataset impl ────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl<'a> QueryableDataset<'a> for &'a FrozenIndexedDataset {
    type InternalTerm = TermId;
    type Error = Infallible;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&TermId>,
        predicate: Option<&TermId>,
        object: Option<&TermId>,
        graph_name: Option<Option<&TermId>>,
    ) -> QuadIter<'a> {
        // Dereference once to get &'a FrozenIndexedDataset, preserving the full
        // 'a lifetime instead of the shorter borrow lifetime of &self. (Not an
        // auto-deref: the explicit `*` is what keeps the `'a` lifetime.)
        #[allow(clippy::explicit_auto_deref)]
        let ds: &'a FrozenIndexedDataset = *self;
        let s = subject.copied();
        let p = predicate.copied();
        let o = object.copied();
        match graph_name {
            Some(None) => default_graph_quads(ds, s, p, o),
            Some(Some(&g)) => named_graph_quads(ds, g, s, p, o),
            None => all_named_quads(ds, s, p, o),
        }
    }

    fn internalize_term(&self, term: Term) -> Result<TermId, Infallible> {
        // Lazily assigns IDs to query constants that weren't in the loaded triples.
        // These will never match any triple index entry but get unique IDs so
        // term-equality in SPARQL expressions is still correct.
        Ok(self.terms.intern(term))
    }

    fn externalize_term(&self, id: TermId) -> Result<Term, Infallible> {
        Ok(self
            .terms
            .externalize(id)
            .expect("TermId always originates from this dataset's internalize_term or internal_quads_for_pattern"))
    }
}

// ─── quad-iterator helpers ───────────────────────────────────────────────────

type QuadIter<'a> = Box<dyn Iterator<Item = Result<InternalQuad<TermId>, Infallible>> + 'a>;

fn mk_quad(s: TermId, p: TermId, o: TermId, g: Option<TermId>) -> InternalQuad<TermId> {
    InternalQuad {
        subject: s,
        predicate: p,
        object: o,
        graph_name: g,
    }
}

/// Quads from the default graph matching an optional S/P/O pattern.
fn default_graph_quads<'a>(
    ds: &'a FrozenIndexedDataset,
    s: Option<TermId>,
    p: Option<TermId>,
    o: Option<TermId>,
) -> QuadIter<'a> {
    match (s, p, o) {
        // Membership check
        (Some(s), Some(p), Some(o)) => {
            let hit = ds.default_graph.contains(s, p, o);
            Box::new(hit.then(|| Ok(mk_quad(s, p, o, None))).into_iter())
        }
        // Subject + predicate lookup (SPO range for S,P prefix)
        (Some(s), Some(p), None) => {
            let slice = ds.default_graph.range_sp(s, p);
            Box::new(slice.iter().map(|&t| Ok(mk_quad(t[0], t[1], t[2], None))))
        }
        // Subject + object: scan S range, filter by O
        (Some(s), None, Some(o)) => {
            let slice = ds.default_graph.range_s(s);
            Box::new(
                slice
                    .iter()
                    .filter(move |t| t[2] == o)
                    .map(|&t| Ok(mk_quad(t[0], t[1], t[2], None))),
            )
        }
        // Subject only
        (Some(s), None, None) => {
            let slice = ds.default_graph.range_s(s);
            Box::new(slice.iter().map(|&t| Ok(mk_quad(t[0], t[1], t[2], None))))
        }
        // Predicate + object (POS range; elements are [p,o,s])
        (None, Some(p), Some(o)) => {
            let slice = ds.default_graph.range_po(p, o);
            Box::new(slice.iter().map(|&t| Ok(mk_quad(t[2], t[0], t[1], None))))
        }
        // Predicate only (POS range; elements are [p,o,s])
        (None, Some(p), None) => {
            let slice = ds.default_graph.range_p(p);
            Box::new(slice.iter().map(|&t| Ok(mk_quad(t[2], t[0], t[1], None))))
        }
        // Object only (OSP range; elements are [o,s,p])
        (None, None, Some(o)) => {
            let slice = ds.default_graph.range_o(o);
            Box::new(slice.iter().map(|&t| Ok(mk_quad(t[1], t[2], t[0], None))))
        }
        // Full scan
        (None, None, None) => Box::new(
            ds.default_graph
                .spo
                .iter()
                .map(|&t| Ok(mk_quad(t[0], t[1], t[2], None))),
        ),
    }
}

/// Quads from a specific named graph matching an optional S/P/O pattern.
/// Named graphs are small (usually just the shapes graph), so a linear scan is
/// sufficient for correctness; stage 5 can add indexes if profiling shows need.
fn named_graph_quads<'a>(
    ds: &'a FrozenIndexedDataset,
    g: TermId,
    s: Option<TermId>,
    p: Option<TermId>,
    o: Option<TermId>,
) -> QuadIter<'a> {
    let Some(triples) = ds.named_graphs.get(&g) else {
        return Box::new(std::iter::empty());
    };
    Box::new(
        triples
            .iter()
            .filter(move |&&[ts, tp, to]| {
                s.is_none_or(|v| ts == v) && p.is_none_or(|v| tp == v) && o.is_none_or(|v| to == v)
            })
            .map(move |&[ts, tp, to]| Ok(mk_quad(ts, tp, to, Some(g)))),
    )
}

/// Quads from ALL named graphs (but NOT the default graph). Used when the
/// `graph_name` pattern is `None` (SPARQL: no GRAPH clause, any named graph).
fn all_named_quads<'a>(
    ds: &'a FrozenIndexedDataset,
    s: Option<TermId>,
    p: Option<TermId>,
    o: Option<TermId>,
) -> QuadIter<'a> {
    Box::new(
        ds.named_graphs
            .keys()
            .copied()
            .flat_map(move |g| named_graph_quads(ds, g, s, p, o)),
    )
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{NamedNode, Triple};
    use spareval::QueryableDataset;

    fn nn(iri: &str) -> NamedNode {
        NamedNode::new(iri).unwrap()
    }

    fn triple_nnn(s: &str, p: &str, o: &str) -> Triple {
        Triple::new(nn(s), nn(p), nn(o))
    }

    fn small_graph() -> Graph {
        let mut g = Graph::new();
        g.insert(triple_nnn("http://ex/a", "http://ex/p", "http://ex/b").as_ref());
        g.insert(triple_nnn("http://ex/a", "http://ex/p", "http://ex/c").as_ref());
        g.insert(triple_nnn("http://ex/b", "http://ex/q", "http://ex/c").as_ref());
        g
    }

    #[test]
    fn intern_round_trips() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let a = Term::NamedNode(nn("http://ex/a"));
        let id = ds.terms.intern(a.clone());
        assert_eq!(ds.terms.externalize(id), Some(a));
    }

    #[test]
    fn contains_triple() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let s = ds.terms.get(&Term::NamedNode(nn("http://ex/a"))).unwrap();
        let p = ds.terms.get(&Term::NamedNode(nn("http://ex/p"))).unwrap();
        let o = ds.terms.get(&Term::NamedNode(nn("http://ex/b"))).unwrap();
        assert!(ds.default_graph.contains(s, p, o));
    }

    #[test]
    fn missing_triple_not_found() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let s = ds.terms.get(&Term::NamedNode(nn("http://ex/a"))).unwrap();
        let p = ds.terms.get(&Term::NamedNode(nn("http://ex/q"))).unwrap();
        let o = ds.terms.get(&Term::NamedNode(nn("http://ex/c"))).unwrap();
        assert!(!ds.default_graph.contains(s, p, o));
    }

    #[test]
    fn range_s_returns_correct_triples() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let s = ds.terms.get(&Term::NamedNode(nn("http://ex/a"))).unwrap();
        let slice = ds.default_graph.range_s(s);
        assert_eq!(slice.len(), 2); // a has two triples
    }

    #[test]
    fn extend_triples_updates_indexes_and_statistics() {
        let g = small_graph();
        let mut ds = FrozenIndexedDataset::from_graph(&g);
        let added = triple_nnn("http://ex/new", "http://ex/p", "http://ex/b");
        ds.extend_triples([&added, &added]);

        let s = ds.intern(&Term::NamedNode(nn("http://ex/new")));
        let p = ds.intern(&Term::NamedNode(nn("http://ex/p")));
        let o = ds.intern(&Term::NamedNode(nn("http://ex/b")));
        assert!(ds.contains_ids(s, p, o));
        assert_eq!(ds.stats.triple_count, 4);
        assert_eq!(ds.stats.predicate_cardinality[&p], 3);
    }

    #[test]
    fn internalize_unknown_term_gets_unique_id() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let rds: &FrozenIndexedDataset = &ds;
        let unknown1 = Term::NamedNode(nn("http://ex/unknown1"));
        let unknown2 = Term::NamedNode(nn("http://ex/unknown2"));
        let id1 = rds.internalize_term(unknown1.clone()).unwrap();
        let id2 = rds.internalize_term(unknown2.clone()).unwrap();
        assert_ne!(id1, id2, "different unknown terms must get different IDs");
        // Same term gets same ID (idempotent)
        let id1b = rds.internalize_term(unknown1).unwrap();
        assert_eq!(id1, id1b);
    }

    #[test]
    fn queryable_dataset_default_graph_pattern() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let rds: &FrozenIndexedDataset = &ds;
        // Query: ?s ?p ?o in default graph → should return all 3 triples
        let quads: Vec<_> = rds
            .internal_quads_for_pattern(None, None, None, Some(None))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(quads.len(), 3);
    }

    #[test]
    fn queryable_dataset_named_graph_empty_when_not_loaded() {
        let g = small_graph();
        let ds = FrozenIndexedDataset::from_graph(&g);
        let rds: &FrozenIndexedDataset = &ds;
        // any named graph → nothing (no named graphs loaded)
        let quads: Vec<_> = rds
            .internal_quads_for_pattern(None, None, None, None)
            .collect();
        assert!(quads.is_empty());
    }

    #[test]
    fn from_graphs_loads_both_default_and_named() {
        let data = small_graph();
        let mut shapes = Graph::new();
        shapes.insert(
            triple_nnn(
                "http://ex/S",
                "http://www.w3.org/ns/shacl#targetNode",
                "http://ex/a",
            )
            .as_ref(),
        );
        let ds = FrozenIndexedDataset::from_graphs(&data, &shapes);
        let rds: &FrozenIndexedDataset = &ds;

        // default graph has 3 triples
        let default_quads: Vec<_> = rds
            .internal_quads_for_pattern(None, None, None, Some(None))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(default_quads.len(), 3);

        // named graph has 1 triple
        let shapes_iri_id = ds
            .terms
            .get(&Term::NamedNode(nn(SHAPES_GRAPH_IRI)))
            .expect("shapes graph IRI should be interned");
        let named_quads: Vec<_> = rds
            .internal_quads_for_pattern(None, None, None, Some(Some(&shapes_iri_id)))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(named_quads.len(), 1);
    }

    #[test]
    fn graph_union_builds_one_deduplicated_default_graph() {
        let left = small_graph();
        let mut right = Graph::new();
        right.insert(triple_nnn("http://ex/a", "http://ex/p", "http://ex/b").as_ref());
        right.insert(triple_nnn("http://ex/new", "http://ex/p", "http://ex/b").as_ref());

        let ds = FrozenIndexedDataset::from_graph_union(&left, &right);

        assert_eq!(ds.stats.triple_count, 4);
    }
}
