//! The universe of injections over a reference graph, and drawing cases
//! from it: weighted reservoir sampling, then conflict-free composition with
//! revalidation.

use crate::expand::{Expander, Leaves, fail_shape};
use crate::hash::{StableHasher, hash_str, unit_hash};
use crate::index::{Index, is_type_path};
use crate::model::{Case, CaseFailure, Injection, OperationKind};
use crate::nt::{self, Triple};
use crate::propose;
use crate::{Error, ExpansionLimits};
use oxrdf::dataset::CanonicalizationAlgorithm;
use oxrdf::{NamedOrBlankNodeRef, TermRef};
use shifty_algebra::{Schema, Shape, ShapeArena, ShapeId};
use shifty_engine::{
    CompiledShapes, ConformanceOptions, EngineOptions, EvaluationSession, Evidence,
    EvidenceOptions, EvidenceRun, Failure, SessionData, SessionOptions, StatementEvaluation,
    ValidationGraphMode,
};
use shifty_parse::Loaded;
use shifty_repair::GraphDelta;
use std::cell::{OnceCell, RefCell};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

/// How [`Universe::new`] reads its inputs and bounds the expansion.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Run SHACL-AF rules once and use the inferred closure as the reference
    /// graph. Models that rely on rules, such as ASHRAE 223P models, need
    /// this to conform, and edits may then touch inferred triples.
    pub materialize: bool,
    /// Re-run SHACL-AF rules on every validation instead.
    pub infer: bool,
    /// Also target focus nodes with no outgoing triple in the reference
    /// graph, such as vocabulary terms that the shapes graph describes.
    pub include_external_foci: bool,
    /// Relabel the reference graph's blank nodes canonically. Parsers give
    /// anonymous blank nodes fresh labels on every parse, and injections are
    /// ordered and named by term spelling, so without this a graph with
    /// blank nodes yields a different universe in every process.
    pub canonicalize_blank_nodes: bool,
    pub graph_mode: ValidationGraphMode,
    pub engine: EngineOptions,
    pub limits: ExpansionLimits,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            materialize: false,
            infer: false,
            include_external_foci: false,
            canonicalize_blank_nodes: true,
            graph_mode: ValidationGraphMode::Union,
            engine: EngineOptions::default(),
            limits: ExpansionLimits::default(),
        }
    }
}

/// Optional sampling weights. With none, every injection is equally likely.
///
/// `w(v)` is the geometric mean of `predicates` over the predicates an
/// injection edits, times the geometric mean of `kinds` over its
/// operations. A predicate or kind without an entry weighs 1, so only
/// relative weights matter.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Weights {
    /// Weight by predicate, as an N-Triples IRI `<...>`.
    pub predicates: BTreeMap<String, f64>,
    pub kinds: BTreeMap<OperationKind, f64>,
}

impl Weights {
    /// Every weight must be positive and finite.
    fn validate(&self) -> Result<(), Error> {
        let named = self
            .predicates
            .iter()
            .map(|(p, w)| (p.clone(), *w))
            .chain(self.kinds.iter().map(|(k, w)| (k.as_str().to_owned(), *w)));
        for (name, w) in named {
            if !(w > 0.0 && w.is_finite()) {
                return Err(Error::Weight { name, weight: w });
            }
        }
        Ok(())
    }

    /// `w(v)`.
    fn of(&self, inj: &Injection) -> f64 {
        geometric_mean(&self.predicates, inj.predicates().into_iter())
            * geometric_mean(&self.kinds, inj.operations.iter().map(|op| op.kind))
    }
}

/// The geometric mean of `weights[k]` over `keys`, 1 for a key without an
/// entry or for no keys.
fn geometric_mean<K: Ord>(weights: &BTreeMap<K, f64>, keys: impl Iterator<Item = K>) -> f64 {
    if weights.is_empty() {
        return 1.0;
    }
    let (sum, n) = keys.fold((0.0, 0usize), |(sum, n), k| {
        (sum + weights.get(&k).copied().unwrap_or(1.0).ln(), n + 1)
    });
    if n == 0 { 1.0 } else { (sum / n as f64).exp() }
}

/// A hard filter on injections, shared by the views derived from it.
type Keep = Rc<dyn Fn(&Injection) -> bool>;

/// Every way to break the reference graph, as [`Injection`]s, plus the
/// filter and weights a draw samples them by.
///
/// Construction compiles the shapes and validates the reference graph, but
/// does not expand. [`draw`](Self::draw) and [`draws`](Self::draws) stream
/// the expansion through the filter into weighted reservoirs, holding only
/// the reservoirs in memory. [`len`](Self::len), [`get`](Self::get), and
/// [`iter`](Self::iter) expand on demand and cache the result, which later
/// draws reuse. Either way a draw returns the same case.
///
/// A universe is immutable. [`filter`](Self::filter) and
/// [`weighted`](Self::weighted) return views that share the session, the
/// cached expansion, and the verification cache, so deriving one is cheap.
#[derive(Clone)]
pub struct Universe {
    core: Rc<Core>,
    keep: Option<Keep>,
    weights: Weights,
    /// Indices into the cached expansion, once computed.
    members: OnceCell<Rc<[usize]>>,
}

impl std::fmt::Debug for Universe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Universe")
            .field("filtered", &self.keep.is_some())
            .field("weights", &self.weights)
            .finish_non_exhaustive()
    }
}

impl Universe {
    /// Compile `shapes` and prepare `data` as the reference graph. The
    /// expansion runs later, when a draw or a listing needs it.
    pub fn new(shapes: Loaded, data: oxrdf::Graph, options: Options) -> Result<Self, Error> {
        let compiled =
            CompiledShapes::compile(shapes).map_err(|e| Error::Compile(e.to_string()))?;
        let mut reference = if options.materialize {
            let closure = compiled
                .session(
                    SessionData::Separate(data),
                    SessionOptions {
                        graph_mode: ValidationGraphMode::Data,
                        inference: true,
                        engine: options.engine,
                    },
                )
                .map_err(|e| Error::Session(e.to_string()))?;
            closure.data().clone()
        } else {
            data
        };
        if options.canonicalize_blank_nodes && has_blank_nodes(&reference) {
            reference.canonicalize(CanonicalizationAlgorithm::Unstable);
        }
        Ok(Self {
            core: Rc::new(Core::new(&compiled, reference, &options)?),
            keep: None,
            weights: Weights::default(),
            members: OnceCell::new(),
        })
    }

    /// Indices of the members in the cached expansion, expanding if needed.
    fn members(&self) -> &[usize] {
        self.members.get_or_init(|| {
            self.core
                .all()
                .iter()
                .enumerate()
                .filter(|(_, e)| self.keeps(e))
                .map(|(i, _)| i)
                .collect()
        })
    }

    fn keeps(&self, inj: &Injection) -> bool {
        self.keep.as_ref().is_none_or(|keep| keep(inj))
    }

    /// The number of injections. Expands the universe and caches it.
    pub fn len(&self) -> usize {
        self.members().len()
    }

    pub fn is_empty(&self) -> bool {
        self.members().is_empty()
    }

    pub fn get(&self, i: usize) -> Option<&Injection> {
        let j = *self.members().get(i)?;
        Some(&self.core.all()[j])
    }

    /// The injections, in expansion order: by shape name, then focus.
    /// Expands the universe and caches it.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Injection> + '_ {
        let all = self.core.all();
        self.members().iter().map(move |&j| &all[j])
    }

    /// Visit every member in expansion order: from the cache when there is
    /// one, otherwise streamed from the expansion without storing it.
    fn each(&self, mut visit: impl FnMut(&Injection)) {
        if self.core.all.get().is_some() {
            self.iter().for_each(visit);
        } else {
            self.core.expand(|inj| {
                if self.keeps(&inj) {
                    visit(&inj);
                }
            });
        }
    }

    /// The injections `keep` accepts: a hard filter, such as one shape or
    /// the injections editing one predicate. Filters compose, and the
    /// weights carry over.
    pub fn filter(&self, keep: impl Fn(&Injection) -> bool + 'static) -> Self {
        let keep: Keep = match &self.keep {
            None => Rc::new(keep),
            Some(prev) => {
                let prev = Rc::clone(prev);
                Rc::new(move |inj| prev(inj) && keep(inj))
            }
        };
        let members = OnceCell::new();
        if let Some(mine) = self.members.get() {
            let all = self.core.all();
            let narrowed: Rc<[usize]> = mine.iter().copied().filter(|&j| keep(&all[j])).collect();
            let _ = members.set(narrowed);
        }
        Self {
            core: Rc::clone(&self.core),
            keep: Some(keep),
            weights: self.weights.clone(),
            members,
        }
    }

    /// The same injections, drawn by `weights` instead. Weights favour
    /// injections but never exclude them; use [`filter`](Self::filter) for
    /// that.
    pub fn weighted(&self, weights: Weights) -> Result<Self, Error> {
        weights.validate()?;
        Ok(Self {
            weights,
            ..self.clone()
        })
    }

    pub fn weights(&self) -> &Weights {
        &self.weights
    }

    /// The reference graph `G_ref`, as N-Triples triples.
    pub fn reference(&self) -> &BTreeSet<Triple> {
        &self.core.index.triples
    }

    /// `(focus, statement)` pairs that already fail in the reference graph.
    /// They are never broken, and never count as injected failures.
    pub fn baseline_failures(&self) -> &BTreeSet<(String, usize)> {
        &self.core.baseline_failures
    }

    /// Compose a case of up to `k` injections. The same `(k, seed)` draws
    /// the same case from the same universe in every process.
    ///
    /// The members stream into a weighted reservoir of the `max(8k, 64)`
    /// with the smallest keys `-ln h(v) / w(v)`, which is then scanned in key
    /// order. As in arXiv 2611.07342 (Sec. 6.3), an injection is admitted
    /// when it can be applied with the ones already chosen: it does not
    /// delete what another adds, replace a value another replaces
    /// differently, or need the opposite polarity of a requirement. Several
    /// injections may break one property of one focus node, and an
    /// injection may fail pairs it did not aim at. Beyond the paper, each
    /// injection must make its target fail when applied alone, and the
    /// composed delta is revalidated: an injection whose targeted property
    /// no longer fails is dropped and replaced from the reservoir. The case
    /// can hold fewer than `k` injections when the reservoir runs out.
    pub fn draw(&self, k: usize, seed: u64) -> Result<Case, Error> {
        let mut cases = self.draws(&[(k, seed)])?;
        Ok(cases.remove(0))
    }

    /// One case per `(k, seed)`, as [`draw`](Self::draw) would compose it,
    /// from a single pass over the expansion: each request fills its own
    /// reservoir.
    pub fn draws(&self, requests: &[(usize, u64)]) -> Result<Vec<Case>, Error> {
        if requests.iter().any(|&(k, _)| k < 1) {
            return Err(Error::Budget);
        }
        let mut reservoirs: Vec<Reservoir> = requests
            .iter()
            .map(|&(k, seed)| Reservoir::new((8 * k).max(64), format!("{seed}:{k}")))
            .collect();
        let mut seen = false;
        self.each(|inj| {
            seen = true;
            for r in &mut reservoirs {
                r.offer(inj, &self.weights);
            }
        });
        if !seen {
            return Err(Error::Empty);
        }
        requests
            .iter()
            .zip(reservoirs)
            .map(|(&(k, seed), r)| self.compose(&r.into_sorted(), k, seed))
            .collect()
    }

    fn compose(&self, pool: &[Injection], k: usize, seed: u64) -> Result<Case, Error> {
        let core = &self.core;
        let mut chosen: Vec<&Injection> = Vec::new();
        let mut rejected: HashSet<&str> = HashSet::new();
        let mut cursor = 0;
        let final_failures = loop {
            while chosen.len() < k && cursor < pool.len() {
                let cand = &pool[cursor];
                cursor += 1;
                if rejected.contains(cand.key()) || chosen.iter().any(|c| cand.conflicts_with(c)) {
                    continue;
                }
                if !core.verify(cand) {
                    rejected.insert(cand.key());
                    continue;
                }
                chosen.push(cand);
            }
            if chosen.is_empty() {
                break Failures::new();
            }
            let (adds, deletes) = union_delta(&chosen);
            let failures = core.revalidate(&adds, &deletes, |focus, statement| {
                !core
                    .baseline_failures
                    .contains(&(focus.to_owned(), statement))
            })?;
            let masked: Vec<&str> = chosen
                .iter()
                .filter(|c| !realized(c, &failures))
                .map(|c| c.key())
                .collect();
            if masked.is_empty() {
                break failures;
            }
            // The combination masked these, for example when another edit
            // restored support they removed. Drop them, refill from the
            // pool, and revalidate. Each round rejects at least one
            // injection for good, so the loop ends.
            rejected.extend(masked);
            chosen.retain(|c| realized(c, &failures));
        };
        let (adds, deletes) = union_delta(&chosen);
        let failures = final_failures
            .iter()
            .map(|((focus, statement), components)| CaseFailure {
                focus: focus.clone(),
                statement: *statement,
                shape: core.shape_name(*statement).map(str::to_owned),
                injections: chosen
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        c.target() == (focus.as_str(), *statement)
                            && components.contains(&c.component)
                    })
                    .map(|(i, _)| i)
                    .collect(),
            })
            .collect();
        let delta = GraphDelta {
            add: parse_triples(&adds)?,
            delete: parse_triples(&deletes)?,
        };
        Ok(Case::new(
            k,
            seed,
            chosen.into_iter().cloned().collect(),
            failures,
            deletes,
            adds,
            delta,
            Rc::clone(&core.index.triples),
        ))
    }
}

impl<'a> IntoIterator for &'a Universe {
    type Item = &'a Injection;
    type IntoIter = Box<dyn ExactSizeIterator<Item = &'a Injection> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// A passing `(focus, statement)` pair of the reference graph.
pub(crate) struct Target {
    pub focus: String,
    pub statement: usize,
    pub shape: Option<String>,
    run_statement: usize,
    run_focus: usize,
}

/// Engine evidence by `(term, normalized constraint)`. `None` records a
/// term the engine could not evaluate.
type EvidenceCache = HashMap<(String, ShapeId), Option<Rc<Evidence>>>;

/// Failing pairs, mapped to their failing top-level conjuncts.
type Failures = BTreeMap<(String, usize), BTreeSet<ShapeId>>;

/// What every view of a universe shares: the session over the reference
/// graph, the expansion, and the engine caches.
pub(crate) struct Core {
    session: EvaluationSession,
    index: Index,
    limits: ExpansionLimits,
    names: Vec<Option<String>>,
    baseline_failures: BTreeSet<(String, usize)>,
    /// The baseline evidence the expansion rewrites. The expander memoizes
    /// by evidence address, which stays valid while this lives.
    baseline: EvidenceRun,
    targets: Vec<Target>,
    /// The whole expansion, once a listing has asked for it.
    all: OnceCell<Vec<Injection>>,
    evidence: RefCell<EvidenceCache>,
    shape_keys: RefCell<HashMap<ShapeId, u64>>,
    leaves: RefCell<HashMap<ShapeId, Rc<Leaves>>>,
    verified: RefCell<HashMap<String, Option<Rc<Failures>>>>,
}

impl Core {
    fn new(
        compiled: &CompiledShapes,
        reference: oxrdf::Graph,
        options: &Options,
    ) -> Result<Self, Error> {
        let triples: BTreeSet<Triple> = reference
            .iter()
            .map(|t| nt::triple(&t.into_owned()))
            .collect();
        let session = compiled
            .session(
                SessionData::Separate(reference),
                SessionOptions {
                    graph_mode: options.graph_mode,
                    inference: options.infer,
                    engine: options.engine,
                },
            )
            .map_err(|e| Error::Session(e.to_string()))?;
        let baseline = session
            .evidence(&EvidenceOptions::default())
            .map_err(|e| Error::Evaluation(e.to_string()))?;
        let authored: Arc<Schema> = compiled.authored_schema_shared();
        let names = authored
            .statements
            .iter()
            .map(|st| authored.name_of(st.shape).map(str::to_owned))
            .collect();
        let mut core = Self {
            session,
            index: Index::new(triples),
            limits: options.limits,
            names,
            baseline_failures: failing_components(&baseline.statements)
                .into_keys()
                .collect(),
            baseline,
            targets: Vec::new(),
            all: OnceCell::new(),
            evidence: RefCell::new(HashMap::new()),
            shape_keys: RefCell::new(HashMap::new()),
            leaves: RefCell::new(HashMap::new()),
            verified: RefCell::new(HashMap::new()),
        };
        core.targets = core.collect_targets(options.include_external_foci);
        Ok(core)
    }

    pub(crate) fn limits(&self) -> ExpansionLimits {
        self.limits
    }

    /// The authored name of statement `statement`'s shape.
    fn shape_name(&self, statement: usize) -> Option<&str> {
        self.names.get(statement)?.as_deref()
    }

    /// Every passing `(focus, statement)` pair, sorted by shape name, focus,
    /// and statement.
    fn collect_targets(&self, include_external_foci: bool) -> Vec<Target> {
        let mut out = Vec::new();
        for (si, st) in self.baseline.statements.iter().enumerate() {
            for (fi, fe) in st.selected_foci.iter().enumerate() {
                if !matches!(fe.evidence, Evidence::Satisfaction(_)) {
                    continue;
                }
                let focus = nt::term(&fe.focus);
                if !include_external_foci && !self.index.has_outgoing(&focus) {
                    continue;
                }
                out.push(Target {
                    focus,
                    statement: st.source_statement_id,
                    shape: self.shape_name(st.source_statement_id).map(str::to_owned),
                    run_statement: si,
                    run_focus: fi,
                });
            }
        }
        out.sort_by(|a, b| {
            (a.shape.as_deref().unwrap_or(""), &a.focus, a.statement).cmp(&(
                b.shape.as_deref().unwrap_or(""),
                &b.focus,
                b.statement,
            ))
        });
        out
    }

    /// The whole expansion, computed on first use.
    fn all(&self) -> &[Injection] {
        self.all.get_or_init(|| {
            let mut out = Vec::new();
            self.expand(|inj| out.push(inj));
            out
        })
    }

    /// Stream the distinct injections of every target, in target order.
    /// Only key digests are kept to drop duplicates: a delta reached again
    /// from a later target is the same injection, and the first one wins.
    fn expand(&self, mut emit: impl FnMut(Injection)) {
        let lim = self.limits;
        let mut seen: HashSet<u64> = HashSet::new();
        for target in &self.targets {
            let Evidence::Satisfaction(root) = &self.baseline.statements[target.run_statement]
                .selected_foci[target.run_focus]
                .evidence
            else {
                unreachable!("targets hold")
            };
            let mut expander = Expander::new(self, target);
            for (component, alt) in expander.components(root, lim.max_per_target) {
                let inj = Injection::new(
                    target.focus.clone(),
                    target.statement,
                    target.shape.clone(),
                    component,
                    alt,
                );
                if inj.key().is_empty() || !seen.insert(hash_str(inj.key())) {
                    continue;
                }
                emit(inj);
                if seen.len() >= lim.max_injections {
                    return;
                }
            }
        }
    }

    /// Whether applying `inj` alone makes its target fail.
    fn verify(&self, inj: &Injection) -> bool {
        if self
            .baseline_failures
            .contains(&(inj.focus.clone(), inj.statement))
        {
            return false;
        }
        let cached = self.verified.borrow().get(inj.key()).cloned();
        let failures = match cached {
            Some(f) => f,
            None => {
                let target = |focus: &str, statement: usize| {
                    focus == inj.focus && statement == inj.statement
                };
                let f = self
                    .revalidate(&inj.adds(), &inj.deletes(), target)
                    .ok()
                    .map(Rc::new);
                self.verified
                    .borrow_mut()
                    .insert(inj.key().to_owned(), f.clone());
                f
            }
        };
        failures.is_some_and(|f| realized(inj, &f))
    }

    /// The failing pairs of the reference graph with `adds` and `deletes`
    /// applied, restricted to those `wanted` keeps, with their failing
    /// components.
    fn revalidate(
        &self,
        adds: &BTreeSet<Triple>,
        deletes: &BTreeSet<Triple>,
        wanted: impl Fn(&str, usize) -> bool,
    ) -> Result<Failures, Error> {
        let delta = GraphDelta {
            add: parse_triples(adds)?,
            delete: parse_triples(deletes)?,
        };
        let next = self
            .session
            .with_delta(&delta)
            .map_err(|e| Error::Session(e.to_string()))?;
        // Only wanted failing pairs need evidence: find the failing pairs by
        // conformance, which builds none, then explain the wanted ones.
        let (_, pairs) = next
            .find_failures(&ConformanceOptions::default())
            .map_err(|e| Error::Evaluation(e.to_string()))?;
        let mut statements = Vec::new();
        for pair in &pairs {
            let focus = nt::term(pair.focus());
            if !pair.source_statements().iter().any(|&s| wanted(&focus, s)) {
                continue;
            }
            statements.extend(
                next.explain_canonical(pair)
                    .map_err(|e| Error::Evaluation(e.to_string()))?,
            );
        }
        let mut failures = failing_components(&statements);
        failures.retain(|(focus, statement), _| wanted(focus, *statement));
        Ok(failures)
    }

    // ── engine probes used by the expander ──

    pub(crate) fn index(&self) -> &Index {
        &self.index
    }

    fn arena(&self) -> &ShapeArena {
        &self.session.prepared_evidence().schema().arena
    }

    /// The evidence for `term` against normalized constraint `q`, or `None`
    /// when the term does not parse or the engine rejects it.
    pub(crate) fn evidence(&self, term: &str, q: ShapeId) -> Option<Rc<Evidence>> {
        let key = (term.to_owned(), q);
        if let Some(hit) = self.evidence.borrow().get(&key) {
            return hit.clone();
        }
        let ev = nt::parse_term(term).and_then(|t| {
            self.session
                .prepared_evidence()
                .explain_constraint(&t, q)
                .ok()
                .flatten()
                .map(Rc::new)
        });
        self.evidence.borrow_mut().insert(key, ev.clone());
        ev
    }

    pub(crate) fn holds(&self, term: &str, q: ShapeId) -> Option<bool> {
        self.evidence(term, q)
            .map(|ev| matches!(ev.as_ref(), Evidence::Satisfaction(_)))
    }

    /// `sid`, past any annotation wrappers.
    pub(crate) fn unwrap_shape(&self, mut sid: ShapeId) -> &Shape {
        let arena = self.arena();
        loop {
            match arena.get(sid) {
                Shape::Annotated { shape, .. } => sid = *shape,
                other => return other,
            }
        }
    }

    /// A digest of normalized shape `sid` by structure, not by id: ids can
    /// differ between processes.
    pub(crate) fn shape_key(&self, sid: ShapeId) -> u64 {
        if let Some(k) = self.shape_keys.borrow().get(&sid) {
            return *k;
        }
        self.shape_key_in(sid, &mut Vec::new()).0
    }

    fn shape_key_in(&self, sid: ShapeId, stack: &mut Vec<ShapeId>) -> (u64, bool) {
        if let Some(k) = self.shape_keys.borrow().get(&sid) {
            return (*k, false);
        }
        if stack.contains(&sid) {
            return (crate::hash::hash_str("recursive"), true);
        }
        stack.push(sid);
        let mut cyclic = false;
        let mut sub = |child: ShapeId, stack: &mut Vec<ShapeId>| {
            let (k, cut) = self.shape_key_in(child, stack);
            cyclic |= cut;
            k
        };
        let mut h = StableHasher::new();
        match self.arena().get(sid) {
            Shape::Annotated {
                severity,
                messages,
                shape,
            } => {
                let k = sub(*shape, stack);
                h.str("annotated")
                    .str(&format!("{severity:?}{messages:?}"))
                    .u64(k);
            }
            Shape::Not(inner) => {
                let k = sub(*inner, stack);
                h.str("not").u64(k);
            }
            Shape::And(parts) | Shape::Or(parts) => {
                let tag = if matches!(self.arena().get(sid), Shape::And(_)) {
                    "and"
                } else {
                    "or"
                };
                let mut keys: Vec<u64> = parts.iter().map(|p| sub(*p, stack)).collect();
                keys.sort_unstable();
                h.str(tag);
                for k in keys {
                    h.u64(k);
                }
            }
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                let k = sub(*qualifier, stack);
                h.str("count")
                    .str(&format!("{path:?}{min:?}{max:?}"))
                    .u64(k);
            }
            other => {
                h.str(&format!("{other:?}"));
            }
        }
        stack.pop();
        let key = h.finish();
        // A key computed below a cut cycle depends on where the walk
        // entered, so only acyclic keys are shared.
        if !cyclic {
            self.shape_keys.borrow_mut().insert(sid, key);
        }
        (key, cyclic)
    }

    /// Constants, facets, and classes mentioned locally under `q`. Only
    /// class membership speaks about the value itself; other counts
    /// constrain the value's own neighbours, so the walk does not descend
    /// into them.
    pub(crate) fn leaves(&self, q: ShapeId) -> Rc<Leaves> {
        if let Some(hit) = self.leaves.borrow().get(&q) {
            return Rc::clone(hit);
        }
        let mut consts = BTreeSet::new();
        let mut facets = Vec::new();
        let mut classes = BTreeSet::new();
        let mut stack = vec![q];
        let mut seen = HashSet::new();
        while let Some(sid) = stack.pop() {
            if !seen.insert(sid) {
                continue;
            }
            match self.unwrap_shape(sid) {
                Shape::TestConst(t) => {
                    consts.insert(nt::term(t));
                }
                Shape::TestType(vt) => facets.extend(propose::flatten_facets(vt)),
                Shape::Not(inner) => stack.push(*inner),
                Shape::And(parts) | Shape::Or(parts) => stack.extend(parts.iter().copied()),
                Shape::Count {
                    path, qualifier, ..
                } if is_type_path(path) => {
                    if let Shape::TestConst(class) = self.unwrap_shape(*qualifier) {
                        classes.insert(nt::term(class));
                    }
                }
                _ => {}
            }
        }
        facets.sort_by_cached_key(|f| format!("{f:?}"));
        let leaves = Rc::new(Leaves {
            consts: consts.into_iter().collect(),
            facets,
            classes: classes.into_iter().collect(),
        });
        self.leaves.borrow_mut().insert(q, Rc::clone(&leaves));
        leaves
    }
}

/// A weighted reservoir: the `size` injections with the smallest
/// Efraimidis–Spirakis keys `-ln h(v) / w(v)`, where `h` hashes the salt and
/// the injection key into `(0, 1]`. Which injections it keeps does not
/// depend on the order they arrive in.
struct Reservoir {
    size: usize,
    salt: String,
    heap: BinaryHeap<Keyed>,
}

impl Reservoir {
    fn new(size: usize, salt: String) -> Self {
        Self {
            size,
            salt,
            heap: BinaryHeap::with_capacity(size + 1),
        }
    }

    fn offer(&mut self, inj: &Injection, weights: &Weights) {
        let key = -unit_hash(&self.salt, inj.key()).ln() / weights.of(inj);
        let beats = |worst: &Keyed| {
            key.total_cmp(&worst.key)
                .then_with(|| inj.key().cmp(worst.inj.key()))
                .is_lt()
        };
        if self.heap.len() < self.size {
            self.heap.push(Keyed {
                key,
                inj: inj.clone(),
            });
        } else if self.heap.peek().is_some_and(beats) {
            self.heap.pop();
            self.heap.push(Keyed {
                key,
                inj: inj.clone(),
            });
        }
    }

    /// The kept injections, in ascending key order.
    fn into_sorted(self) -> Vec<Injection> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|k| k.inj)
            .collect()
    }
}

/// A reservoir entry, ordered by key and then by injection key, so ties
/// break the same way in every process.
struct Keyed {
    key: f64,
    inj: Injection,
}

impl PartialEq for Keyed {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Keyed {}

impl PartialOrd for Keyed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Keyed {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .total_cmp(&other.key)
            .then_with(|| self.inj.key().cmp(other.inj.key()))
    }
}

fn union_delta(chosen: &[&Injection]) -> (BTreeSet<Triple>, BTreeSet<Triple>) {
    let adds = chosen.iter().flat_map(|c| c.adds()).collect();
    let deletes = chosen.iter().flat_map(|c| c.deletes()).collect();
    (adds, deletes)
}

fn parse_triples(triples: &BTreeSet<Triple>) -> Result<Vec<oxrdf::Triple>, Error> {
    triples
        .iter()
        .map(|t| nt::parse_triple(t).ok_or_else(|| Error::Term(nt::line(t))))
        .collect()
}

fn realized(inj: &Injection, failures: &Failures) -> bool {
    failures
        .get(&(inj.focus.clone(), inj.statement))
        .is_some_and(|c| c.contains(&inj.component))
}

/// Failing pairs mapped to the ids of their failing top-level conjuncts.
fn failing_components(statements: &[StatementEvaluation]) -> Failures {
    let mut out = Failures::new();
    for st in statements {
        for fe in &st.selected_foci {
            let Evidence::Failure(failure) = &fe.evidence else {
                continue;
            };
            let mut ids = BTreeSet::from([fail_shape(failure)]);
            if let Failure::All { failed, .. } = failure {
                ids.extend(failed.iter().map(fail_shape));
            }
            out.insert((nt::term(&fe.focus), st.source_statement_id), ids);
        }
    }
    out
}

fn has_blank_nodes(graph: &oxrdf::Graph) -> bool {
    graph.iter().any(|t| {
        matches!(t.subject, NamedOrBlankNodeRef::BlankNode(_))
            || matches!(t.object, TermRef::BlankNode(_))
    })
}
