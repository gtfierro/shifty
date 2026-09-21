//! Dictionary-encoded RDF dataset shared by inference's native and fallback
//! evaluators. A committed inference batch updates its indexes before the next
//! rule group reads them. Validation selects its own default-graph view over
//! the final inference snapshot without rebuilding the dataset.
//!
//! This file is the read-mostly dataset boundary. Source terms and predicate
//! partitions are shared across sessions. Session-local terms extend the source
//! dictionary; a complete PSO primary index retains every local triple, while
//! optional secondary indexes accelerate selected access patterns. Evaluation
//! stays in IDs until a result crosses back into RDF terms.
//!
//! Query-only terms and reachability results may be cached behind interior
//! mutability, but neither changes the RDF snapshot. The only real mutation,
//! `extend_triples`, updates affected indexes/statistics and discards derived closures.
//! Thus every scan sees one coherent snapshot and every cached closure belongs
//! to that snapshot.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use crate::path_plan::ReachStep;
use crate::profile::IndexRecord;
use oxrdf::{Graph, NamedNode, Term};
use shifty_opt::{AccessCatalog, ClosureKind};
use spareval::{InternalQuad, QueryableDataset};
use web_time::Instant;

/// IRI under which the shapes graph is loaded into the named-graph slot.
pub(crate) const SHAPES_GRAPH_IRI: &str = "urn:x-shacl:shapes-graph";

/// Dictionary index for a term. Chosen as `u32` to keep triple arrays at 12
/// bytes each. Allocation checks the capacity rather than truncating ids.
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
struct TermDictionary {
    source: Option<Arc<SourceStorage>>,
    local: RefCell<TermDictInner>,
}

/// Immutable source IDs and indexes shared by sessions of one compilation.
/// Query constants and data terms are never inserted here.
pub(crate) struct SourceStorage {
    id_to_term: Vec<Term>,
    term_to_id: HashMap<Term, TermId>,
    index: Arc<TripleIndex>,
    reverse_demand: Vec<NamedNode>,
    subject_demand: bool,
    object_demand: bool,
}

impl SourceStorage {
    #[cfg(test)]
    pub(crate) fn encode(graph: &Graph) -> Arc<Self> {
        Self::encode_with_demand(graph, &AccessCatalog::default())
    }

    pub(crate) fn encode_with_demand(graph: &Graph, catalog: &AccessCatalog) -> Arc<Self> {
        let started = crate::profile::is_enabled().then(Instant::now);
        let terms = TermDictionary::new();
        let triples = intern_graph(graph, &terms);
        let reverse_demand: HashSet<_> = catalog
            .consumers
            .iter()
            .filter(|consumer| consumer.default.probes.reverse || consumer.shapes.probes.reverse)
            .flat_map(|consumer| {
                consumer
                    .default
                    .predicates
                    .iter()
                    .chain(&consumer.shapes.predicates)
            })
            .cloned()
            .collect();
        let source_demand = reverse_demand
            .iter()
            .filter_map(|p| terms.get(&Term::NamedNode(p.clone())))
            .collect();
        let predicate_labels = predicate_labels(&triples, &terms);
        let subject_demand = catalog.consumers.iter().any(|consumer| {
            (consumer.default.any_predicate && consumer.default.probes.forward)
                || (consumer.shapes.any_predicate && consumer.shapes.probes.forward)
        });
        let object_demand = catalog.consumers.iter().any(|consumer| {
            (consumer.default.any_predicate && consumer.default.probes.reverse)
                || (consumer.shapes.any_predicate && consumer.shapes.probes.reverse)
        });
        let index = Arc::new(TripleIndex::build_with_scope(
            triples,
            IndexPolicy::DemandDriven,
            SOURCE_INDEX_BUDGET,
            source_demand,
            subject_demand,
            object_demand,
            IndexScope::Source,
            predicate_labels,
        ));
        let local = terms.local.into_inner();
        crate::profile::record_source_build(index.len());
        if let Some(started) = started {
            crate::profile::record_source_encode_time(started.elapsed().as_micros() as u64);
        }
        Arc::new(Self {
            id_to_term: local.id_to_term,
            term_to_id: local.term_to_id,
            index,
            reverse_demand: reverse_demand.into_iter().collect(),
            subject_demand,
            object_demand,
        })
    }
}

impl TermDictionary {
    fn new() -> Self {
        Self {
            source: None,
            local: RefCell::new(TermDictInner {
                id_to_term: Vec::new(),
                term_to_id: HashMap::new(),
            }),
        }
    }

    fn from_source(source: Arc<SourceStorage>) -> Self {
        Self {
            source: Some(source),
            local: RefCell::new(TermDictInner {
                id_to_term: Vec::new(),
                term_to_id: HashMap::new(),
            }),
        }
    }

    /// Return existing ID or assign a new one.
    fn intern(&self, term: Term) -> TermId {
        if let Some(id) = self.source.as_ref().and_then(|s| s.term_to_id.get(&term)) {
            return *id;
        }
        let mut inner = self.local.borrow_mut();
        if let Some(&id) = inner.term_to_id.get(&term) {
            return id;
        }
        let source_len = self.source.as_ref().map_or(0, |s| s.id_to_term.len());
        let id = TermId::try_from(source_len + inner.id_to_term.len())
            .expect("RDF term dictionary exceeded u32 ID capacity");
        inner.term_to_id.insert(term.clone(), id);
        inner.id_to_term.push(term);
        id
    }

    fn get(&self, term: &Term) -> Option<TermId> {
        self.source
            .as_ref()
            .and_then(|s| s.term_to_id.get(term).copied())
            .or_else(|| self.local.borrow().term_to_id.get(term).copied())
    }

    fn externalize(&self, id: TermId) -> Option<Term> {
        let source_len = self.source.as_ref().map_or(0, |s| s.id_to_term.len());
        if (id as usize) < source_len {
            return self.source.as_ref()?.id_to_term.get(id as usize).cloned();
        }
        self.local
            .borrow()
            .id_to_term
            .get(id as usize - source_len)
            .cloned()
    }
}

// ── Triple index ─────────────────────────────────────────────────────────────

/// Complete predicate-partitioned primary representation. Each partition is a
/// sorted set of `(subject, object)` pairs, so known-predicate scans, forward
/// probes, and exact membership need no secondary index. Every other pattern
/// has a correct scan fallback while selective indexes are introduced.
struct TripleIndex {
    partitions: BTreeMap<TermId, PredicatePartition>,
    len: usize,
    policy: IndexPolicy,
    reverse_demand: HashSet<TermId>,
    secondary_budget: usize,
    secondary_bytes: AtomicUsize,
    subject_dir: OnceLock<Option<Vec<[TermId; 3]>>>,
    object_dir: OnceLock<Option<Vec<[TermId; 3]>>>,
    subject_probes: AtomicU32,
    object_probes: AtomicU32,
    subject_demand: bool,
    object_demand: bool,
    scope: IndexScope,
    predicate_labels: HashMap<TermId, String>,
}

struct PredicatePartition {
    pairs: Vec<[TermId; 2]>,
    reverse: OnceLock<Option<Vec<[TermId; 2]>>>,
    reverse_probes: AtomicU32,
}

impl Default for PredicatePartition {
    fn default() -> Self {
        Self {
            pairs: Vec::new(),
            reverse: OnceLock::new(),
            reverse_probes: AtomicU32::new(0),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexPolicy {
    BaseOnly,
    AllIndexes,
    DemandDriven,
}

#[derive(Clone, Copy)]
enum IndexScope {
    Source,
    Session,
}

impl IndexScope {
    fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Session => "session",
        }
    }
}

const SOURCE_INDEX_BUDGET: usize = 64 * 1024 * 1024;
const SESSION_INDEX_BUDGET: usize = 32 * 1024 * 1024;

fn merge_sorted<T: Copy + Ord>(existing: &mut Vec<T>, mut incoming: Vec<T>) {
    incoming.sort_unstable();
    incoming.dedup();
    let mut merged = Vec::with_capacity(existing.len() + incoming.len());
    let (mut a, mut b) = (0, 0);
    while a < existing.len() && b < incoming.len() {
        match existing[a].cmp(&incoming[b]) {
            std::cmp::Ordering::Less => {
                merged.push(existing[a]);
                a += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(incoming[b]);
                b += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(existing[a]);
                a += 1;
                b += 1;
            }
        }
    }
    merged.extend_from_slice(&existing[a..]);
    merged.extend_from_slice(&incoming[b..]);
    *existing = merged;
}

impl TripleIndex {
    fn build(triples: Vec<[TermId; 3]>) -> Self {
        Self::build_with_policy(
            triples,
            IndexPolicy::DemandDriven,
            SESSION_INDEX_BUDGET,
            HashSet::new(),
            false,
            false,
        )
    }

    fn build_with_policy(
        triples: Vec<[TermId; 3]>,
        policy: IndexPolicy,
        secondary_budget: usize,
        reverse_demand: HashSet<TermId>,
        subject_demand: bool,
        object_demand: bool,
    ) -> Self {
        Self::build_with_scope(
            triples,
            policy,
            secondary_budget,
            reverse_demand,
            subject_demand,
            object_demand,
            IndexScope::Session,
            HashMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_with_scope(
        triples: Vec<[TermId; 3]>,
        policy: IndexPolicy,
        secondary_budget: usize,
        reverse_demand: HashSet<TermId>,
        subject_demand: bool,
        object_demand: bool,
        scope: IndexScope,
        predicate_labels: HashMap<TermId, String>,
    ) -> Self {
        let mut partitions: BTreeMap<TermId, PredicatePartition> = BTreeMap::new();
        for [s, p, o] in triples {
            partitions.entry(p).or_default().pairs.push([s, o]);
        }
        let mut len = 0;
        for partition in partitions.values_mut() {
            partition.pairs.sort_unstable();
            partition.pairs.dedup();
            len += partition.pairs.len();
        }
        let index = Self {
            partitions,
            len,
            policy,
            reverse_demand,
            secondary_budget,
            secondary_bytes: AtomicUsize::new(0),
            subject_dir: OnceLock::new(),
            object_dir: OnceLock::new(),
            subject_probes: AtomicU32::new(0),
            object_probes: AtomicU32::new(0),
            subject_demand,
            object_demand,
            scope,
            predicate_labels,
        };
        if crate::profile::is_enabled() {
            crate::profile::record_primary_index_bytes(
                index.scope.label(),
                0,
                index.primary_bytes(),
            );
        }
        if policy == IndexPolicy::AllIndexes {
            for (&predicate, partition) in &index.partitions {
                index.reverse(predicate, partition);
            }
            index.subject_directory();
            index.object_directory();
        }
        index
    }

    fn contains(&self, s: TermId, p: TermId, o: TermId) -> bool {
        self.partitions
            .get(&p)
            .is_some_and(|partition| partition.pairs.binary_search(&[s, o]).is_ok())
    }

    fn extend(&mut self, triples: Vec<[TermId; 3]>) {
        let old_primary_bytes = crate::profile::is_enabled().then(|| self.primary_bytes());
        let mut batches: BTreeMap<TermId, Vec<[TermId; 2]>> = BTreeMap::new();
        let mut added_rows = Vec::new();
        for [s, p, o] in triples {
            batches.entry(p).or_default().push([s, o]);
        }
        for (predicate, mut incoming) in batches {
            incoming.sort_unstable();
            incoming.dedup();
            let partition = self.partitions.entry(predicate).or_default();
            let existing = &mut partition.pairs;
            if existing.is_empty() {
                self.len += incoming.len();
                added_rows.extend(incoming.iter().map(|&[s, o]| [s, predicate, o]));
                *existing = incoming;
                continue;
            }
            let additions: Vec<_> = incoming
                .iter()
                .copied()
                .filter(|row| existing.binary_search(row).is_err())
                .collect();
            self.len += additions.len();
            added_rows.extend(additions.iter().map(|&[s, o]| [s, predicate, o]));
            merge_sorted(existing, incoming);
            if let Some(Some(reverse)) = partition.reverse.get_mut() {
                let bytes = additions.len() * std::mem::size_of::<[TermId; 2]>();
                if self
                    .secondary_bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(bytes)
                    <= self.secondary_budget
                {
                    self.secondary_bytes.fetch_add(bytes, Ordering::Relaxed);
                    merge_sorted(
                        reverse,
                        additions.into_iter().map(|[s, o]| [o, s]).collect(),
                    );
                } else {
                    let old_bytes = reverse.len() * std::mem::size_of::<[TermId; 2]>();
                    partition.reverse.take();
                    self.secondary_bytes.fetch_sub(old_bytes, Ordering::Relaxed);
                }
            }
        }
        if let Some(Some(directory)) = self.subject_dir.get_mut() {
            let bytes = added_rows.len() * std::mem::size_of::<[TermId; 3]>();
            if self
                .secondary_bytes
                .load(Ordering::Relaxed)
                .saturating_add(bytes)
                <= self.secondary_budget
            {
                self.secondary_bytes.fetch_add(bytes, Ordering::Relaxed);
                merge_sorted(directory, added_rows.clone());
            } else {
                let old_bytes = directory.len() * std::mem::size_of::<[TermId; 3]>();
                self.subject_dir.take();
                self.secondary_bytes.fetch_sub(old_bytes, Ordering::Relaxed);
            }
        }
        if let Some(Some(directory)) = self.object_dir.get_mut() {
            let bytes = added_rows.len() * std::mem::size_of::<[TermId; 3]>();
            if self
                .secondary_bytes
                .load(Ordering::Relaxed)
                .saturating_add(bytes)
                <= self.secondary_budget
            {
                self.secondary_bytes.fetch_add(bytes, Ordering::Relaxed);
                merge_sorted(
                    directory,
                    added_rows.into_iter().map(|[s, p, o]| [o, p, s]).collect(),
                );
            } else {
                let old_bytes = directory.len() * std::mem::size_of::<[TermId; 3]>();
                self.object_dir.take();
                self.secondary_bytes.fetch_sub(old_bytes, Ordering::Relaxed);
            }
        }
        if self.policy == IndexPolicy::AllIndexes {
            for (&predicate, partition) in &self.partitions {
                self.reverse(predicate, partition);
            }
        }
        if let Some(old_primary_bytes) = old_primary_bytes {
            crate::profile::record_primary_index_bytes(
                self.scope.label(),
                old_primary_bytes,
                self.primary_bytes(),
            );
        }
    }

    fn primary_bytes(&self) -> usize {
        self.partitions
            .values()
            .map(|partition| partition.pairs.capacity() * std::mem::size_of::<[TermId; 2]>())
            .sum()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn rows(&self) -> impl Iterator<Item = [TermId; 3]> + '_ {
        self.partitions
            .iter()
            .flat_map(|(&p, partition)| partition.pairs.iter().map(move |&[s, o]| [s, p, o]))
    }

    fn reserve_secondary(&self, bytes: usize) -> bool {
        self.secondary_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.secondary_budget)
            })
            .is_ok()
    }

    fn record_decision(
        &self,
        kind: &'static str,
        predicate: Option<TermId>,
        reason: &'static str,
        rows: usize,
        bytes: usize,
        built: Option<(u64, usize)>,
    ) {
        crate::profile::record_index_decision(IndexRecord {
            scope: self.scope.label(),
            kind,
            predicate: predicate.and_then(|id| self.predicate_labels.get(&id).cloned()),
            reason,
            rows,
            estimated_bytes: bytes,
            actual_bytes: built.map_or(0, |(_, bytes)| bytes),
            budget_bytes: self.secondary_budget,
            build_us: built.map_or(0, |(us, _)| us),
            accepted: built.is_some(),
        });
    }

    fn reverse<'a>(
        &self,
        predicate: TermId,
        partition: &'a PredicatePartition,
    ) -> Option<&'a Vec<[TermId; 2]>> {
        if self.policy == IndexPolicy::BaseOnly {
            return None;
        }
        let probes = partition.reverse_probes.fetch_add(1, Ordering::Relaxed) + 1;
        if self.policy == IndexPolicy::DemandDriven
            && (partition.pairs.len() < 64
                || (probes < 3 && !self.reverse_demand.contains(&predicate)))
        {
            return None;
        }
        partition
            .reverse
            .get_or_init(|| {
                let bytes = partition.pairs.len() * std::mem::size_of::<[TermId; 2]>();
                let reason = if self.policy == IndexPolicy::AllIndexes {
                    "full-index policy"
                } else if self.reverse_demand.contains(&predicate) {
                    "compiled demand"
                } else {
                    "observed probes"
                };
                if !self.reserve_secondary(bytes) {
                    self.record_decision(
                        "reverse",
                        Some(predicate),
                        reason,
                        partition.pairs.len(),
                        bytes,
                        None,
                    );
                    return None;
                }
                let started = web_time::Instant::now();
                let mut rows: Vec<_> = partition.pairs.iter().map(|&[s, o]| [o, s]).collect();
                rows.sort_unstable();
                self.record_decision(
                    "reverse",
                    Some(predicate),
                    reason,
                    rows.len(),
                    bytes,
                    Some((
                        started.elapsed().as_micros() as u64,
                        rows.capacity() * std::mem::size_of::<[TermId; 2]>(),
                    )),
                );
                Some(rows)
            })
            .as_ref()
    }

    fn subject_directory(&self) -> Option<&Vec<[TermId; 3]>> {
        if self.policy == IndexPolicy::BaseOnly {
            return None;
        }
        let probes = self.subject_probes.fetch_add(1, Ordering::Relaxed) + 1;
        if self.policy == IndexPolicy::DemandDriven
            && (self.len < 256 || (probes < 3 && !self.subject_demand))
        {
            return None;
        }
        self.subject_dir
            .get_or_init(|| {
                let bytes = self.len * std::mem::size_of::<[TermId; 3]>();
                let reason = if self.policy == IndexPolicy::AllIndexes {
                    "full-index policy"
                } else if self.subject_demand {
                    "compiled demand"
                } else {
                    "observed probes"
                };
                if !self.reserve_secondary(bytes) {
                    self.record_decision("subject", None, reason, self.len, bytes, None);
                    return None;
                }
                let started = web_time::Instant::now();
                let mut rows = Vec::with_capacity(self.len);
                rows.extend(self.rows());
                rows.sort_unstable();
                self.record_decision(
                    "subject",
                    None,
                    reason,
                    rows.len(),
                    bytes,
                    Some((
                        started.elapsed().as_micros() as u64,
                        rows.capacity() * std::mem::size_of::<[TermId; 3]>(),
                    )),
                );
                Some(rows)
            })
            .as_ref()
    }

    fn object_directory(&self) -> Option<&Vec<[TermId; 3]>> {
        if self.policy == IndexPolicy::BaseOnly {
            return None;
        }
        let probes = self.object_probes.fetch_add(1, Ordering::Relaxed) + 1;
        if self.policy == IndexPolicy::DemandDriven
            && (self.len < 256 || (probes < 3 && !self.object_demand))
        {
            return None;
        }
        self.object_dir
            .get_or_init(|| {
                let bytes = self.len * std::mem::size_of::<[TermId; 3]>();
                let reason = if self.policy == IndexPolicy::AllIndexes {
                    "full-index policy"
                } else if self.object_demand {
                    "compiled demand"
                } else {
                    "observed probes"
                };
                if !self.reserve_secondary(bytes) {
                    self.record_decision("object", None, reason, self.len, bytes, None);
                    return None;
                }
                let started = web_time::Instant::now();
                let mut rows = Vec::with_capacity(self.len);
                rows.extend(self.rows().map(|[s, p, o]| [o, p, s]));
                rows.sort_unstable();
                self.record_decision(
                    "object",
                    None,
                    reason,
                    rows.len(),
                    bytes,
                    Some((
                        started.elapsed().as_micros() as u64,
                        rows.capacity() * std::mem::size_of::<[TermId; 3]>(),
                    )),
                );
                Some(rows)
            })
            .as_ref()
    }

    fn scan(
        &self,
        s: Option<TermId>,
        p: Option<TermId>,
        o: Option<TermId>,
    ) -> Box<dyn Iterator<Item = [TermId; 3]> + '_> {
        let mask = usize::from(s.is_some())
            | (usize::from(p.is_some()) << 1)
            | (usize::from(o.is_some()) << 2);
        let record = |candidates| crate::profile::record_scan(self.scope.label(), mask, candidates);
        if let Some(p) = p {
            let Some(partition) = self.partitions.get(&p) else {
                record(0);
                return Box::new(std::iter::empty());
            };
            if s.is_none()
                && let Some(o) = o
                && let Some(reverse) = self.reverse(p, partition)
            {
                let lo = reverse.partition_point(|row| row[0] < o);
                let hi = reverse.partition_point(|row| row[0] <= o);
                record(hi - lo);
                return Box::new(reverse[lo..hi].iter().map(move |&[o, s]| [s, p, o]));
            }
            let pairs = &partition.pairs;
            let pairs = if let Some(s) = s {
                let lo = pairs.partition_point(|row| row[0] < s);
                let hi = pairs.partition_point(|row| row[0] <= s);
                &pairs[lo..hi]
            } else {
                pairs.as_slice()
            };
            record(pairs.len());
            Box::new(
                pairs
                    .iter()
                    .filter(move |row| o.is_none_or(|o| row[1] == o))
                    .map(move |&[s, o]| [s, p, o]),
            )
        } else {
            if let Some(s) = s
                && let Some(directory) = self.subject_directory()
            {
                let lo = directory.partition_point(|row| row[0] < s);
                let hi = directory.partition_point(|row| row[0] <= s);
                record(hi - lo);
                return Box::new(
                    directory[lo..hi]
                        .iter()
                        .filter(move |row| o.is_none_or(|o| row[2] == o))
                        .copied(),
                );
            }
            if let Some(o) = o
                && let Some(directory) = self.object_directory()
            {
                let lo = directory.partition_point(|row| row[0] < o);
                let hi = directory.partition_point(|row| row[0] <= o);
                record(hi - lo);
                return Box::new(
                    directory[lo..hi]
                        .iter()
                        .filter(move |row| s.is_none_or(|s| row[2] == s))
                        .map(|&[o, p, s]| [s, p, o]),
                );
            }
            record(self.len);
            Box::new(self.rows().filter(move |&[subject, _, object]| {
                s.is_none_or(|s| subject == s) && o.is_none_or(|o| object == o)
            }))
        }
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
        Self::compute_rows(triples.rows())
    }

    fn compute_rows(rows: impl Iterator<Item = [TermId; 3]>) -> Self {
        let mut triple_count = 0;
        let mut subjects = HashSet::new();
        let mut objects = HashSet::new();
        let mut predicate_cardinality: HashMap<TermId, u64> = HashMap::new();
        for [s, p, o] in rows {
            triple_count += 1;
            subjects.insert(s);
            objects.insert(o);
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

/// Source rows visible in this dataset's default graph. Embedded snapshots
/// usually contain almost every source row, so keep exclusions in that case;
/// sparse separate-data overlap uses inclusions instead.
enum SourceMembership {
    All,
    Included(HashSet<[TermId; 3]>),
    Excluded(HashSet<[TermId; 3]>),
}

struct SourceView {
    index: Arc<TripleIndex>,
    membership: SourceMembership,
    include_all: bool,
}

impl SourceView {
    fn data_member(&self, row: [TermId; 3]) -> bool {
        match &self.membership {
            SourceMembership::All => true,
            SourceMembership::Included(rows) => rows.contains(&row),
            SourceMembership::Excluded(rows) => !rows.contains(&row),
        }
    }

    fn includes(&self, row: [TermId; 3]) -> bool {
        self.include_all || self.data_member(row)
    }
}

// ── FrozenIndexedDataset ─────────────────────────────────────────────────────

/// Immutable, dictionary-encoded snapshot of a post-inference RDF dataset.
/// Intended to be built once at the inference→validation boundary and shared
/// across all per-focus-node SPARQL evaluations.
pub struct FrozenIndexedDataset {
    terms: TermDictionary,
    /// Triples outside the immutable source partition.
    default_graph: TripleIndex,
    source_view: Option<SourceView>,
    /// Named graphs keyed by graph-IRI ID. Compiled sessions point at the
    /// immutable source index, rather than copying its rows per session.
    named_graphs: HashMap<TermId, Arc<TripleIndex>>,
    reach_cache: RefCell<ReachCache>,
    /// Monotonic identity for data-dependent caches and physical plans. Every
    /// committed RDF batch and default-view change advances it.
    revision: u64,
    pub stats: DatasetStatistics,
}

/// A closure cache is an acceleration, never part of query semantics. Bound it
/// by the number of retained result ids rather than by entry count: one large
/// `p*` closure is the memory risk, while many empty/small closures are cheap.
/// On saturation we simply stop admitting new entries, retaining predictable
/// memory use and the same indexed evaluation behavior for later requests.
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
    /// Shared so callers can cheaply keep a result while the cache remains
    /// borrowable for another query. Results are immutable sets, making this
    /// aliasing safe and avoiding a clone proportional to the closure size.
    entries: HashMap<ReachCacheKey, Rc<HashSet<TermId>>>,
    cached_ids: usize,
}

impl FrozenIndexedDataset {
    /// Create a session dataset using the compilation's source dictionary and
    /// named-graph index. `include_source` applies only to the default view:
    /// embedded edited data must pass `false` so deleted data membership is not
    /// restored merely because the triple remains in the shapes graph.
    pub(crate) fn from_data_with_source(
        data: &Graph,
        source: Arc<SourceStorage>,
        include_source: bool,
    ) -> Self {
        let started = crate::profile::is_enabled().then(Instant::now);
        if started.is_some() {
            crate::profile::observe_shared_source_primary_bytes(source.index.primary_bytes());
        }
        let terms = TermDictionary::from_source(Arc::clone(&source));
        let mut local = Vec::new();
        let mut overlapping = HashSet::new();
        for row in intern_graph(data, &terms) {
            if source.index.contains(row[0], row[1], row[2]) {
                overlapping.insert(row);
            } else {
                local.push(row);
            }
        }
        let local_demand = source
            .reverse_demand
            .iter()
            .map(|p| terms.intern(Term::NamedNode(p.clone())))
            .collect();
        let predicate_labels = predicate_labels(&local, &terms);
        let default_graph = TripleIndex::build_with_scope(
            local,
            IndexPolicy::DemandDriven,
            SESSION_INDEX_BUDGET,
            local_demand,
            source.subject_demand,
            source.object_demand,
            IndexScope::Session,
            predicate_labels,
        );
        let membership = if overlapping.len() == source.index.len() {
            SourceMembership::All
        } else if overlapping.len() > source.index.len() / 2 {
            let excluded = source
                .index
                .rows()
                .filter(|row| !overlapping.contains(row))
                .collect();
            SourceMembership::Excluded(excluded)
        } else {
            SourceMembership::Included(overlapping)
        };
        let source_view = SourceView {
            index: Arc::clone(&source.index),
            membership,
            include_all: include_source,
        };
        let stats = DatasetStatistics::compute_rows(
            default_graph.rows().chain(
                source_view
                    .index
                    .rows()
                    .filter(|row| source_view.includes(*row)),
            ),
        );
        crate::profile::record_dataset_build(default_graph.len());
        let graph_id = terms.intern(Term::NamedNode(
            NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid"),
        ));
        let named_graphs = HashMap::from([(graph_id, Arc::clone(&source.index))]);
        if let Some(started) = started {
            crate::profile::record_session_encode_time(started.elapsed().as_micros() as u64);
        }
        Self {
            terms,
            default_graph,
            source_view: Some(source_view),
            named_graphs,
            reach_cache: RefCell::new(ReachCache::default()),
            revision: 0,
            stats,
        }
    }

    /// Build from a single graph loaded into the default graph slot.
    pub fn from_graph(graph: &Graph) -> Self {
        let terms = TermDictionary::new();
        let triples = intern_graph(graph, &terms);
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);
        crate::profile::record_dataset_build(default_graph.len());
        Self {
            terms,
            default_graph,
            source_view: None,
            named_graphs: HashMap::new(),
            reach_cache: RefCell::new(ReachCache::default()),
            revision: 0,
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
        crate::profile::record_dataset_build(default_graph.len());
        Self {
            terms,
            default_graph,
            source_view: None,
            named_graphs: HashMap::new(),
            reach_cache: RefCell::new(ReachCache::default()),
            revision: 0,
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
            || self.source_view.as_ref().is_some_and(|source| {
                source.index.contains(subject, predicate, object)
                    && source.includes([subject, predicate, object])
            })
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
        // No eviction policy is needed: this snapshot is short-lived and an
        // admission-only cap avoids turning a hot closure into repeated
        // allocate-and-evict churn. A miss remains correct and index-backed.
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
        if let (GraphSel::Default, Some(s), Some(p), Some(o)) = (graph, s, p, o) {
            let found = self.contains_ids(s, p, o);
            crate::profile::record_scan("session", 7, usize::from(found));
            return Box::new(found.then_some([s, p, o]).into_iter());
        }
        let iter = match graph {
            GraphSel::Default => default_graph_quads(self, s, p, o),
            GraphSel::Named(g) => named_graph_quads(self, g, s, p, o),
        };
        Box::new(iter.map(|q| {
            let q = q.expect("infallible");
            [q.subject, q.predicate, q.object]
        }))
    }

    /// Data membership independent of whether the default read graph also
    /// includes source-only rows. Focus selection uses this in Union mode.
    pub(crate) fn scan_data(
        &self,
        s: Option<TermId>,
        p: Option<TermId>,
        o: Option<TermId>,
    ) -> Box<dyn Iterator<Item = [TermId; 3]> + '_> {
        let local = self.default_graph.scan(s, p, o);
        let Some(source) = &self.source_view else {
            return local;
        };
        Box::new(
            local.chain(
                source
                    .index
                    .scan(s, p, o)
                    .filter(|row| source.data_member(*row)),
            ),
        )
    }

    /// Compatibility projection of evaluated data, excluding source-only rows
    /// even when the default query view is a data/shapes union.
    pub(crate) fn data_graph_projection(&self) -> Graph {
        self.project_rows(self.scan_data(None, None, None))
    }

    /// Compatibility projection for APIs that explicitly request a Graph.
    pub(crate) fn default_graph_projection(&self) -> Graph {
        self.project_rows(self.scan(None, None, None, GraphSel::Default))
    }

    fn project_rows(&self, rows: impl Iterator<Item = [TermId; 3]>) -> Graph {
        let mut graph = Graph::new();
        for [subject, predicate, object] in rows {
            let subject: oxrdf::NamedOrBlankNode = match self.externalize_id(subject) {
                Term::NamedNode(node) => node.into(),
                Term::BlankNode(node) => node.into(),
                Term::Literal(_) => unreachable!("RDF subjects cannot be literals"),
            };
            let Term::NamedNode(predicate) = self.externalize_id(predicate) else {
                unreachable!("RDF predicates are named nodes")
            };
            graph.insert(&oxrdf::Triple::new(
                subject,
                predicate,
                self.externalize_id(object),
            ));
        }
        crate::profile::record_graph_projection(graph.len());
        graph
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
        let started = crate::profile::is_enabled().then(Instant::now);
        let mut encoded: Vec<_> = triples
            .into_iter()
            .map(|triple| self.encode_triple(triple))
            .collect();
        encoded.sort_unstable();
        encoded.dedup();
        encoded.retain(|&[s, p, o]| !self.contains_ids(s, p, o));
        let committed = encoded.len();
        self.stats.extend(&encoded);
        for &[_, predicate, _] in &encoded {
            if !self.default_graph.predicate_labels.contains_key(&predicate)
                && let Term::NamedNode(node) = self.externalize_id(predicate)
            {
                self.default_graph
                    .predicate_labels
                    .insert(predicate, node.as_str().to_owned());
            }
        }
        self.default_graph.extend(encoded);
        *self.reach_cache.borrow_mut() = ReachCache::default();
        if committed != 0 {
            self.revision = self
                .revision
                .checked_add(1)
                .expect("dataset revision overflowed u64");
        }
        if let Some(started) = started {
            crate::profile::record_dataset_commit(committed, started.elapsed().as_micros() as u64);
        }
    }

    /// Select the evaluated-data view after union-based inference. Source
    /// membership is retained separately, so this changes no stored rows or
    /// named-shapes reads and needs no second dictionary/index build.
    pub(crate) fn select_data_view(&mut self) {
        let Some(source) = &mut self.source_view else {
            return;
        };
        if !source.include_all {
            return;
        }
        source.include_all = false;
        self.stats = DatasetStatistics::compute_rows(
            self.default_graph
                .rows()
                .chain(source.index.rows().filter(|row| source.includes(*row))),
        );
        *self.reach_cache.borrow_mut() = ReachCache::default();
        self.revision = self
            .revision
            .checked_add(1)
            .expect("dataset revision overflowed u64");
    }

    /// Current data/view revision for cache and physical-plan identities.
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    /// Build with `context` in the default graph and `shapes` in the named
    /// graph `urn:x-shacl:shapes-graph`, mirroring what `SparqlExecutor::build`
    /// does with the Oxigraph Store.
    pub fn from_graphs(context: &Graph, shapes: &Graph) -> Self {
        let terms = TermDictionary::new();
        let triples = intern_graph(context, &terms);
        let default_graph = TripleIndex::build(triples);
        let stats = DatasetStatistics::compute(&default_graph);
        crate::profile::record_dataset_build(default_graph.len());

        let shapes_iri = NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid");
        let graph_id = terms.intern(Term::NamedNode(shapes_iri));
        let named_triples = Arc::new(TripleIndex::build(intern_graph(shapes, &terms)));

        let mut named_graphs = HashMap::new();
        named_graphs.insert(graph_id, named_triples);

        Self {
            terms,
            default_graph,
            source_view: None,
            named_graphs,
            reach_cache: RefCell::new(ReachCache::default()),
            revision: 0,
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
        crate::profile::record_dataset_build(default_graph.len());

        let shapes_iri = NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid");
        let graph_id = terms.intern(Term::NamedNode(shapes_iri));
        let named_triples = Arc::new(TripleIndex::build(intern_graph(shapes, &terms)));
        let mut named_graphs = HashMap::new();
        named_graphs.insert(graph_id, named_triples);

        Self {
            terms,
            default_graph,
            source_view: None,
            named_graphs,
            reach_cache: RefCell::new(ReachCache::default()),
            revision: 0,
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

fn predicate_labels(rows: &[[TermId; 3]], terms: &TermDictionary) -> HashMap<TermId, String> {
    rows.iter()
        .map(|row| row[1])
        .collect::<HashSet<_>>()
        .into_iter()
        .filter_map(|id| match terms.externalize(id) {
            Some(Term::NamedNode(node)) => Some((id, node.as_str().to_owned())),
            _ => None,
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

    fn internal_named_graphs(&self) -> impl Iterator<Item = Result<TermId, Infallible>> + use<'a> {
        self.named_graphs
            .keys()
            .copied()
            .map(Ok)
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn contains_internal_graph_name(&self, graph_name: &TermId) -> Result<bool, Infallible> {
        Ok(self.named_graphs.contains_key(graph_name))
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
    let local = indexed_graph_quads(&ds.default_graph, None, s, p, o);
    let Some(source) = &ds.source_view else {
        return local;
    };
    // Local rows were partitioned against the source index on construction;
    // commits check default membership before insertion. The two streams are
    // therefore disjoint within this view, so concatenation preserves a set.
    let shared = indexed_graph_quads(&source.index, None, s, p, o).filter(move |quad| {
        let quad = quad.as_ref().expect("infallible RDF scan");
        let row = [quad.subject, quad.predicate, quad.object];
        source.includes(row)
    });
    Box::new(local.chain(shared))
}

fn indexed_graph_quads<'a>(
    index: &'a TripleIndex,
    graph: Option<TermId>,
    s: Option<TermId>,
    p: Option<TermId>,
    o: Option<TermId>,
) -> QuadIter<'a> {
    Box::new(
        index
            .scan(s, p, o)
            .map(move |[s, p, o]| Ok(mk_quad(s, p, o, graph))),
    )
}

/// Quads from a specific named graph matching an optional S/P/O pattern.
/// Named shapes use the same shared source primary index as default-view
/// probes, without copying its rows into each session.
fn named_graph_quads<'a>(
    ds: &'a FrozenIndexedDataset,
    g: TermId,
    s: Option<TermId>,
    p: Option<TermId>,
    o: Option<TermId>,
) -> QuadIter<'a> {
    let Some(index) = ds.named_graphs.get(&g) else {
        return Box::new(std::iter::empty());
    };
    indexed_graph_quads(index, Some(g), s, p, o)
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
    fn profile_accounts_for_source_index_reused_after_encoding() {
        let source = SourceStorage::encode(&small_graph());
        crate::profile::enable();
        let _dataset =
            FrozenIndexedDataset::from_data_with_source(&Graph::new(), Arc::clone(&source), false);
        let profile = crate::profile::take().unwrap();
        assert_eq!(profile.storage().source_builds, 0);
        assert_eq!(
            profile.storage().source_primary_bytes as usize,
            source.index.primary_bytes()
        );
    }

    #[test]
    fn empty_named_graph_remains_visible_to_sparql() {
        let source = SourceStorage::encode(&Graph::new());
        let dataset = FrozenIndexedDataset::from_data_with_source(&Graph::new(), source, false);
        let graph_id = dataset
            .terms
            .get(&Term::NamedNode(nn(SHAPES_GRAPH_IRI)))
            .unwrap();
        let queryable = &dataset;

        assert_eq!(
            queryable
                .internal_named_graphs()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![graph_id]
        );
        assert!(queryable.contains_internal_graph_name(&graph_id).unwrap());
        assert!(!queryable.contains_internal_graph_name(&u32::MAX).unwrap());
        assert!(
            queryable
                .internal_quads_for_pattern(None, None, None, Some(Some(&graph_id)))
                .next()
                .is_none()
        );
    }

    #[test]
    fn reverse_index_policies_preserve_all_pattern_answers() {
        let rows: Vec<_> = (0..128).map(|s| [s, 9, s % 7]).collect();
        let base = TripleIndex::build_with_policy(
            rows.clone(),
            IndexPolicy::BaseOnly,
            0,
            HashSet::new(),
            false,
            false,
        );
        let all = TripleIndex::build_with_policy(
            rows.clone(),
            IndexPolicy::AllIndexes,
            usize::MAX,
            HashSet::new(),
            false,
            false,
        );
        let demand = TripleIndex::build_with_policy(
            rows.clone(),
            IndexPolicy::DemandDriven,
            0,
            HashSet::from([9]),
            false,
            false,
        );
        for (s, p, o) in [
            (None, None, None),
            (Some(3), None, None),
            (None, Some(9), None),
            (None, None, Some(2)),
            (Some(3), Some(9), None),
            (None, Some(9), Some(2)),
            (Some(3), None, Some(3)),
            (Some(3), Some(9), Some(3)),
        ] {
            let mut expected: Vec<_> = base.scan(s, p, o).collect();
            expected.sort_unstable();
            for index in [&all, &demand] {
                let mut got: Vec<_> = index.scan(s, p, o).collect();
                got.sort_unstable();
                assert_eq!(got, expected, "pattern {s:?} {p:?} {o:?}");
            }
        }
        assert_eq!(
            all.secondary_bytes.load(Ordering::Relaxed),
            128 * (8 + 12 + 12)
        );
        assert_eq!(demand.secondary_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn reverse_index_promotes_on_demand_and_merges_committed_rows() {
        let rows: Vec<_> = (0..128).map(|s| [s, 9, s % 7]).collect();
        let mut index = TripleIndex::build_with_policy(
            rows,
            IndexPolicy::DemandDriven,
            2048,
            HashSet::from([9]),
            false,
            false,
        );
        assert_eq!(index.scan(None, Some(9), Some(2)).count(), 18);
        assert!(index.partitions[&9].reverse.get().is_some());
        index.extend(vec![[200, 9, 2], [200, 9, 2], [201, 10, 2]]);
        assert!(index.contains(200, 9, 2));
        assert_eq!(index.scan(None, Some(9), Some(2)).count(), 19);
        assert_eq!(index.scan(None, Some(10), Some(2)).count(), 1);
        assert_eq!(index.len(), 130);
    }

    #[test]
    fn general_directories_obey_budget_and_track_commits() {
        crate::profile::enable();
        let rows: Vec<_> = (0..300).map(|i| [i % 3, i + 10, i % 5]).collect();
        let mut index = TripleIndex::build_with_policy(
            rows.clone(),
            IndexPolicy::DemandDriven,
            300 * 12,
            HashSet::new(),
            true,
            true,
        );
        assert_eq!(index.scan(Some(1), None, None).count(), 100);
        assert!(index.subject_dir.get().is_some());
        // The first directory consumes the whole byte budget. The object
        // probe must use the complete scan fallback without changing answers.
        assert_eq!(index.scan(None, None, Some(2)).count(), 60);
        assert!(matches!(index.object_dir.get(), Some(None)));
        index.extend(vec![[1, 900, 2]]);
        assert!(index.contains(1, 900, 2));
        assert_eq!(index.scan(Some(1), None, None).count(), 101);
        assert_eq!(index.scan(None, None, Some(2)).count(), 61);
        let profile = crate::profile::take().unwrap();
        assert_eq!(profile.indexes().len(), 3);
        assert_eq!(profile.indexes()[0].kind, "subject");
        assert!(profile.indexes()[0].accepted);
        assert_eq!(profile.indexes()[0].actual_bytes, 300 * 12);
        assert_eq!(profile.indexes()[1].kind, "object");
        assert!(!profile.indexes()[1].accepted);
        assert_eq!(profile.indexes()[1].budget_bytes, 300 * 12);
        assert_eq!(profile.indexes()[2].kind, "subject");
        assert!(!profile.indexes()[2].accepted);
        assert_eq!(profile.indexes()[2].estimated_bytes, 301 * 12);
        assert_eq!(
            profile.storage().session_primary_bytes as usize,
            index.primary_bytes()
        );
    }

    #[test]
    fn source_index_profile_names_observed_predicate() {
        let mut graph = Graph::new();
        for index in 0..128 {
            graph.insert(&triple_nnn(
                &format!("http://ex/s{index}"),
                "http://ex/p",
                "http://ex/o",
            ));
        }
        crate::profile::enable();
        let source = SourceStorage::encode(&graph);
        let p = source.term_to_id[&Term::NamedNode(nn("http://ex/p"))];
        let o = source.term_to_id[&Term::NamedNode(nn("http://ex/o"))];
        for _ in 0..3 {
            assert_eq!(source.index.scan(None, Some(p), Some(o)).count(), 128);
        }
        let profile = crate::profile::take().unwrap();
        assert_eq!(profile.indexes().len(), 1);
        let index = &profile.indexes()[0];
        assert_eq!(index.scope, "source");
        assert_eq!(index.kind, "reverse");
        assert_eq!(index.predicate.as_deref(), Some("http://ex/p"));
        assert_eq!(index.reason, "observed probes");
        assert!(index.actual_bytes >= index.estimated_bytes);
        assert!(index.accepted);
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
        assert_eq!(ds.default_graph.scan(Some(s), None, None).count(), 2);
    }

    #[test]
    fn extend_triples_updates_indexes_and_statistics() {
        let g = small_graph();
        let mut ds = FrozenIndexedDataset::from_graph(&g);
        let added = triple_nnn("http://ex/new", "http://ex/p", "http://ex/b");
        assert_eq!(ds.revision(), 0);
        ds.extend_triples([&added, &added]);
        assert_eq!(ds.revision(), 1);
        ds.extend_triples([&added]);
        assert_eq!(ds.revision(), 1, "a no-op batch is not a commit");

        let s = ds.intern(&Term::NamedNode(nn("http://ex/new")));
        let p = ds.intern(&Term::NamedNode(nn("http://ex/p")));
        let o = ds.intern(&Term::NamedNode(nn("http://ex/b")));
        assert!(ds.contains_ids(s, p, o));
        assert_eq!(ds.stats.triple_count, 4);
        assert_eq!(ds.stats.predicate_cardinality[&p], 3);
    }

    #[test]
    fn inferred_predicate_has_an_index_profile_label() {
        let mut ds = FrozenIndexedDataset::from_graph(&Graph::new());
        let predicate = nn("http://ex/inferred");
        let object = nn("http://ex/value");
        let triples: Vec<_> = (0..128)
            .map(|i| {
                Triple::new(
                    nn(&format!("http://ex/s{i}")),
                    predicate.clone(),
                    object.clone(),
                )
            })
            .collect();
        ds.extend_triples(triples.iter());
        let p = ds.intern(&Term::NamedNode(predicate));
        let o = ds.intern(&Term::NamedNode(object));
        crate::profile::enable();
        for _ in 0..3 {
            assert_eq!(ds.default_graph.scan(None, Some(p), Some(o)).count(), 128);
        }
        let profile = crate::profile::take().unwrap();
        assert_eq!(
            profile.indexes()[0].predicate.as_deref(),
            Some("http://ex/inferred")
        );
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
    fn named_graph_bound_subject_and_predicate_ranges_preserve_answers() {
        let data = Graph::new();
        let mut shapes = Graph::new();
        for triple in [
            triple_nnn("http://ex/a", "http://ex/p", "http://ex/one"),
            triple_nnn("http://ex/a", "http://ex/p", "http://ex/two"),
            triple_nnn("http://ex/a", "http://ex/q", "http://ex/three"),
            triple_nnn("http://ex/b", "http://ex/p", "http://ex/four"),
        ] {
            shapes.insert(&triple);
        }
        let ds = FrozenIndexedDataset::from_graphs(&data, &shapes);
        let a = ds.intern(&Term::NamedNode(nn("http://ex/a")));
        let p = ds.intern(&Term::NamedNode(nn("http://ex/p")));
        let g = ds.intern(&Term::NamedNode(nn(SHAPES_GRAPH_IRI)));
        assert_eq!(ds.scan(Some(a), None, None, GraphSel::Named(g)).count(), 3);
        assert_eq!(
            ds.scan(Some(a), Some(p), None, GraphSel::Named(g)).count(),
            2
        );
        assert_eq!(
            ds.scan(
                Some(a),
                Some(p),
                Some(ds.intern(&Term::NamedNode(nn("http://ex/two")))),
                GraphSel::Named(g)
            )
            .count(),
            1
        );
    }

    #[test]
    fn source_ids_and_named_index_are_shared_without_restoring_embedded_deletions() {
        let mut shapes = Graph::new();
        let overlap = triple_nnn("http://ex/a", "http://ex/p", "http://ex/b");
        shapes.insert(&overlap);
        let source = SourceStorage::encode(&shapes);
        let empty = Graph::new();
        let separate =
            FrozenIndexedDataset::from_data_with_source(&empty, Arc::clone(&source), true);
        let embedded_after_delete =
            FrozenIndexedDataset::from_data_with_source(&empty, Arc::clone(&source), false);
        let s = separate.intern(&Term::NamedNode(nn("http://ex/a")));
        let p = separate.intern(&Term::NamedNode(nn("http://ex/p")));
        let o = separate.intern(&Term::NamedNode(nn("http://ex/b")));
        let g = separate.intern(&Term::NamedNode(nn(SHAPES_GRAPH_IRI)));
        assert_eq!(
            s,
            embedded_after_delete.intern(&Term::NamedNode(nn("http://ex/a")))
        );
        assert!(separate.contains_ids(s, p, o));
        assert!(!embedded_after_delete.contains_ids(s, p, o));
        assert_eq!(separate.stats.triple_count, 1);
        assert_eq!(embedded_after_delete.stats.triple_count, 0);
        assert_eq!(separate.default_graph.len(), 0);
        assert_eq!(
            separate.scan(None, None, None, GraphSel::Default).count(),
            1
        );
        assert_eq!(
            embedded_after_delete
                .scan(None, None, None, GraphSel::Default)
                .count(),
            0
        );
        assert_eq!(
            embedded_after_delete
                .scan(Some(s), Some(p), Some(o), GraphSel::Named(g))
                .count(),
            1
        );
        assert!(Arc::ptr_eq(
            separate.named_graphs.get(&g).unwrap(),
            embedded_after_delete.named_graphs.get(&g).unwrap(),
        ));
    }

    #[test]
    fn separate_overlap_has_two_memberships_but_one_union_result() {
        let mut shapes = Graph::new();
        let overlap = triple_nnn("http://ex/a", "http://ex/p", "http://ex/b");
        shapes.insert(&overlap);
        shapes.insert(&triple_nnn(
            "http://ex/a",
            "http://ex/q",
            "http://ex/source",
        ));
        let mut data = Graph::new();
        data.insert(&overlap);
        data.insert(&triple_nnn("http://ex/a", "http://ex/r", "http://ex/local"));
        let source = SourceStorage::encode(&shapes);
        let mut union =
            FrozenIndexedDataset::from_data_with_source(&data, Arc::clone(&source), true);
        let data_only = FrozenIndexedDataset::from_data_with_source(&data, source, false);
        let g = union.intern(&Term::NamedNode(nn(SHAPES_GRAPH_IRI)));
        assert_eq!(union.scan(None, None, None, GraphSel::Default).count(), 3);
        assert_eq!(
            data_only.scan(None, None, None, GraphSel::Default).count(),
            2
        );
        assert_eq!(union.scan(None, None, None, GraphSel::Named(g)).count(), 2);
        assert_eq!(union.stats.triple_count, 3);
        assert_eq!(data_only.stats.triple_count, 2);
        union.select_data_view();
        assert_eq!(
            union
                .scan(None, None, None, GraphSel::Default)
                .collect::<HashSet<_>>(),
            data_only
                .scan(None, None, None, GraphSel::Default)
                .collect::<HashSet<_>>()
        );
        assert_eq!(union.scan(None, None, None, GraphSel::Named(g)).count(), 2);
        assert_eq!(union.stats.triple_count, 2);
    }

    #[test]
    fn embedded_source_membership_uses_exclusions_after_deletion() {
        let mut source_graph = Graph::new();
        let first = triple_nnn("http://ex/a", "http://ex/p", "http://ex/one");
        let second = triple_nnn("http://ex/a", "http://ex/p", "http://ex/two");
        source_graph.insert(&first);
        source_graph.insert(&second);
        let source = SourceStorage::encode(&source_graph);
        let full =
            FrozenIndexedDataset::from_data_with_source(&source_graph, Arc::clone(&source), false);
        assert!(matches!(
            full.source_view.as_ref().unwrap().membership,
            SourceMembership::All
        ));
        let mut edited = source_graph.clone();
        edited.remove(&second);
        let after_delete = FrozenIndexedDataset::from_data_with_source(&edited, source, false);
        assert_eq!(
            after_delete
                .scan(None, None, None, GraphSel::Default)
                .count(),
            1
        );
        assert_eq!(after_delete.stats.triple_count, 1);
        assert_eq!(after_delete.default_graph.len(), 0);
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
