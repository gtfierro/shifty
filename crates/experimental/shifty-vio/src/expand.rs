//! The `VIO`/`SAT` rewrite system.
//!
//! `VIO(x, φ)` rewrites the satisfaction evidence of a holding shape into
//! edit sets that make it fail; `SAT(x, φ)` rewrites failure evidence into
//! edit sets that make it hold. They cross at `¬`. Each choice point keeps a
//! bounded prefix of its alternatives ([`ExpansionLimits`]), and siblings
//! are ordered by content rather than by arena id, so expansion reproduces in every process.

use crate::ExpansionLimits;
use crate::hash::{Rng, StableHasher};
use crate::index::{self, Step};
use crate::model::{Operation, OperationKind, Polarity, merge};
use crate::nt::{self, Triple};
use crate::propose;
use crate::universe::{Core, Target};
use shifty_algebra::{Path, Shape, ShapeId, ValueType};
use shifty_engine::{BlockReason, Evidence, Failure, PathSupport, Satisfaction};
use std::collections::{BTreeSet, HashMap};

/// One conjunction of operations.
pub(crate) type Alt = Vec<Operation>;

/// A value reached from `owner` along a path, with the qualifier that
/// governs it.
struct Value {
    owner: String,
    term: String,
    edge: Option<Triple>,
    support: Vec<Triple>,
    qualifier: ShapeId,
    constraint: ShapeId,
}

pub(crate) struct Expander<'g> {
    g: &'g Core,
    lim: ExpansionLimits,
    target: &'g Target,
    rng: Rng,
    /// `(node, path)` → universals that hold on that path, as (qualifier,
    /// status a value needs). New values prefer to keep them holding.
    universals: HashMap<(String, Path), BTreeSet<(ShapeId, bool)>>,
    /// Content keys by node address. Every evidence tree the expansion
    /// reads stays alive in the generator, so addresses are not reused.
    memo: HashMap<usize, u64>,
}

impl<'g> Expander<'g> {
    pub fn new(g: &'g Core, target: &'g Target) -> Self {
        let seed = StableHasher::new()
            .str(&target.focus)
            .str(target.shape.as_deref().unwrap_or(""))
            .finish();
        Self {
            g,
            lim: g.limits(),
            target,
            rng: Rng::new(seed),
            universals: HashMap::new(),
            memo: HashMap::new(),
        }
    }

    /// `VIO` of the target, each alternative tagged with the top-level
    /// conjunct it breaks, interleaved across conjuncts.
    pub fn components(&mut self, root: &Satisfaction, limit: usize) -> Vec<(ShapeId, Alt)> {
        let parts: Vec<(ShapeId, &Satisfaction)> = match root {
            Satisfaction::AllHeld { children, .. } => {
                self.register(children);
                self.stable_sat(children)
                    .into_iter()
                    .map(|c| (sat_shape(c), c))
                    .collect()
            }
            other => vec![(sat_shape(other), other)],
        };
        let groups: Vec<Vec<(ShapeId, Alt)>> = parts
            .into_iter()
            .map(|(cid, ev)| self.vio(ev, 1).into_iter().map(|a| (cid, a)).collect())
            .collect();
        round_robin(groups, limit)
    }

    // ── combinators ──

    /// Round-robin interleaving, so a capped prefix samples every branch.
    fn union(&self, branches: Vec<Vec<Alt>>) -> Vec<Alt> {
        round_robin(branches, self.lim.max_alternatives)
    }

    /// Joint choice: one alternative from every pool, merged.
    fn product(&mut self, pools: Vec<Vec<Alt>>) -> Vec<Alt> {
        if pools.is_empty() {
            return Vec::new();
        }
        let mut pools = pools;
        for pool in &mut pools {
            if pool.is_empty() {
                return Vec::new();
            }
            pool.truncate(self.lim.max_alternatives);
            self.rng.shuffle(pool);
        }
        let mut out = Vec::new();
        let mut idx = vec![0usize; pools.len()];
        let mut attempts = 0;
        loop {
            let combo = pools.iter().zip(&idx).map(|(p, &i)| p[i].as_slice());
            if let Some(merged) = merge(combo) {
                out.push(merged);
                if out.len() >= self.lim.max_combinations {
                    break;
                }
            }
            attempts += 1;
            if attempts >= self.attempt_cap() || !odometer(&mut idx, &pools) {
                break;
            }
        }
        out
    }

    fn attempt_cap(&self) -> usize {
        self.lim.max_combinations.saturating_mul(64).max(64)
    }

    // ── VIO: make a holding shape fail ──

    fn vio(&mut self, ev: &Satisfaction, depth: usize) -> Vec<Alt> {
        if depth > self.lim.max_depth {
            return Vec::new();
        }
        match ev {
            Satisfaction::AllHeld { children, .. } => {
                self.register(children);
                let branches = self
                    .stable_sat(children)
                    .into_iter()
                    .map(|c| self.vio(c, depth + 1))
                    .collect();
                self.union(branches)
            }
            Satisfaction::AnyHeld { satisfied, .. } => {
                let pools = self
                    .stable_sat(satisfied)
                    .into_iter()
                    .map(|c| self.vio(c, depth + 1))
                    .collect();
                self.product(pools)
            }
            Satisfaction::NotHeld { inner_fails, .. } => self.sat(inner_fails, depth + 1),
            Satisfaction::ForAllHeld {
                shape,
                node,
                path,
                qualifier,
                values,
            } => self.vio_forall(*shape, &nt::term(node), path, *qualifier, values, depth),
            Satisfaction::CountHeld {
                shape,
                node,
                path,
                qualifier,
                matches,
                observed_count,
                min,
                max,
            } => {
                let owner = nt::term(node);
                let mut branches = Vec::new();
                if let Some(lo) = min.filter(|lo| *lo > 0) {
                    // ∃≥n π.q holds: knock out observed − n + 1 qualifying values.
                    let need = (observed_count + 1).saturating_sub(lo) as usize;
                    let rows = by_term(matches);
                    let rows = self.rng.choose(&rows, self.lim.max_values.max(need));
                    let per_value = rows
                        .into_iter()
                        .map(|(v, support, trace)| {
                            (value(&owner, v, support, *qualifier, *shape), trace)
                        })
                        .collect();
                    branches.push(self.knock_out(per_value, need, depth, *qualifier));
                }
                if let Some(hi) = max {
                    // ∃≤n π.q holds: make n − observed + 1 more values qualify.
                    let need = (hi + 1).saturating_sub(*observed_count) as usize;
                    let qualifying = matches.iter().map(|m| nt::term(&m.0)).collect();
                    branches.push(self.add_values(AddValues {
                        owner: &owner,
                        path,
                        q: *qualifier,
                        constraint: *shape,
                        want: true,
                        need,
                        exclude: &qualifying,
                        polarity: Polarity::Violate,
                        also: &[],
                        depth,
                    }));
                }
                self.union(branches)
            }
            Satisfaction::Blocked {
                shape,
                node,
                reason: BlockReason::ClosedNeedsAdd,
            } => self.vio_closed(*shape, &nt::term(node)),
            // Irrefutable, coinductive, focus-level atoms, relational and
            // SPARQL constraints: no edit here falsifies them.
            _ => Vec::new(),
        }
    }

    fn vio_forall(
        &mut self,
        shape: ShapeId,
        owner: &str,
        path: &Path,
        q: ShapeId,
        values: &[(oxrdf::Term, PathSupport, Satisfaction)],
        depth: usize,
    ) -> Vec<Alt> {
        let rows = by_term(values);
        let mut branches = Vec::new();
        for (v, support, trace) in self.rng.choose(&rows, self.lim.max_values) {
            let val = value(owner, v, support, q, shape);
            branches.push(self.substitute(&val, false, Polarity::Violate, &[]));
            branches.push(self.vio(trace, depth + 1));
        }
        let existing = values.iter().map(|v| nt::term(&v.0)).collect();
        branches.push(self.add_values(AddValues {
            owner,
            path,
            q,
            constraint: shape,
            want: false,
            need: 1,
            exclude: &existing,
            polarity: Polarity::Violate,
            also: &[],
            depth,
        }));
        self.union(branches)
    }

    /// Every way to make `need` of these values stop counting (Rule 2): cut
    /// the edge that reaches one, substitute a value that does not qualify,
    /// or break the qualifier at the value.
    fn knock_out(
        &mut self,
        per_value: Vec<(Value, &Satisfaction)>,
        need: usize,
        depth: usize,
        q: ShapeId,
    ) -> Vec<Alt> {
        if need == 0 || need > per_value.len() {
            return Vec::new();
        }
        let top = self.is_top(q);
        let options: Vec<Vec<Alt>> = per_value
            .iter()
            .map(|(val, trace)| {
                let mut parts = vec![self.cut(val, Polarity::Violate)];
                if !top {
                    parts.push(self.substitute(val, false, Polarity::Violate, &[]));
                    parts.push(self.vio(trace, depth + 1));
                }
                self.union(parts)
            })
            .collect();
        let subsets = combinations(per_value.len(), need, self.lim.max_combinations);
        let alts = subsets
            .into_iter()
            .map(|subset| {
                let pools = subset.iter().map(|&i| options[i].clone()).collect();
                self.product(pools)
            })
            .collect();
        self.union(alts)
    }

    fn vio_closed(&mut self, shape: ShapeId, owner: &str) -> Vec<Alt> {
        if nt::is_literal(owner) {
            return Vec::new();
        }
        let allowed: BTreeSet<String> = match self.shape(shape) {
            Shape::Closed(preds) => preds.iter().map(ToString::to_string).collect(),
            _ => BTreeSet::new(),
        };
        let index = self.g.index();
        let preds: Vec<String> = index
            .predicates()
            .iter()
            .filter(|p| !allowed.contains(*p))
            .cloned()
            .collect();
        let mut out = Vec::new();
        for p in self.rng.choose(&preds, self.lim.max_candidates) {
            let objects = index.objects_of(&p);
            if objects.is_empty() {
                continue;
            }
            let o = objects[self.rng.below(objects.len())].clone();
            let t = (owner.to_owned(), p.clone(), o);
            if index.contains(&t) {
                continue;
            }
            out.push(vec![Operation {
                kind: OperationKind::AddForbidden,
                node: owner.to_owned(),
                constraint: shape,
                polarity: Polarity::Violate,
                deletes: Vec::new(),
                adds: vec![t],
                requires: Vec::new(),
                detail: format!("add {} outside the closed shape", nt::short(&p)),
            }]);
        }
        out
    }

    // ── SAT: make a failing shape hold ──

    fn sat(&mut self, ev: &Failure, depth: usize) -> Vec<Alt> {
        if depth > self.lim.max_depth {
            return Vec::new();
        }
        match ev {
            Failure::All { failed, .. } => {
                let pools = self
                    .stable_fail(failed)
                    .into_iter()
                    .map(|c| self.sat(c, depth + 1))
                    .collect();
                self.product(pools)
            }
            Failure::Any { branches, .. } => {
                let alts = self
                    .stable_fail(branches)
                    .into_iter()
                    .map(|c| self.sat(c, depth + 1))
                    .collect();
                self.union(alts)
            }
            Failure::Not { inner, .. } => self.vio(inner, depth + 1),
            Failure::CountLow {
                shape,
                node,
                path,
                qualifier,
                have,
                min,
                qualifying_matches,
                rejected_candidates,
                sibling_qualifiers,
            } => {
                let owner = nt::term(node);
                let q = *qualifier;
                let need = min.saturating_sub(*have) as usize;
                let mut rejected: Vec<_> = rejected_candidates.iter().collect();
                rejected.sort_by_cached_key(|c| nt::term(&c.value));
                let mut units: Vec<(String, Vec<Alt>)> = Vec::new();
                for cand in self.rng.choose(&rejected, self.lim.max_values) {
                    let val = value(&owner, &cand.value, &cand.path_support, q, *shape);
                    let parts = vec![
                        self.substitute(&val, true, Polarity::Satisfy, sibling_qualifiers),
                        self.sat(&cand.failure, depth + 1),
                    ];
                    let alts = self.union(parts);
                    units.push((val.term, alts));
                }
                let mut exclude: BTreeSet<String> = qualifying_matches
                    .iter()
                    .map(|m| nt::term(&m.value))
                    .collect();
                exclude.extend(units.iter().map(|(t, _)| t.clone()));
                let combined = self.combine(units, need);
                let added = self.add_values(AddValues {
                    owner: &owner,
                    path,
                    q,
                    constraint: *shape,
                    want: true,
                    need,
                    exclude: &exclude,
                    polarity: Polarity::Satisfy,
                    also: sibling_qualifiers,
                    depth,
                });
                self.union(vec![combined, added])
            }
            Failure::CountHigh {
                shape,
                node,
                qualifier,
                matched,
                max,
                ..
            } => {
                let owner = nt::term(node);
                let q = *qualifier;
                let excess = (matched.len() as u64).saturating_sub(*max) as usize;
                let rows = by_term_pairs(matched);
                let rows = self.rng.choose(&rows, self.lim.max_values.max(excess));
                let options: Vec<Vec<Alt>> = rows
                    .into_iter()
                    .map(|(v, support)| {
                        let val = value(&owner, v, support, q, *shape);
                        let mut parts = vec![self.cut(&val, Polarity::Satisfy)];
                        if let Some(ev) = self.g.evidence(&val.term, q)
                            && let Evidence::Satisfaction(trace) = ev.as_ref()
                        {
                            parts.push(self.vio(trace, depth + 1));
                        }
                        self.union(parts)
                    })
                    .collect();
                if excess == 0 || excess > options.len() {
                    return Vec::new();
                }
                let alts = combinations(options.len(), excess, self.lim.max_combinations)
                    .into_iter()
                    .map(|subset| {
                        let pools = subset.iter().map(|&i| options[i].clone()).collect();
                        self.product(pools)
                    })
                    .collect();
                self.union(alts)
            }
            _ => Vec::new(),
        }
    }

    // ── leaf operations ──

    fn cut(&self, val: &Value, polarity: Polarity) -> Vec<Alt> {
        val.support
            .iter()
            .filter(|edge| self.g.index().contains(edge))
            .map(|edge| {
                vec![Operation {
                    kind: OperationKind::Remove,
                    node: val.owner.clone(),
                    constraint: val.constraint,
                    polarity,
                    deletes: vec![edge.clone()],
                    adds: Vec::new(),
                    requires: Vec::new(),
                    detail: format!("remove {} {}", nt::short(&edge.1), nt::short(&edge.2)),
                }]
            })
            .collect()
    }

    /// Replace `val` on its incoming edge with a term whose qualifier status
    /// is `want`.
    fn substitute(
        &mut self,
        val: &Value,
        want: bool,
        polarity: Polarity,
        also: &[ShapeId],
    ) -> Vec<Alt> {
        let Some(edge) = val.edge.as_ref().filter(|e| self.g.index().contains(e)) else {
            return Vec::new();
        };
        let subject = if edge.2 == val.term {
            false
        } else if edge.0 == val.term {
            true
        } else {
            return Vec::new();
        };
        let cands = self.candidates(Candidates {
            current: Some(&val.term),
            q: val.qualifier,
            want,
            pred: Some(&edge.1),
            also,
            limit: None,
            prefer: &[],
            need: 1,
        });
        let mut out = Vec::new();
        for cand in cands {
            if subject && nt::is_literal(&cand) {
                continue;
            }
            let new = if subject {
                (cand.clone(), edge.1.clone(), edge.2.clone())
            } else {
                (edge.0.clone(), edge.1.clone(), cand.clone())
            };
            if self.g.index().contains(&new) {
                continue;
            }
            out.push(vec![Operation {
                kind: OperationKind::Substitute,
                node: val.owner.clone(),
                constraint: val.constraint,
                polarity,
                deletes: vec![edge.clone()],
                adds: vec![new],
                requires: vec![crate::model::Requirement {
                    polarity: Polarity::wanting(want),
                    node: cand.clone(),
                    constraint: val.qualifier,
                }],
                detail: format!("replace {} with {}", nt::short(&val.term), nt::short(&cand)),
            }]);
        }
        out
    }

    /// Make `need` more values reached along `path` have qualifier status
    /// `want`: link new values, or change reached ones in place.
    fn add_values(&mut self, a: AddValues<'_>) -> Vec<Alt> {
        if a.need < 1 {
            return Vec::new();
        }
        let word = index::minimal_word(a.path);
        let reached = self.g.index().eval_path(a.owner, a.path);
        let mut units: Vec<(String, Vec<Alt>)> = Vec::new();
        if a.want {
            // Reached values that do not count yet can be fixed in place.
            let fresh: Vec<String> = reached.difference(a.exclude).cloned().collect();
            for v in self.rng.choose(&fresh, self.lim.max_values) {
                let Some(ev) = self.g.evidence(&v, a.q) else {
                    continue;
                };
                let Evidence::Failure(failure) = ev.as_ref() else {
                    continue;
                };
                let mut fixes = vec![self.sat(failure, a.depth + 1)];
                if let Some(edge) = index::single_edge(a.owner, word.as_deref(), &v) {
                    // Or swap the value for one that counts, such as a status
                    // outside an `sh:in` list.
                    let val = Value {
                        owner: a.owner.to_owned(),
                        term: v.clone(),
                        edge: Some(edge.clone()),
                        support: vec![edge],
                        qualifier: a.q,
                        constraint: a.constraint,
                    };
                    fixes.push(self.substitute(&val, true, a.polarity, a.also));
                }
                let alts = self.union(fixes);
                units.push((v, alts));
            }
        }
        if let Some(word) = word.filter(|w| !w.is_empty()) {
            let seed_value = reached.iter().next().cloned();
            let pred = word.last().map(|(p, _)| p.clone());
            let mut prefer: Vec<(ShapeId, bool)> = self
                .universals
                .get(&(a.owner.to_owned(), a.path.clone()))
                .map(|rules| {
                    rules
                        .iter()
                        .filter(|rule| **rule != (a.q, a.want))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();
            prefer.sort_by_cached_key(|(sid, want)| (self.g.shape_key(*sid), *want));
            let cands = self.candidates(Candidates {
                current: seed_value.as_deref(),
                q: a.q,
                want: a.want,
                pred: pred.as_deref(),
                also: a.also,
                limit: Some(self.lim.max_candidates.max(a.need)),
                prefer: &prefer,
                need: a.need,
            });
            for cand in cands {
                if reached.contains(&cand) || a.exclude.contains(&cand) {
                    continue;
                }
                let Some(triples) = self.materialize(a.owner, &word, &cand) else {
                    continue;
                };
                if triples.iter().any(|t| self.g.index().contains(t)) {
                    continue;
                }
                let op = Operation {
                    kind: OperationKind::AddValue,
                    node: a.owner.to_owned(),
                    constraint: a.constraint,
                    polarity: a.polarity,
                    deletes: Vec::new(),
                    adds: triples,
                    requires: vec![crate::model::Requirement {
                        polarity: Polarity::wanting(a.want),
                        node: cand.clone(),
                        constraint: a.q,
                    }],
                    detail: format!("add {} via {}", nt::short(&cand), index::short_word(&word)),
                };
                units.push((cand, vec![vec![op]]));
            }
        }
        self.combine(units, a.need)
    }

    /// Choose `need` units over distinct values and merge their edits, one
    /// joint choice per subset of units, so subsets stay diverse.
    fn combine(&self, units: Vec<(String, Vec<Alt>)>, need: usize) -> Vec<Alt> {
        let pools: Vec<Vec<Alt>> = units
            .into_iter()
            .map(|(_, mut alts)| {
                alts.truncate(self.lim.max_candidates);
                alts
            })
            .filter(|alts| !alts.is_empty())
            .collect();
        if need == 0 || pools.len() < need {
            return Vec::new();
        }
        let mut out = Vec::new();
        let cap = self.attempt_cap();
        let mut attempts = 0;
        for subset in combinations(pools.len(), need, cap) {
            let chosen: Vec<Vec<Alt>> = subset.iter().map(|&i| pools[i].clone()).collect();
            let mut idx = vec![0usize; chosen.len()];
            loop {
                attempts += 1;
                let combo = chosen.iter().zip(&idx).map(|(p, &i)| p[i].as_slice());
                if let Some(merged) = merge(combo) {
                    out.push(merged);
                    break;
                }
                if attempts >= cap || !odometer(&mut idx, &chosen) {
                    break;
                }
            }
            if out.len() >= self.lim.max_combinations || attempts >= cap {
                break;
            }
        }
        out
    }

    /// The triples linking `owner` to `value` along `word`, through fresh
    /// intermediate nodes.
    fn materialize(&self, owner: &str, word: &[Step], value: &str) -> Option<Vec<Triple>> {
        let mut nodes = vec![owner.to_owned()];
        for i in 0..word.len() - 1 {
            nodes.push(self.fresh(&format!("{owner}|{value}|{i}")));
        }
        nodes.push(value.to_owned());
        let mut out = Vec::new();
        for ((pred, inverse), pair) in word.iter().zip(nodes.windows(2)) {
            let (s, o) = if *inverse {
                (&pair[1], &pair[0])
            } else {
                (&pair[0], &pair[1])
            };
            if nt::is_literal(s) {
                return None;
            }
            out.push((s.clone(), pred.clone(), o.clone()));
        }
        Some(out)
    }

    // ── candidate terms ──

    /// Terms whose status for qualifier `q` is `want`, checked by the
    /// engine; `also` lists sibling qualifiers they must satisfy too.
    ///
    /// Terms that also keep each `prefer` rule come first; the others fill
    /// in only when fewer than `need` do. A value added to break a maximum
    /// count then stays well formed for the other constraints on its path,
    /// so the case is a cardinality error rather than a mix.
    fn candidates(&mut self, c: Candidates<'_>) -> Vec<String> {
        let limit = c.limit.unwrap_or(self.lim.max_candidates);
        let hints: Vec<ShapeId> = c.prefer.iter().map(|(sid, _)| *sid).collect();
        let pool = self.proposals(c.current, c.q, c.pred, &hints);
        let mut good = Vec::new();
        let mut fallback = Vec::new();
        for cand in pool {
            if Some(cand.as_str()) == c.current {
                continue;
            }
            if self.g.holds(&cand, c.q) != Some(c.want) {
                continue;
            }
            if c.also.iter().any(|s| self.g.holds(&cand, *s) != Some(true)) {
                continue;
            }
            if c.prefer
                .iter()
                .all(|(r, w)| self.g.holds(&cand, *r) == Some(*w))
            {
                good.push(cand);
                if good.len() >= limit {
                    break;
                }
            } else if fallback.len() < limit {
                fallback.push(cand);
            }
        }
        if good.len() >= c.need {
            return good;
        }
        good.extend(fallback);
        good.truncate(limit);
        good
    }

    fn proposals(
        &mut self,
        current: Option<&str>,
        q: ShapeId,
        pred: Option<&str>,
        hints: &[ShapeId],
    ) -> Vec<String> {
        let mut leaves = self.g.leaves(q).as_ref().clone();
        for hint in hints {
            let more = self.g.leaves(*hint);
            leaves.consts.extend(more.consts.iter().cloned());
            leaves.facets.extend(more.facets.iter().cloned());
            leaves.classes.extend(more.classes.iter().cloned());
        }
        let consts: BTreeSet<String> = leaves.consts.into_iter().collect();
        let classes: BTreeSet<String> = leaves.classes.into_iter().collect();
        let mut facets = leaves.facets;
        facets.sort_by_cached_key(|f| format!("{f:?}"));
        let index = self.g.index();
        // With no current value, a value of the same predicate elsewhere
        // tells what kind of term the predicate takes.
        let siblings = pred.map_or(&[][..], |p| index.objects_of(p));
        let reference = current.or(siblings.first().map(String::as_str));
        let (lex, dt, lang) = match reference {
            Some(r) => nt::literal_parts(r),
            None => ("x".to_owned(), None, None),
        };
        let mut props: Vec<String> = consts.into_iter().collect();
        for class in &classes {
            props.extend(self.rng.choose(index.instances_of(class), 3));
        }
        for facet in &facets {
            props.extend(propose::facet_proposals(
                facet,
                &lex,
                dt.as_deref(),
                lang.as_deref(),
            ));
        }
        props.extend(propose::mutations(
            reference,
            &lex,
            dt.as_deref(),
            lang.as_deref(),
        ));
        props.extend(self.rng.choose(siblings, 4));
        if let Some(cur) = current.filter(|c| !nt::is_literal(c)) {
            for class in index.types_of(cur) {
                props.extend(self.rng.choose(index.instances_of(class), 3));
            }
        }
        props.extend(self.rng.choose(index.typed_nodes(), 4));
        props.push(self.fresh(&format!(
            "{}|{}",
            current.unwrap_or("None"),
            pred.unwrap_or("None")
        )));
        let mut seen = BTreeSet::new();
        props.retain(|p| seen.insert(p.clone()));
        if let Some(r) = reference {
            // Prefer replacements of the same kind, so an object property
            // keeps getting nodes and a datatype property literals. Other
            // kinds remain as a fallback.
            let literal = nt::is_literal(r);
            props.sort_by_key(|t| nt::is_literal(t) != literal);
        }
        props
    }

    fn fresh(&self, hint: &str) -> String {
        let focus = &self.target.focus;
        let base = focus
            .strip_prefix('<')
            .and_then(|f| f.strip_suffix('>'))
            .unwrap_or("urn:vio:");
        let ns = match base.rfind(['/', '#', ':']) {
            Some(cut) => &base[..=cut],
            None => base,
        };
        let digest = crate::hash::hash_str(hint) & 0xffff_ffff;
        format!("<{ns}vio_{digest:08x}>")
    }

    // ── helpers ──

    fn shape(&self, sid: ShapeId) -> &'g Shape {
        self.g.unwrap_shape(sid)
    }

    fn is_top(&self, sid: ShapeId) -> bool {
        matches!(self.shape(sid), Shape::Top)
    }

    fn register(&mut self, children: &[Satisfaction]) {
        for child in children {
            let (node, path, rule) = match child {
                Satisfaction::ForAllHeld {
                    node,
                    path,
                    qualifier,
                    ..
                } => (node, path, (*qualifier, true)),
                Satisfaction::CountHeld {
                    node,
                    path,
                    qualifier,
                    min,
                    max: Some(0),
                    ..
                } if min.unwrap_or(0) == 0 => (node, path, (*qualifier, false)),
                _ => continue,
            };
            self.universals
                .entry((nt::term(node), path.clone()))
                .or_default()
                .insert(rule);
        }
    }

    fn stable_sat<'e>(&mut self, evs: &'e [Satisfaction]) -> Vec<&'e Satisfaction> {
        let mut keyed: Vec<(u64, &Satisfaction)> =
            evs.iter().map(|e| (self.sat_key(e), e)).collect();
        keyed.sort_by_key(|(k, _)| *k);
        keyed.into_iter().map(|(_, e)| e).collect()
    }

    fn stable_fail<'e>(&mut self, evs: &'e [Failure]) -> Vec<&'e Failure> {
        let mut keyed: Vec<(u64, &Failure)> = evs.iter().map(|e| (self.fail_key(e), e)).collect();
        keyed.sort_by_key(|(k, _)| *k);
        keyed.into_iter().map(|(_, e)| e).collect()
    }

    /// A digest of an evidence subtree that names shapes by structure, so
    /// siblings order the same in every process.
    fn sat_key(&mut self, ev: &Satisfaction) -> u64 {
        let addr = std::ptr::from_ref(ev) as usize;
        if let Some(k) = self.memo.get(&addr) {
            return *k;
        }
        let mut h = StableHasher::new();
        let sk = |g: &Core, sid: &ShapeId| g.shape_key(*sid);
        match ev {
            Satisfaction::Irrefutable { shape } => {
                h.str("irrefutable").u64(sk(self.g, shape));
            }
            Satisfaction::Atom {
                shape,
                node,
                observed,
                expected,
                reached_by,
                produced_by,
            } => {
                h.str("atom")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string())
                    .str(&format!(
                        "{observed:?}{expected:?}{reached_by:?}{produced_by:?}"
                    ));
            }
            Satisfaction::AllHeld {
                shape,
                node,
                children,
            }
            | Satisfaction::AnyHeld {
                shape,
                node,
                satisfied: children,
            } => {
                let tag = if matches!(ev, Satisfaction::AllHeld { .. }) {
                    "all"
                } else {
                    "any"
                };
                h.str(tag).u64(sk(self.g, shape)).str(&node.to_string());
                for c in children {
                    let k = self.sat_key(c);
                    h.u64(k);
                }
            }
            Satisfaction::CountHeld {
                shape,
                node,
                path,
                qualifier,
                matches,
                observed_count,
                min,
                max,
            } => {
                h.str("count")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string())
                    .str(&format!("{path:?}{observed_count}{min:?}{max:?}"))
                    .u64(sk(self.g, qualifier));
                for (v, support, trace) in matches {
                    h.str(&v.to_string()).str(&format!("{support:?}"));
                    let k = self.sat_key(trace);
                    h.u64(k);
                }
            }
            Satisfaction::ForAllHeld {
                shape,
                node,
                path,
                qualifier,
                values,
            } => {
                h.str("forall")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string())
                    .str(&format!("{path:?}"))
                    .u64(sk(self.g, qualifier));
                for (v, support, trace) in values {
                    h.str(&v.to_string()).str(&format!("{support:?}"));
                    let k = self.sat_key(trace);
                    h.u64(k);
                }
            }
            Satisfaction::NotHeld {
                shape,
                node,
                inner_fails,
            } => {
                let k = self.fail_key(inner_fails);
                h.str("not")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string())
                    .u64(k);
            }
            Satisfaction::Blocked {
                shape,
                node,
                reason,
            } => {
                h.str("blocked")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string())
                    .str(&format!("{reason:?}"));
            }
            Satisfaction::Coinductive { shape, node } => {
                h.str("coinductive")
                    .u64(sk(self.g, shape))
                    .str(&node.to_string());
            }
        }
        let k = h.finish();
        self.memo.insert(addr, k);
        k
    }

    fn fail_key(&mut self, ev: &Failure) -> u64 {
        let addr = std::ptr::from_ref(ev) as usize;
        if let Some(k) = self.memo.get(&addr) {
            return *k;
        }
        let g = self.g;
        let mut h = StableHasher::new();
        h.str(failure_tag(ev)).u64(g.shape_key(fail_shape(ev)));
        match ev {
            Failure::Atom {
                node,
                observed,
                expected,
                reached_by,
                produced_by,
                ..
            } => {
                h.str(&node.to_string()).str(&format!(
                    "{observed:?}{expected:?}{reached_by:?}{produced_by:?}"
                ));
            }
            Failure::Relational {
                node,
                kind,
                lhs,
                rhs,
                offending,
                ..
            } => {
                h.str(&node.to_string())
                    .str(&format!("{kind:?}{lhs:?}{rhs:?}{offending:?}"));
            }
            Failure::Closed {
                node, offenders, ..
            } => {
                h.str(&node.to_string()).str(&format!("{offenders:?}"));
            }
            Failure::Not { node, inner, .. } => {
                let k = self.sat_key(inner);
                h.str(&node.to_string()).u64(k);
            }
            Failure::All { node, failed, .. }
            | Failure::Any {
                node,
                branches: failed,
                ..
            } => {
                h.str(&node.to_string());
                for c in failed {
                    let k = self.fail_key(c);
                    h.u64(k);
                }
            }
            Failure::CountLow {
                node,
                path,
                qualifier,
                have,
                min,
                qualifying_matches,
                rejected_candidates,
                sibling_qualifiers,
                ..
            } => {
                h.str(&node.to_string())
                    .str(&format!("{path:?}{have}{min}"))
                    .u64(g.shape_key(*qualifier));
                for m in qualifying_matches {
                    h.str(&m.value.to_string())
                        .str(&format!("{:?}", m.path_support));
                    let k = self.sat_key(&m.satisfaction);
                    h.u64(k);
                }
                for c in rejected_candidates {
                    h.str(&c.value.to_string())
                        .str(&format!("{:?}", c.path_support));
                    let k = self.fail_key(&c.failure);
                    h.u64(k);
                }
                let mut siblings: Vec<u64> =
                    sibling_qualifiers.iter().map(|s| g.shape_key(*s)).collect();
                siblings.sort_unstable();
                for s in siblings {
                    h.u64(s);
                }
            }
            Failure::CountHigh {
                node,
                path,
                qualifier,
                matched,
                max,
                excess_values,
                per_value,
                ..
            } => {
                h.str(&node.to_string())
                    .str(&format!("{path:?}{max}{matched:?}{excess_values:?}"))
                    .u64(g.shape_key(*qualifier));
                for (v, f) in per_value {
                    h.str(&v.to_string());
                    let k = self.fail_key(f);
                    h.u64(k);
                }
            }
            Failure::Opaque { node, messages, .. } => {
                h.str(&node.to_string()).str(&format!("{messages:?}"));
            }
        }
        let k = h.finish();
        self.memo.insert(addr, k);
        k
    }
}

struct AddValues<'a> {
    owner: &'a str,
    path: &'a Path,
    q: ShapeId,
    constraint: ShapeId,
    want: bool,
    need: usize,
    exclude: &'a BTreeSet<String>,
    polarity: Polarity,
    also: &'a [ShapeId],
    depth: usize,
}

struct Candidates<'a> {
    current: Option<&'a str>,
    q: ShapeId,
    want: bool,
    pred: Option<&'a str>,
    also: &'a [ShapeId],
    limit: Option<usize>,
    prefer: &'a [(ShapeId, bool)],
    need: usize,
}

/// Constants, value-type facets, and classes mentioned locally under a
/// qualifier: the raw material for proposals.
#[derive(Debug, Clone, Default)]
pub(crate) struct Leaves {
    pub consts: Vec<String>,
    pub facets: Vec<ValueType>,
    pub classes: Vec<String>,
}

pub(crate) fn sat_shape(ev: &Satisfaction) -> ShapeId {
    match ev {
        Satisfaction::Irrefutable { shape }
        | Satisfaction::Atom { shape, .. }
        | Satisfaction::AllHeld { shape, .. }
        | Satisfaction::AnyHeld { shape, .. }
        | Satisfaction::CountHeld { shape, .. }
        | Satisfaction::ForAllHeld { shape, .. }
        | Satisfaction::NotHeld { shape, .. }
        | Satisfaction::Blocked { shape, .. }
        | Satisfaction::Coinductive { shape, .. } => *shape,
    }
}

pub(crate) fn fail_shape(ev: &Failure) -> ShapeId {
    match ev {
        Failure::Atom { shape, .. }
        | Failure::Relational { shape, .. }
        | Failure::Closed { shape, .. }
        | Failure::Not { shape, .. }
        | Failure::All { shape, .. }
        | Failure::Any { shape, .. }
        | Failure::CountLow { shape, .. }
        | Failure::CountHigh { shape, .. }
        | Failure::Opaque { shape, .. } => *shape,
    }
}

fn failure_tag(ev: &Failure) -> &'static str {
    match ev {
        Failure::Atom { .. } => "atom",
        Failure::Relational { .. } => "relational",
        Failure::Closed { .. } => "closed",
        Failure::Not { .. } => "not",
        Failure::All { .. } => "all",
        Failure::Any { .. } => "any",
        Failure::CountLow { .. } => "count_low",
        Failure::CountHigh { .. } => "count_high",
        Failure::Opaque { .. } => "opaque",
    }
}

fn value(owner: &str, v: &oxrdf::Term, support: &PathSupport, q: ShapeId, c: ShapeId) -> Value {
    let mut edges = Vec::new();
    support_edges(support, &mut edges);
    Value {
        owner: owner.to_owned(),
        term: nt::term(v),
        edge: edges.last().cloned(),
        support: edges,
        qualifier: q,
        constraint: c,
    }
}

fn support_edges(support: &PathSupport, out: &mut Vec<Triple>) {
    match support {
        PathSupport::Empty => {}
        PathSupport::Edge(t) => out.push(nt::triple(t)),
        PathSupport::Chain(parts) => {
            for part in parts {
                support_edges(part, out);
            }
        }
        PathSupport::Alt(parts) => {
            if let Some(first) = parts.first() {
                support_edges(first, out);
            }
        }
    }
}

fn by_term<A, B>(rows: &[(oxrdf::Term, A, B)]) -> Vec<(&oxrdf::Term, &A, &B)> {
    let mut out: Vec<_> = rows.iter().map(|(t, a, b)| (t, a, b)).collect();
    out.sort_by_cached_key(|row| nt::term(row.0));
    out
}

fn by_term_pairs<A>(rows: &[(oxrdf::Term, A)]) -> Vec<(&oxrdf::Term, &A)> {
    let mut out: Vec<_> = rows.iter().map(|(t, a)| (t, a)).collect();
    out.sort_by_cached_key(|row| nt::term(row.0));
    out
}

/// Interleave groups one item at a time, up to `limit` items.
fn round_robin<T>(groups: Vec<Vec<T>>, limit: usize) -> Vec<T> {
    let mut iters: Vec<_> = groups.into_iter().map(Vec::into_iter).collect();
    let mut out = Vec::new();
    while out.len() < limit {
        let mut any = false;
        for it in &mut iters {
            if let Some(item) = it.next() {
                any = true;
                out.push(item);
                if out.len() >= limit {
                    return out;
                }
            }
        }
        if !any {
            break;
        }
    }
    out
}

/// Advance a mixed-radix counter over `pools`, last position fastest.
/// `false` once it wraps around.
fn odometer<T>(idx: &mut [usize], pools: &[Vec<T>]) -> bool {
    for pos in (0..idx.len()).rev() {
        idx[pos] += 1;
        if idx[pos] < pools[pos].len() {
            return true;
        }
        idx[pos] = 0;
    }
    false
}

/// The first `limit` `k`-subsets of `0..n`, in lexicographic order.
fn combinations(n: usize, k: usize, limit: usize) -> Vec<Vec<usize>> {
    if k > n {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut idx: Vec<usize> = (0..k).collect();
    loop {
        out.push(idx.clone());
        if out.len() >= limit {
            break;
        }
        let Some(pos) = (0..k).rev().find(|&i| idx[i] != i + n - k) else {
            break;
        };
        idx[pos] += 1;
        for j in pos + 1..k {
            idx[j] = idx[j - 1] + 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combinations_are_lexicographic() {
        assert_eq!(
            combinations(4, 2, 100),
            vec![
                vec![0, 1],
                vec![0, 2],
                vec![0, 3],
                vec![1, 2],
                vec![1, 3],
                vec![2, 3]
            ]
        );
        assert_eq!(combinations(3, 0, 10), vec![Vec::<usize>::new()]);
        assert!(combinations(2, 3, 10).is_empty());
        assert_eq!(combinations(5, 2, 3).len(), 3);
    }

    #[test]
    fn round_robin_interleaves() {
        assert_eq!(
            round_robin(vec![vec![1, 2, 3], vec![], vec![10, 20]], 4),
            vec![1, 10, 2, 20]
        );
    }
}
