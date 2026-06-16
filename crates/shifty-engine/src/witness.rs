//! The witnessing evaluator (`docs/06-repair.md` §5) — the structured, lossless
//! sibling of [`explain`](crate::validate). For a focus node that fails a
//! statement it returns a [`Witness`]: the failed sub-DAG of `φ`, pruned to
//! exactly what did not hold, with the structural gap at each node. Its dual,
//! [`SatTrace`], records *why* a shape currently holds, so a `Not(φ)` failure can
//! be repaired by breaking `φ`. The two are mutually recursive through `Not`.
//!
//! This is the input to repair synthesis; it makes no repair decisions. It reuses
//! the [`ShapeEvaluator`] satisfaction oracle (`holds`) and its gfp back-edge
//! guard verbatim, so witnessing agrees with validation by construction.

use crate::frozen::FrozenIndexedDataset;
use crate::path::{PathBackend, node_of, succ};
use crate::sparql::SparqlExecutor;
use crate::validate::{
    NonStratifiable, ShapeEvaluator, focus_nodes_with, uses_shapes_graph,
};
use oxrdf::{Graph, NamedNode, Term, Triple};
use serde::{Deserialize, Serialize};
use shifty_algebra::{Path, Schema, Shape, ShapeId};
use shifty_opt::analyze;
use std::collections::{BTreeSet, HashSet, VecDeque};

/// Why one focus node failed one statement: the failed sub-structure of `φ`,
/// pruned to exactly the parts that did not hold. The input to repair synthesis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusWitness {
    pub focus: Term,
    /// Index of the violated `(selector, shape)` statement in the schema.
    pub statement: usize,
    pub failure: Witness,
}

/// The relational (pairwise) leaf constraints — distinct from value-type atoms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelKind {
    Eq,
    Disj,
    Lt,
    Le,
    UniqueLang,
}

/// The existing triples that make a node a `π`-successor of its parent: the
/// edges a deletion would cut. The deletion-side dual of path materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathSupport {
    /// Reflexive (`Id`): the node reached itself; nothing to cut.
    Empty,
    /// A single triple edge whose removal breaks this step.
    Edge(Triple),
    /// A chain (`Seq`/`Star` expansion): every edge present; cut any one to break.
    Chain(Vec<PathSupport>),
    /// Parallel witnessing chains (`Alt`): cut all to break.
    Alt(Vec<PathSupport>),
}

/// The failed sub-structure of `φ` (additive direction: what to *add*).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Witness {
    /// A value-type leaf failed at `node` (TestConst / TestType / TestKind).
    /// `produced_by` names the triples that made `node` a value (`Some` for a
    /// value-scoped atom, `None` for one on the focus itself).
    Atom {
        shape: ShapeId,
        node: Term,
        reached_by: Path,
        produced_by: Option<PathSupport>,
    },
    /// A relational leaf failed (Eq / Disj / Lt / Le / UniqueLang): `lhs`/`rhs`
    /// carry the two compared value-sets with their support, `offending` the
    /// witnessing pairs/members.
    Relational {
        shape: ShapeId,
        node: Term,
        kind: RelKind,
        lhs: Vec<(Term, PathSupport)>,
        rhs: Vec<(Term, PathSupport)>,
        offending: Vec<(Term, Term)>,
    },
    /// `closed(Q)` failed: these (predicate, object) pairs are not allowed.
    Closed {
        shape: ShapeId,
        node: Term,
        offenders: Vec<(NamedNode, Term)>,
    },
    /// `¬φ` failed because `φ` holds at `node` — it must be falsified.
    Not {
        shape: ShapeId,
        node: Term,
        inner: Box<SatTrace>,
    },
    /// Conjunction: every listed child failed and ALL must be repaired.
    All {
        shape: ShapeId,
        node: Term,
        failed: Vec<Witness>,
    },
    /// Disjunction: no branch held; repairing ANY ONE suffices.
    Any {
        shape: ShapeId,
        node: Term,
        branches: Vec<Witness>,
    },
    /// `∃≥min π.q` under-satisfied: `have` values match, `min` required.
    CountLow {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        have: u64,
        min: u64,
    },
    /// `∃≤max π.q` over-satisfied. `matched` pairs each counted value with its
    /// support (so deletion cuts the right edges for `Seq`/`Star` paths).
    /// `per_value` is populated only for the `∀`-encoding (`∃≤0 π.¬inner`).
    CountHigh {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        matched: Vec<(Term, PathSupport)>,
        max: u64,
        per_value: Vec<(Term, Witness)>,
    },
    /// Opaque SPARQL — no algebraic witness.
    Opaque {
        shape: ShapeId,
        node: Term,
    },
}

/// Why `φ` currently *holds* at a node (deletive direction: what to *delete*).
/// The dual of [`Witness`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SatTrace {
    /// `⊤` — vacuously true; no graph edit falsifies it.
    Irrefutable { shape: ShapeId },
    /// A value-type leaf holds at `node`; `produced_by` names the edges to cut.
    Atom {
        shape: ShapeId,
        node: Term,
        reached_by: Path,
        produced_by: PathSupport,
    },
    /// Conjunction holds because ALL children hold ⟹ break ANY ONE.
    AllHeld {
        shape: ShapeId,
        node: Term,
        children: Vec<SatTrace>,
    },
    /// Disjunction holds because these branches hold ⟹ break EVERY one.
    AnyHeld {
        shape: ShapeId,
        node: Term,
        satisfied: Vec<SatTrace>,
    },
    /// `∃[min..max] π.q` holds. `matches` carries each counted value with its
    /// q-support.
    CountHeld {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        matches: Vec<(Term, SatTrace)>,
        min: Option<u64>,
        max: Option<u64>,
    },
    /// `¬φ` holds because `φ` fails ⟹ make `φ` hold. Flips to the additive side.
    NotHeld {
        shape: ShapeId,
        node: Term,
        inner_fails: Box<Witness>,
    },
    /// Holds but cannot be falsified by data deletion in scope (closed / relational
    /// / opaque SPARQL).
    Blocked {
        shape: ShapeId,
        node: Term,
        reason: BlockReason,
    },
    /// Support reached only through a gfp back-edge: coinductively assumed true,
    /// with no finite set of facts to delete.
    Coinductive { shape: ShapeId, node: Term },
}

/// Why a holding shape admits no data-deletion repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockReason {
    OpaqueSparql,
    /// Falsifying `closed(Q)` would need *adding* a disallowed predicate.
    ClosedNeedsAdd,
    /// Relational falsification is not synthesized in this cut.
    Unsupported,
}

type Stack = HashSet<(ShapeId, Term)>;

/// Strat-check and build the SPARQL executor shared by every witnessing entry
/// point. The caller derives `backend`/`ShapeEvaluator` from the returned
/// executor — those borrow it, so they can't be bundled in here.
fn prepare(data: &Graph, schema: &Schema) -> Result<SparqlExecutor, NonStratifiable> {
    let strat = analyze(&schema.arena);
    if !strat.stratifiable {
        let components = strat
            .strata
            .iter()
            .filter(|s| !s.stratifiable)
            .map(|s| s.shapes.clone())
            .collect();
        return Err(NonStratifiable { components });
    }

    let uses_shapes = uses_shapes_graph(&schema.arena);
    let frozen = if uses_shapes {
        FrozenIndexedDataset::from_graphs(data, data)
    } else {
        FrozenIndexedDataset::from_graph(data)
    };
    Ok(SparqlExecutor::from_frozen(frozen, uses_shapes))
}

/// Witness every `(focus, statement)` that fails, mirroring `validate`'s driver.
pub fn witness_violations(
    data: &Graph,
    schema: &Schema,
) -> Result<Vec<FocusWitness>, NonStratifiable> {
    let sparql = prepare(data, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(failure) =
                witness(&mut evaluator, &v, st.shape, &Path::Id, None, &mut stack)
            {
                out.push(FocusWitness {
                    focus: v,
                    statement: i,
                    failure,
                });
            }
        }
    }
    Ok(out)
}

/// The arena slot a named shape IRI refers to, if the schema names one. `iri` is
/// matched bare (no angle brackets), the form stored in [`Schema::names`].
pub fn shape_id_for_iri(schema: &Schema, iri: &str) -> Option<ShapeId> {
    schema
        .names
        .iter()
        .find_map(|(id, name)| (name == iri).then_some(*id))
}

/// Witness only the `(focus, statement)` violations whose statement targets
/// `shape` — the shape-scoped sibling of [`witness_violations`]. Returns the
/// *failing* foci with their [`Witness`] trees; passing foci are the domain of
/// [`satisfy_shape`]. Use [`shape_id_for_iri`] to resolve an IRI to its
/// `ShapeId`.
pub fn witness_shape(
    data: &Graph,
    schema: &Schema,
    shape: ShapeId,
) -> Result<Vec<FocusWitness>, NonStratifiable> {
    let sparql = prepare(data, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        if st.shape != shape {
            continue;
        }
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(failure) =
                witness(&mut evaluator, &v, st.shape, &Path::Id, None, &mut stack)
            {
                out.push(FocusWitness {
                    focus: v,
                    statement: i,
                    failure,
                });
            }
        }
    }
    Ok(out)
}

/// Why one focus node *satisfies* one statement: the [`SatTrace`] recording why
/// `φ` holds, including the values matched along each checked path. The
/// satisfaction-side dual of [`FocusWitness`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusSat {
    pub focus: Term,
    /// Index of the satisfied `(selector, shape)` statement in the schema.
    pub statement: usize,
    pub trace: SatTrace,
}

/// Trace every `(focus, statement)` that *holds* for a statement targeting
/// `shape` — the dual of [`witness_shape`]. Each [`FocusSat`] carries why the
/// node conforms, down to the values matched along every checked path (see
/// [`SatTrace`]). Use [`shape_id_for_iri`] to resolve an IRI to its `ShapeId`.
pub fn satisfy_shape(
    data: &Graph,
    schema: &Schema,
    shape: ShapeId,
) -> Result<Vec<FocusSat>, NonStratifiable> {
    let sparql = prepare(data, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        if st.shape != shape {
            continue;
        }
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(trace) =
                sat_trace(&mut evaluator, &v, st.shape, &Path::Id, None, &mut stack)
            {
                out.push(FocusSat {
                    focus: v,
                    statement: i,
                    trace,
                });
            }
        }
    }
    Ok(out)
}

/// Witness one specific `node` against one specific `shape` (by id) — the building
/// block for repairing a `ConformsTo` hole: bind it to a node, then witness that
/// node against the sub-shape and synthesize its repair. Returns `Ok(None)` when
/// the node already conforms (nothing to build), `Ok(Some(_))` otherwise.
///
/// The `statement` field of the returned [`FocusWitness`] is a sentinel
/// (`usize::MAX`): this is not a top-level statement, and synthesis ignores it.
pub fn witness_node(
    data: &Graph,
    schema: &Schema,
    node: &Term,
    shape: ShapeId,
) -> Result<Option<FocusWitness>, NonStratifiable> {
    let sparql = prepare(data, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut stack = Stack::new();
    Ok(
        witness(&mut evaluator, node, shape, &Path::Id, None, &mut stack).map(|failure| {
            FocusWitness {
                focus: node.clone(),
                statement: usize::MAX,
                failure,
            }
        }),
    )
}

/// Reasons `φ` (slot `id`) fails at `node`. `None` ⟺ it holds (incl. on a gfp
/// back-edge). `reached_by` is the structured path from the focus; `produced_by`
/// is how `node` was reached from its parent value (for replace-in-place).
fn witness(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    reached_by: &Path,
    produced_by: Option<&PathSupport>,
    stack: &mut Stack,
) -> Option<Witness> {
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        return None; // back-edge ⇒ assume conforming (gfp)
    }
    if eval.holds(node, id) {
        stack.remove(&key);
        return None; // holds ⇒ nothing to repair
    }
    let shape = eval.arena().get(id).clone();
    let result = match shape {
        Shape::Top | Shape::Pending => None,
        // `sh:severity` is transparent to witnessing: repair the wrapped shape.
        Shape::Annotated { shape, .. } => {
            let inner = witness(eval, node, shape, reached_by, produced_by, stack);
            stack.remove(&key);
            return inner;
        }
        Shape::TestConst(_) | Shape::TestType(_) | Shape::TestKind(_) => Some(Witness::Atom {
            shape: id,
            node: node.clone(),
            reached_by: reached_by.clone(),
            produced_by: produced_by.cloned(),
        }),
        Shape::Eq(..) | Shape::Disj(..) | Shape::Lt(..) | Shape::Le(..) | Shape::UniqueLang(_) => {
            Some(relational_witness(eval, node, id, &shape))
        }
        Shape::Closed(ref q) => {
            let offenders = closed_offenders(eval.backend(), node, q);
            (!offenders.is_empty()).then(|| Witness::Closed {
                shape: id,
                node: node.clone(),
                offenders,
            })
        }
        Shape::Not(c) => sat_trace(eval, node, c, reached_by, produced_by, stack).map(|t| {
            Witness::Not {
                shape: id,
                node: node.clone(),
                inner: Box::new(t),
            }
        }),
        Shape::And(cs) => {
            let failed: Vec<Witness> = cs
                .iter()
                .filter_map(|c| witness(eval, node, *c, reached_by, produced_by, stack))
                .collect();
            (!failed.is_empty()).then(|| Witness::All {
                shape: id,
                node: node.clone(),
                failed,
            })
        }
        Shape::Or(cs) => {
            // Or failed ⟹ every disjunct failed.
            let branches: Vec<Witness> = cs
                .iter()
                .filter_map(|c| witness(eval, node, *c, reached_by, produced_by, stack))
                .collect();
            (!branches.is_empty()).then(|| Witness::Any {
                shape: id,
                node: node.clone(),
                branches,
            })
        }
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => count_witness(eval, node, id, &path, min, max, qualifier, reached_by, stack),
        Shape::Sparql(_) => Some(Witness::Opaque {
            shape: id,
            node: node.clone(),
        }),
    };
    stack.remove(&key);
    result
}

#[allow(clippy::too_many_arguments)]
fn count_witness(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    path: &Path,
    min: Option<u64>,
    max: Option<u64>,
    qualifier: ShapeId,
    reached_by: &Path,
    stack: &mut Stack,
) -> Option<Witness> {
    let values: Vec<Term> = succ(eval.backend(), node, path).into_iter().collect();
    let matched: Vec<Term> = values
        .into_iter()
        .filter(|u| eval.holds(u, qualifier))
        .collect();
    let n = matched.len() as u64;

    if let Some(m) = min
        && n < m
    {
        return Some(Witness::CountLow {
            shape: id,
            node: node.clone(),
            path: path.clone(),
            qualifier,
            have: n,
            min: m,
        });
    }
    if let Some(m) = max
        && n > m
    {
        let matched_sup: Vec<(Term, PathSupport)> = matched
            .iter()
            .map(|u| {
                (
                    u.clone(),
                    path_support(eval.backend(), node, path, u).unwrap_or(PathSupport::Empty),
                )
            })
            .collect();
        // ∀-encoding `∃≤0 π.¬inner`: drill into each offending value.
        let per_value = if m == 0 {
            if let Shape::Not(inner) = eval.arena().get(qualifier).clone() {
                let reached = Path::seq(vec![reached_by.clone(), path.clone()]);
                let mut pv = Vec::new();
                for u in &matched {
                    let ps = path_support(eval.backend(), node, path, u);
                    if let Some(w) = witness(eval, u, inner, &reached, ps.as_ref(), stack) {
                        pv.push((u.clone(), w));
                    }
                }
                pv
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        return Some(Witness::CountHigh {
            shape: id,
            node: node.clone(),
            path: path.clone(),
            qualifier,
            matched: matched_sup,
            max: m,
            per_value,
        });
    }
    None
}

fn relational_witness(
    eval: &ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    shape: &Shape,
) -> Witness {
    let g = eval.backend();
    let (kind, lpath, rpred) = match shape {
        Shape::Eq(p, q) => (RelKind::Eq, p.clone(), Some(q.clone())),
        Shape::Disj(p, q) => (RelKind::Disj, p.clone(), Some(q.clone())),
        Shape::Lt(p, q) => (RelKind::Lt, p.clone(), Some(q.clone())),
        Shape::Le(p, q) => (RelKind::Le, p.clone(), Some(q.clone())),
        Shape::UniqueLang(p) => (RelKind::UniqueLang, p.clone(), None),
        _ => unreachable!("relational_witness on non-relational shape"),
    };
    let with_support = |g: &dyn PathBackend, p: &Path| -> Vec<(Term, PathSupport)> {
        succ(g, node, p)
            .into_iter()
            .map(|v| {
                let s = path_support(g, node, p, &v).unwrap_or(PathSupport::Empty);
                (v, s)
            })
            .collect()
    };
    let lhs = with_support(g, &lpath);
    let rhs = match &rpred {
        Some(q) => with_support(g, &Path::Pred(q.clone())),
        None => Vec::new(),
    };
    let offending = offending_pairs(kind, &lhs, &rhs);
    Witness::Relational {
        shape: id,
        node: node.clone(),
        kind,
        lhs,
        rhs,
        offending,
    }
}

/// The witnessing pairs/members for a failed relational constraint.
fn offending_pairs(
    kind: RelKind,
    lhs: &[(Term, PathSupport)],
    rhs: &[(Term, PathSupport)],
) -> Vec<(Term, Term)> {
    let lvals: Vec<&Term> = lhs.iter().map(|(v, _)| v).collect();
    let rvals: Vec<&Term> = rhs.iter().map(|(v, _)| v).collect();
    match kind {
        RelKind::Eq => lvals
            .iter()
            .filter(|v| !rvals.contains(v))
            .chain(rvals.iter().filter(|v| !lvals.contains(v)))
            .map(|v| ((*v).clone(), (*v).clone()))
            .collect(),
        RelKind::Disj => lvals
            .iter()
            .filter(|v| rvals.contains(v))
            .map(|v| ((*v).clone(), (*v).clone()))
            .collect(),
        RelKind::Lt | RelKind::Le => {
            // pairs (a, b) that fail a < b (resp. a ≤ b)
            let mut bad = Vec::new();
            for a in &lvals {
                for b in &rvals {
                    let ok = match crate::value::compare_terms(a, b) {
                        Some(std::cmp::Ordering::Less) => true,
                        Some(std::cmp::Ordering::Equal) => kind == RelKind::Le,
                        _ => false,
                    };
                    if !ok {
                        bad.push(((*a).clone(), (*b).clone()));
                    }
                }
            }
            bad
        }
        RelKind::UniqueLang => {
            // pairs of lhs values sharing a language tag
            let mut bad = Vec::new();
            for i in 0..lvals.len() {
                for j in (i + 1)..lvals.len() {
                    if let (Term::Literal(a), Term::Literal(b)) = (lvals[i], lvals[j])
                        && let (Some(la), Some(lb)) = (a.language(), b.language())
                        && la.eq_ignore_ascii_case(lb)
                    {
                        bad.push((lvals[i].clone(), lvals[j].clone()));
                    }
                }
            }
            bad
        }
    }
}

/// Support for why `φ` holds at `node`. `None` ⟺ it fails. The dual of [`witness`].
fn sat_trace(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    reached_by: &Path,
    produced_by: Option<&PathSupport>,
    stack: &mut Stack,
) -> Option<SatTrace> {
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        // back-edge: coinductively assumed true, no grounded support to delete.
        return Some(SatTrace::Coinductive {
            shape: id,
            node: node.clone(),
        });
    }
    if !eval.holds(node, id) {
        stack.remove(&key);
        return None; // fails ⇒ no satisfaction to break
    }
    let shape = eval.arena().get(id).clone();
    let result = match shape {
        Shape::Top | Shape::Pending => Some(SatTrace::Irrefutable { shape: id }),
        // `sh:severity` is transparent: break the satisfaction of the wrapped shape.
        Shape::Annotated { shape, .. } => {
            let inner = sat_trace(eval, node, shape, reached_by, produced_by, stack);
            stack.remove(&key);
            return inner;
        }
        Shape::TestConst(_) | Shape::TestType(_) | Shape::TestKind(_) => Some(SatTrace::Atom {
            shape: id,
            node: node.clone(),
            reached_by: reached_by.clone(),
            produced_by: produced_by.cloned().unwrap_or(PathSupport::Empty),
        }),
        Shape::Eq(..) | Shape::Disj(..) | Shape::Lt(..) | Shape::Le(..) | Shape::UniqueLang(_) => {
            Some(SatTrace::Blocked {
                shape: id,
                node: node.clone(),
                reason: BlockReason::Unsupported,
            })
        }
        Shape::Closed(_) => Some(SatTrace::Blocked {
            shape: id,
            node: node.clone(),
            reason: BlockReason::ClosedNeedsAdd,
        }),
        Shape::Sparql(_) => Some(SatTrace::Blocked {
            shape: id,
            node: node.clone(),
            reason: BlockReason::OpaqueSparql,
        }),
        Shape::Not(c) => witness(eval, node, c, reached_by, produced_by, stack).map(|w| {
            SatTrace::NotHeld {
                shape: id,
                node: node.clone(),
                inner_fails: Box::new(w),
            }
        }),
        Shape::And(cs) => {
            // holds ⟹ all children hold.
            let children: Vec<SatTrace> = cs
                .iter()
                .filter_map(|c| sat_trace(eval, node, *c, reached_by, produced_by, stack))
                .collect();
            Some(SatTrace::AllHeld {
                shape: id,
                node: node.clone(),
                children,
            })
        }
        Shape::Or(cs) => {
            let satisfied: Vec<SatTrace> = cs
                .iter()
                .filter_map(|c| sat_trace(eval, node, *c, reached_by, produced_by, stack))
                .collect();
            Some(SatTrace::AnyHeld {
                shape: id,
                node: node.clone(),
                satisfied,
            })
        }
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => {
            let values: Vec<Term> = succ(eval.backend(), node, &path).into_iter().collect();
            let reached = Path::seq(vec![reached_by.clone(), path.clone()]);
            let mut matches = Vec::new();
            for u in values {
                if eval.holds(&u, qualifier) {
                    let ps = path_support(eval.backend(), node, &path, &u);
                    if let Some(t) = sat_trace(eval, &u, qualifier, &reached, ps.as_ref(), stack) {
                        matches.push((u, t));
                    }
                }
            }
            Some(SatTrace::CountHeld {
                shape: id,
                node: node.clone(),
                path: path.clone(),
                qualifier,
                matches,
                min,
                max,
            })
        }
    };
    stack.remove(&key);
    result
}

/// Predicates+objects on `node` not allowed by a closed shape's set `q`.
fn closed_offenders(
    g: &dyn PathBackend,
    node: &Term,
    q: &BTreeSet<NamedNode>,
) -> Vec<(NamedNode, Term)> {
    let allowed: HashSet<&NamedNode> = q.iter().collect();
    let mut out = Vec::new();
    for p in g.out_predicates(node) {
        if !allowed.contains(&p) {
            for o in g.objects(node, &p) {
                out.push((p.clone(), o));
            }
        }
    }
    out
}

/// The existing triples that make `to` a `π`-successor of `from`, if any. The
/// edges a deletion would cut to remove `to` from the value set.
fn path_support(g: &dyn PathBackend, from: &Term, path: &Path, to: &Term) -> Option<PathSupport> {
    match path {
        Path::Id => (from == to).then_some(PathSupport::Empty),
        Path::Pred(q) => {
            if g.objects(from, q).contains(to) {
                let s = node_of(from)?;
                Some(PathSupport::Edge(Triple::new(s, q.clone(), to.clone())))
            } else {
                None
            }
        }
        Path::Inverse(p) => path_support(g, to, p, from),
        Path::Alt(ps) => ps.iter().find_map(|p| path_support(g, from, p, to)),
        Path::Seq(ps) => seq_support(g, from, ps, to),
        Path::Star(p) => star_support(g, from, p, to),
    }
}

fn seq_support(g: &dyn PathBackend, from: &Term, ps: &[Path], to: &Term) -> Option<PathSupport> {
    let Some((first, rest)) = ps.split_first() else {
        return (from == to).then_some(PathSupport::Empty);
    };
    for mid in succ(g, from, first) {
        let Some(head) = path_support(g, from, first, &mid) else {
            continue;
        };
        if let Some(tail) = seq_support(g, &mid, rest, to) {
            let mut chain = vec![head];
            match tail {
                PathSupport::Empty => {}
                PathSupport::Chain(v) => chain.extend(v),
                other => chain.push(other),
            }
            return Some(PathSupport::Chain(chain));
        }
    }
    None
}

fn star_support(g: &dyn PathBackend, from: &Term, p: &Path, to: &Term) -> Option<PathSupport> {
    if from == to {
        return Some(PathSupport::Empty);
    }
    let mut visited: HashSet<Term> = HashSet::from([from.clone()]);
    let mut queue: VecDeque<(Term, Vec<PathSupport>)> = VecDeque::from([(from.clone(), Vec::new())]);
    while let Some((cur, chain)) = queue.pop_front() {
        for next in succ(g, &cur, p) {
            let Some(edge) = path_support(g, &cur, p, &next) else {
                continue;
            };
            let mut chain2 = chain.clone();
            chain2.push(edge);
            if next == *to {
                return Some(PathSupport::Chain(chain2));
            }
            if visited.insert(next.clone()) {
                queue.push_back((next, chain2));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_parse::{load_turtle, parse_turtle};

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:  <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    "#;

    fn run(ttl: &str) -> Vec<FocusWitness> {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        witness_violations(&loaded.graph, &parsed.schema).expect("stratifiable")
    }

    /// Does any node in the witness tree satisfy `pred`?
    fn any(w: &Witness, pred: &impl Fn(&Witness) -> bool) -> bool {
        if pred(w) {
            return true;
        }
        match w {
            Witness::All { failed, .. } => failed.iter().any(|c| any(c, pred)),
            Witness::Any { branches, .. } => branches.iter().any(|c| any(c, pred)),
            Witness::CountHigh { per_value, .. } => {
                per_value.iter().any(|(_, c)| any(c, pred))
            }
            _ => false,
        }
    }

    #[test]
    fn conforming_graph_yields_no_witnesses() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:p ex:y .
            "
        );
        assert!(run(&ttl).is_empty());
    }

    #[test]
    fn min_count_violation_is_count_low() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 2 ] .
            ex:x ex:p ex:y .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].focus.to_string(), "<http://ex/x>");
        assert!(any(&ws[0].failure, &|w| matches!(
            w,
            Witness::CountLow { have: 1, min: 2, .. }
        )));
    }

    #[test]
    fn datatype_violation_is_an_atom_with_support() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        // The bad value is reached via ex:p, so its atom carries a cut edge.
        assert!(any(&ws[0].failure, &|w| matches!(
            w,
            Witness::Atom { produced_by: Some(PathSupport::Edge(_)), .. }
        )));
    }

    #[test]
    fn focus_level_nodekind_atom_has_no_support() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:nodeKind sh:IRI .
            ex:x ex:p \"v\" .
            ex:y ex:q ex:x .
            "
        );
        // ex:x is an IRI so it conforms; use a literal-targeted shape instead:
        let _ = ttl;
        let ttl2 = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:nodeKind sh:Literal .
            ex:x ex:p ex:y .
            "
        );
        let ws = run(&ttl2);
        assert_eq!(ws.len(), 1);
        assert!(matches!(
            ws[0].failure,
            Witness::Atom {
                produced_by: None,
                ..
            }
        ));
    }

    #[test]
    fn non_stratifiable_schema_is_diagnosed() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:not [ sh:path ex:p ; sh:qualifiedValueShape ex:S ; sh:qualifiedMinCount 1 ] .
            ex:x ex:p ex:y .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        assert!(witness_violations(&loaded.graph, &parsed.schema).is_err());
    }

    #[test]
    fn witness_shape_and_satisfy_shape_scope_to_one_shape() {
        // Two targeted shapes; one focus fails ex:S, one passes ex:S, and a third
        // node fails an unrelated shape ex:T that the ex:S queries must ignore.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetClass ex:C ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:T a sh:NodeShape ; sh:targetClass ex:D ;
                sh:property [ sh:path ex:q ; sh:minCount 1 ] .
            ex:good a ex:C ; ex:p ex:y .
            ex:bad  a ex:C .
            ex:other a ex:D .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let schema = &parsed.schema;

        let s = shape_id_for_iri(schema, "http://ex/S").expect("ex:S is named");
        assert!(shape_id_for_iri(schema, "http://ex/missing").is_none());

        // Failures: just ex:bad, never the ex:T violation on ex:other.
        let fails = witness_shape(&loaded.graph, schema, s).expect("stratifiable");
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].focus.to_string(), "<http://ex/bad>");

        // Satisfactions: just ex:good, with the matched value recorded.
        let sats = satisfy_shape(&loaded.graph, schema, s).expect("stratifiable");
        assert_eq!(sats.len(), 1);
        assert_eq!(sats[0].focus.to_string(), "<http://ex/good>");
        // ex:good holds because the ex:p count is met by ex:y.
        assert!(any_sat(&sats[0].trace, &|t| matches!(
            t,
            SatTrace::CountHeld { matches, .. } if matches.iter().any(|(v, _)| v.to_string() == "<http://ex/y>")
        )));
    }

    /// Does any node in the satisfaction trace satisfy `pred`?
    fn any_sat(t: &SatTrace, pred: &impl Fn(&SatTrace) -> bool) -> bool {
        if pred(t) {
            return true;
        }
        match t {
            SatTrace::AllHeld { children, .. } => children.iter().any(|c| any_sat(c, pred)),
            SatTrace::AnyHeld { satisfied, .. } => satisfied.iter().any(|c| any_sat(c, pred)),
            SatTrace::CountHeld { matches, .. } => matches.iter().any(|(_, c)| any_sat(c, pred)),
            _ => false,
        }
    }
}
